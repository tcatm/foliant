use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::cmp::Ordering;
use std::collections::HashMap;

use fst::map::{Map, MapBuilder};
use fst::Streamer;
use memmap2::Mmap;
use roaring::RoaringBitmap;
use unicode_segmentation::UnicodeSegmentation;
use unicode_normalization::UnicodeNormalization;
use regex::Regex;

use crate::error::{IndexError, Result};
use crate::shard::{SharedMmap, Shard};
use crate::builder::CHUNK_SIZE;

const SEARCH_DIR_EXT: &str = "search";
pub(crate) const GRAM_SIZE: usize = 3;
const PROGRESS_STEP: u64 = 10_000;
const EXTERNAL_SORT_THRESHOLD: usize = 1_000_000; // Buffer 1M grams before flushing

/// Read-only handle to a search index per shard, indexing n-grams of keys.
pub struct SearchIndex {
    /// Memory-mapped grams FST (gram → posting offset)
    grams_fst: Map<SharedMmap>,
    /// Memory-mapped postings data
    postings_mmap: SharedMmap,
    /// Size of n-grams used (typically 3)
    gram_size: usize,
}

impl SearchIndex {
    /// Open a shard-local search index in the `<idx_path>.search/` directory.
    pub fn open<P: AsRef<Path>>(idx_path: P) -> Result<Self> {
        let dir = idx_path.as_ref().with_extension(SEARCH_DIR_EXT);
        
        // Open grams.fst
        let grams_path = dir.join("grams.fst");
        let grams_file = File::open(&grams_path).map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to open grams FST {:?}: {}", grams_path, e),
            ))
        })?;
        let grams_mmap = unsafe { Mmap::map(&grams_file) }.map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to mmap grams FST {:?}: {}", grams_path, e),
            ))
        })?;
        let grams_mmap = SharedMmap::from(grams_mmap);
        let grams_fst = Map::new(grams_mmap.clone()).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("fst map error: {}", e),
            ))
        })?;
        
        // Open postings.dat
        let postings_path = dir.join("postings.dat");
        let postings_file = File::open(&postings_path).map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to open postings file {:?}: {}", postings_path, e),
            ))
        })?;
        let postings_mmap = unsafe { Mmap::map(&postings_file) }.map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to mmap postings file {:?}: {}", postings_path, e),
            ))
        })?;
        let postings_mmap = SharedMmap::from(postings_mmap);
        
        Ok(SearchIndex {
            grams_fst,
            postings_mmap,
            gram_size: GRAM_SIZE,
        })
    }
    
    /// Search for keys containing the given query substring.
    /// Returns an iterator of document IDs that potentially match.
    pub fn search(&self, query: &str) -> Result<Vec<u64>> {
        let normalized = normalize_text_unicode(query);
        let grams = extract_ngrams_unicode(&normalized, self.gram_size);
        
        if grams.is_empty() {
            return Ok(Vec::new());
        }
        
        // For short queries, we need to search for the query as a substring
        if grams.len() == 1 && grams[0].contains('\0') {
            // Get the unpadded query
            let unpadded = grams[0].trim_end_matches('\0');
            
            // For short queries, we need to find all documents containing the query
            // This includes:
            // 1. Exact matches of short keys (padded gram matches)
            // 2. Substring matches within longer keys
            let mut union_bitmap = RoaringBitmap::new();
            
            // Check for exact match of the padded gram (for short keys like "世界")
            if let Some(offset) = self.grams_fst.get(grams[0].as_bytes()) {
                let bitmap = self.read_posting_list(offset as usize)?;
                union_bitmap |= bitmap;
            }
            
            // Find all grams that contain the query as a substring
            return self.search_substring(unpadded, union_bitmap);
        }
        
        // Get posting lists for each gram
        let mut posting_lists = Vec::new();
        
        for gram in &grams {
            if let Some(offset) = self.grams_fst.get(gram.as_bytes()) {
                let bitmap = self.read_posting_list(offset as usize)?;
                posting_lists.push(bitmap);
            } else {
                // If any gram is missing, no results
                return Ok(Vec::new());
            }
        }
        
        // Intersect all posting lists (start with smallest)
        posting_lists.sort_by_key(|b| b.len());
        let mut result = posting_lists[0].clone();
        for bitmap in posting_lists.iter().skip(1) {
            result &= bitmap;
            if result.is_empty() {
                break;
            }
        }
        
        // Convert to doc IDs
        Ok(result.iter().map(|id| id as u64).collect())
    }
    
    /// Search for documents containing a substring by checking all grams
    fn search_substring(&self, query: &str, mut union_bitmap: RoaringBitmap) -> Result<Vec<u64>> {
        use fst::Streamer;
        
        // We need to find all grams that contain our query as a substring
        // Since FST doesn't support substring search, we'll iterate through all grams
        // and check if they contain our query
        let mut stream = self.grams_fst.stream();
        
        while let Some((gram_bytes, offset)) = stream.next() {
            if let Ok(gram_str) = std::str::from_utf8(gram_bytes) {
                // Skip null-padded grams
                let gram_trimmed = gram_str.trim_end_matches('\0');
                
                // Check if this gram contains our query
                if gram_trimmed.contains(query) {
                    let bitmap = self.read_posting_list(offset as usize)?;
                    union_bitmap |= bitmap;
                }
            }
        }
        
        // Convert to doc IDs
        Ok(union_bitmap.iter().map(|id| id as u64).collect())
    }
    
    /// Read a posting list from the given offset in the postings file.
    fn read_posting_list(&self, offset: usize) -> Result<RoaringBitmap> {
        let data = self.postings_mmap.as_ref();
        
        if offset + 4 > data.len() {
            return Err(IndexError::InvalidFormat("posting offset out of bounds"));
        }
        
        // Read length prefix
        let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        let start = offset + 4;
        let end = start + len;
        
        if end > data.len() {
            return Err(IndexError::InvalidFormat("posting data out of bounds"));
        }
        
        // Deserialize bitmap
        RoaringBitmap::deserialize_from(&data[start..end]).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to deserialize posting list: {}", e),
            ))
        })
    }
}

/// Normalize text using Unicode normalization (NFKC) and case folding
pub(crate) fn normalize_text_unicode(text: &str) -> String {
    // Apply NFKC normalization and lowercase
    text.nfkc()
        .flat_map(|c| c.to_lowercase())
        .collect()
}

/// Extract n-grams from Unicode text using grapheme clusters
fn extract_ngrams_unicode(text: &str, n: usize) -> Vec<String> {
    let graphemes: Vec<&str> = text.graphemes(true).collect();
    
    if graphemes.is_empty() {
        return Vec::new();
    }
    
    let mut grams = Vec::new();
    let mut seen = HashMap::new();
    
    // If text is shorter than n, pad with null bytes to create one n-gram
    if graphemes.len() < n {
        let mut padded_gram = graphemes.join("");
        // Pad with null bytes to reach n graphemes
        for _ in graphemes.len()..n {
            padded_gram.push('\0');
        }
        grams.push(padded_gram);
    } else {
        // Normal n-gram extraction for longer texts
        for window in graphemes.windows(n) {
            let gram = window.join("");
            // Deduplicate
            let count = seen.entry(gram.clone()).or_insert(0);
            if *count == 0 {
                grams.push(gram);
            }
            *count += 1;
        }
    }
    
    grams
}

/// Strip diacritics (combining marks) from text, for accent-insensitive search.
/// Decompose (NFD), remove Unicode marks (category M), then recompose (NFKC).
pub(crate) fn strip_diacritics(text: &str) -> String {
    let decomp: String = text.nfd().collect();
    let stripped = Regex::new(r"\p{M}").unwrap().replace_all(&decomp, "").to_string();
    stripped.nfkc().collect()
}

// Removed old ASCII-only functions since we now use Unicode-aware versions

/// A single gram-docid pair for external sorting
#[derive(Clone, Debug)]
struct GramPair {
    gram: String,
    doc_id: u32,
}

impl GramPair {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let gram_bytes = self.gram.as_bytes();
        writer.write_all(&(gram_bytes.len() as u32).to_le_bytes())?;
        writer.write_all(gram_bytes)?;
        writer.write_all(&self.doc_id.to_le_bytes())?;
        Ok(())
    }
    
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes)?;
        let gram_len = u32::from_le_bytes(len_bytes) as usize;
        
        let mut gram_bytes = vec![0u8; gram_len];
        reader.read_exact(&mut gram_bytes)?;
        
        let mut doc_bytes = [0u8; 4];
        reader.read_exact(&mut doc_bytes)?;
        
        Ok(GramPair {
            gram: String::from_utf8(gram_bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            doc_id: u32::from_le_bytes(doc_bytes),
        })
    }
}

/// Builder for creating a search index for shard keys.
pub struct SearchIndexBuilder {
    idx_path: PathBuf,
    gram_pairs: Vec<GramPair>,
    temp_files: Vec<PathBuf>,
    temp_dir: PathBuf,
    on_progress: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    processed_count: u64,
}

impl SearchIndexBuilder {
    /// Create a new builder for writing `<idx_path>.search/`.
    pub fn new<P: AsRef<Path>>(idx_path: P) -> Result<Self> {
        let dir = idx_path.as_ref().with_extension(SEARCH_DIR_EXT);
        fs::create_dir_all(&dir).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to create search directory {:?}: {}", dir, e),
            ))
        })?;
        
        let temp_dir = dir.join("tmp");
        fs::create_dir_all(&temp_dir).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to create temp directory {:?}: {}", temp_dir, e),
            ))
        })?;
        
        Ok(SearchIndexBuilder {
            idx_path: idx_path.as_ref().to_path_buf(),
            gram_pairs: Vec::with_capacity(EXTERNAL_SORT_THRESHOLD),
            temp_files: Vec::new(),
            temp_dir,
            on_progress: None,
            processed_count: 0,
        })
    }
    
    /// Set progress callback
    pub fn with_progress(mut self, cb: Arc<dyn Fn(u64) + Send + Sync>) -> Self {
        self.on_progress = Some(cb);
        self
    }
    
    /// Index a key with its document ID.
    pub fn insert(&mut self, doc_id: u64, key: &str) -> Result<()> {
        let normalized = normalize_text_unicode(key);
        let grams = extract_ngrams_unicode(&normalized, GRAM_SIZE);
        
        // Add gram pairs to buffer
        for gram in &grams {
            self.gram_pairs.push(GramPair {
                gram: gram.clone(),
                doc_id: doc_id as u32,
            });
        }
        let deaccented = strip_diacritics(&normalized);
        if deaccented != normalized {
            let grams_deaccented = extract_ngrams_unicode(&deaccented, GRAM_SIZE);
            for gram in &grams_deaccented {
                self.gram_pairs.push(GramPair {
                    gram: gram.clone(),
                    doc_id: doc_id as u32,
                });
            }
        }
        
        // Flush to disk if buffer is full
        if self.gram_pairs.len() >= EXTERNAL_SORT_THRESHOLD {
            self.flush_buffer()?;
        }
        
        self.processed_count += 1;
        // Throttle progress callbacks - check modulo outside to avoid branch
        if self.processed_count % PROGRESS_STEP == 0 {
            if let Some(cb) = &self.on_progress {
                cb(self.processed_count);
            }
        }
        
        Ok(())
    }
    
    /// Flush the current buffer to a temporary file
    fn flush_buffer(&mut self) -> Result<()> {
        if self.gram_pairs.is_empty() {
            return Ok(());
        }
        
        // Sort pairs by gram, then doc_id
        self.gram_pairs.sort_unstable_by(|a, b| {
            match a.gram.cmp(&b.gram) {
                Ordering::Equal => a.doc_id.cmp(&b.doc_id),
                other => other,
            }
        });
        
        // Write to temp file
        let temp_path = self.temp_dir.join(format!("run_{}.dat", self.temp_files.len()));
        let file = File::create(&temp_path)?;
        let mut writer = BufWriter::with_capacity(CHUNK_SIZE, file);
        
        // Write all pairs
        for pair in &self.gram_pairs {
            pair.write_to(&mut writer)?;
        }
        
        writer.flush()?;
        self.temp_files.push(temp_path);
        self.gram_pairs.clear();
        
        Ok(())
    }
    
    /// Finish building and write the index files.
    pub fn finish(mut self) -> Result<()> {
        // Flush any remaining pairs
        self.flush_buffer()?;
        
        let dir = self.idx_path.with_extension(SEARCH_DIR_EXT);
        
        // Write postings file and build gram FST
        let postings_path = dir.join("postings.dat");
        let postings_file = File::create(&postings_path)?;
        let mut postings_writer = BufWriter::with_capacity(CHUNK_SIZE, postings_file);
        
        let grams_path = dir.join("grams.fst");
        let grams_file = File::create(&grams_path)?;
        let mut grams_writer = BufWriter::with_capacity(CHUNK_SIZE, grams_file);
        let mut grams_builder = MapBuilder::new(&mut grams_writer)?;
        
        // Merge sorted runs and build postings
        self.merge_and_build_postings(&mut postings_writer, &mut grams_builder)?;
        
        // Finish FST
        grams_builder.finish()?;
        drop(grams_writer);
        
        // Flush postings
        postings_writer.flush()?;
        
        // Clean up temp files
        for temp_file in &self.temp_files {
            let _ = fs::remove_file(temp_file);
        }
        let _ = fs::remove_dir(&self.temp_dir);
        
        // Final progress callback
        if let Some(cb) = &self.on_progress {
            cb(self.processed_count);
        }
        
        Ok(())
    }
    
    /// Merge sorted runs and build postings
    fn merge_and_build_postings<W: Write>(
        &self,
        postings_writer: &mut W,
        grams_builder: &mut MapBuilder<&mut W>,
    ) -> Result<()> {
        // Open all temp files
        let mut readers: Vec<_> = self.temp_files
            .iter()
            .map(|path| File::open(path).map(|f| BufReader::with_capacity(CHUNK_SIZE, f)))
            .collect::<io::Result<_>>()?;
        
        // Current pair from each file
        let mut current_pairs: Vec<Option<GramPair>> = Vec::with_capacity(readers.len());
        
        // Read initial pair from each file
        for reader in &mut readers {
            current_pairs.push(GramPair::read_from(reader).ok());
        }
        
        let mut current_gram: Option<String> = None;
        let mut current_bitmap = RoaringBitmap::new();
        let mut current_offset = 0u64;
        
        loop {
            // Find the minimum gram among all current pairs
            let mut min_gram = None;
            for pair_opt in &current_pairs {
                if let Some(pair) = pair_opt {
                    match &min_gram {
                        None => min_gram = Some(pair.gram.clone()),
                        Some(min_g) => {
                            if pair.gram < *min_g {
                                min_gram = Some(pair.gram.clone());
                            }
                        }
                    }
                }
            }
            
            // If no more pairs, we're done
            let Some(min_g) = min_gram else {
                // Write the last gram if any
                if let Some(gram) = current_gram {
                    self.write_posting(&gram, &current_bitmap, postings_writer, grams_builder, &mut current_offset)?;
                }
                break;
            };
            
            // If we've moved to a new gram, write the previous one
            if current_gram.is_some() && current_gram.as_ref() != Some(&min_g) {
                let gram = current_gram.unwrap();
                self.write_posting(&gram, &current_bitmap, postings_writer, grams_builder, &mut current_offset)?;
                current_bitmap.clear();
            }
            
            current_gram = Some(min_g.clone());
            
            // Collect all doc_ids for this gram
            for (i, pair_opt) in current_pairs.iter_mut().enumerate() {
                if let Some(pair) = pair_opt {
                    if pair.gram == min_g {
                        current_bitmap.insert(pair.doc_id);
                        // Read next pair from this file
                        *pair_opt = GramPair::read_from(&mut readers[i]).ok();
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Write a single posting list
    fn write_posting<W: Write>(
        &self,
        gram: &str,
        bitmap: &RoaringBitmap,
        postings_writer: &mut W,
        grams_builder: &mut MapBuilder<&mut W>,
        current_offset: &mut u64,
    ) -> Result<()> {
        // Get serialized size without actually serializing
        let bitmap_len = bitmap.serialized_size();
        
        // Write length prefix
        postings_writer.write_all(&(bitmap_len as u32).to_le_bytes())?;
        
        // Write bitmap data directly to writer
        bitmap.serialize_into(postings_writer).map_err(IndexError::Io)?;
        
        // Add to FST
        grams_builder.insert(gram, *current_offset)?;
        
        // Update offset
        *current_offset += 4 + bitmap_len as u64;
        
        Ok(())
    }
    
    /// Build a search index for a single shard's keys.
    pub fn build_shard(
        shard: &Shard<serde_json::Value>,
        on_progress: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    ) -> Result<()> {
        let mut builder = SearchIndexBuilder::new(shard.idx_path())?;
        if let Some(cb) = on_progress {
            builder = builder.with_progress(cb);
        }
        
        // Stream through all keys in the shard
        let mut stream = shard.fst.stream();
        while let Some((key_bytes, doc_id)) = stream.next() {
            if let Ok(key) = std::str::from_utf8(key_bytes) {
                builder.insert(doc_id, key)?;
            }
        }
        
        builder.finish()?;
        Ok(())
    }
    
    /// Build search indices for all shards in a database.
    pub fn build_index(
        db: &mut crate::Database<serde_json::Value>,
        on_progress: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    ) -> Result<()> {
        for shard in db.shards_mut() {
            SearchIndexBuilder::build_shard(shard, on_progress.clone())?;
            shard.search = Some(SearchIndex::open(shard.idx_path())?);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_normalize_text_unicode() {
        // Basic ASCII
        assert_eq!(normalize_text_unicode("Hello World"), "hello world");
        
        // Accented characters - should be preserved but lowercased
        assert_eq!(normalize_text_unicode("Café"), "café");
        assert_eq!(normalize_text_unicode("naïve"), "naïve");
        
        // Ligatures (NFKC normalization)
        assert_eq!(normalize_text_unicode("ﬁle"), "file"); // fi ligature
        assert_eq!(normalize_text_unicode("ﬂower"), "flower"); // fl ligature
        
        // Full-width characters (NFKC normalizes these)
        assert_eq!(normalize_text_unicode("ＨＥＬＬＯ"), "hello");
        
        // Mixed scripts
        assert_eq!(normalize_text_unicode("Hello世界"), "hello世界");
        assert_eq!(normalize_text_unicode("Привет"), "привет");
        
        // Emojis (should remain unchanged)
        assert_eq!(normalize_text_unicode("Hello 👋 World"), "hello 👋 world");
        
        // Combining characters
        let combining = "e\u{0301}"; // e + combining acute accent
        assert_eq!(normalize_text_unicode(combining), "é"); // Should be composed form
    }
    
    #[test]
    fn test_extract_ngrams_unicode() {
        // Basic ASCII
        let grams = extract_ngrams_unicode("hello", 3);
        assert_eq!(grams, vec!["hel", "ell", "llo"]);
        
        // With accented characters
        let grams = extract_ngrams_unicode("café", 3);
        assert_eq!(grams, vec!["caf", "afé"]);
        
        // With emoji - each emoji is one grapheme
        let grams = extract_ngrams_unicode("hi👋", 3);
        assert_eq!(grams, vec!["hi👋"]);
        
        // With emoji in middle
        let grams = extract_ngrams_unicode("hi👋bye", 3);
        assert_eq!(grams, vec!["hi👋", "i👋b", "👋by", "bye"]);
        
        // Family emoji (ZWJ sequence) - treated as single grapheme
        let grams = extract_ngrams_unicode("a👨‍👩‍👧‍👦b", 3);
        assert_eq!(grams, vec!["a👨‍👩‍👧‍👦b"]);
        
        // Test deduplication
        let grams = extract_ngrams_unicode("aaaa", 3);
        assert_eq!(grams, vec!["aaa"]);
        
        // Too short - should return padded gram
        let grams = extract_ngrams_unicode("ab", 3);
        assert_eq!(grams, vec!["ab\0"]);
    }
    
    #[test]
    fn test_unicode_edge_cases() {
        // Zero-width joiners and modifiers
        let text = "👨‍💻"; // Man technologist (ZWJ sequence)
        let normalized = normalize_text_unicode(text);
        assert_eq!(normalized, "👨‍💻");
        let grams = extract_ngrams_unicode(&normalized, 2);
        assert_eq!(grams, vec!["👨‍💻\0"]); // Padded to 2 graphemes
        
        // Skin tone modifiers
        let text = "👋🏽"; // Waving hand with skin tone
        let grams = extract_ngrams_unicode(text, 1);
        assert_eq!(grams.len(), 1); // Treated as single grapheme
        
        // Regional indicators (flags)
        let text = "🇺🇸abc"; // US flag + abc
        let grams = extract_ngrams_unicode(text, 3);
        assert_eq!(grams, vec!["🇺🇸ab", "abc"]);
        
        // Combining diacritics
        let text = "e\u{0301}"; // e + combining acute accent = é
        let normalized = normalize_text_unicode(text);
        assert_eq!(normalized, "é"); // Should be composed form
        
        // Right-to-left text
        let text = "שלום"; // Hebrew "shalom"
        let grams = extract_ngrams_unicode(text, 2);
        assert_eq!(grams.len(), 3); // 4 chars = 3 bigrams
        
        // Zalgo text (many combining characters)
        let text = "h̵̝̰̭̓ę̶̺́ļ̸̳̏l̴̡̦̆ő̶̱"; 
        let normalized = normalize_text_unicode(text);
        // Should handle gracefully without panic
        assert!(!normalized.is_empty());
    }
    
    #[test]
    fn test_mixed_script_ngrams() {
        // English + Chinese
        let text = "hello世界world";
        let grams = extract_ngrams_unicode(text, 3);
        assert!(grams.contains(&"hel".to_string()));
        assert!(grams.contains(&"o世界".to_string()));
        assert!(grams.contains(&"界wo".to_string()));
        
        // English + Arabic + Emoji
        let text = "hi مرحبا 👋";
        let grams = extract_ngrams_unicode(text, 3);
        println!("Arabic text grams: {:?}", grams);
        assert!(grams.contains(&"hi ".to_string()));
        // The last gram should be "ا 👋" not "با 👋"
        assert!(grams.contains(&"ا 👋".to_string()));
    }
}