use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use crate::shard::Shard;
use fst::automaton::{StartsWith, Str};
use fst::map::Map;
use fst::map::StreamBuilder;
use fst::Automaton;
use fst::Streamer;
use memmap2::Mmap;
use roaring::RoaringBitmap;
use std::sync::Arc;

use crate::builder::CHUNK_SIZE;
use crate::error::{IndexError, Result};
use crate::Database;
use fst::map::MapBuilder;
use std::collections::{BTreeMap, HashMap};
use std::io::{BufWriter, Write};

/// In-memory handle to a variant-C tag index file (.tags), embedding an FST mapping
/// each tag to a 64-bit memory offset pointing to:
/// [bitmap_len:u32][string_len:u32][bitmap_data][original_case_string]
/// 
/// Format v2: Simplified aligned format
/// - Header: 8-byte header with bitmap length and string length (64-bit aligned)
/// - Bitmap: RoaringBitmap data immediately after header (naturally 64-bit aligned)
/// - String: UTF-8 string data after bitmap (no CBOR encoding)
pub struct TagIndex {
    /// Underlying shared memory-mapped file
    mmap: crate::shard::SharedMmap,
    /// FST map over in-file FST section
    fst: Map<TagFstBacking>,
    /// Byte offset where the data section begins
    data_offset: usize,
}

/// Backing buffer and slice indices for the embedded FST section within the tags file.
#[derive(Clone)]
struct TagFstBacking {
    mmap: crate::shard::SharedMmap,
    fst_start: usize,
    fst_len: usize,
}

impl AsRef<[u8]> for TagFstBacking {
    fn as_ref(&self) -> &[u8] {
        &self.mmap.as_ref()[self.fst_start..self.fst_start + self.fst_len]
    }
}

impl TagIndex {
    const HEADER_SIZE: usize = 4 + 2 + 8;
    const MAGIC: &'static [u8; 4] = b"FTGT";
    const VERSION: u16 = 2;

    /// Open a variant-C tag index at `<idx_path>.tags`, validating header and preparing FST.
    pub fn open<P: AsRef<Path>>(idx_path: P) -> Result<Self> {
        let path = idx_path.as_ref().with_extension("tags");
        let file = File::open(&path).map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to open tag index {:?}: {}", path, e),
            ))
        })?;
        let raw = unsafe { Mmap::map(&file) }.map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to mmap tag index {:?}: {}", path, e),
            ))
        })?;
        let mmap = crate::shard::SharedMmap::from(raw);
        let buf = mmap.as_ref();
        if buf.len() < Self::HEADER_SIZE {
            return Err(IndexError::InvalidFormat("tags file too small"));
        }
        if &buf[0..4] != Self::MAGIC {
            return Err(IndexError::InvalidFormat("invalid tags magic"));
        }
        let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        if version == 1 {
            return Err(IndexError::InvalidFormat("v1 format no longer supported"));
        } else if version == Self::VERSION {
            let fst_size = u64::from_le_bytes(buf[6..14].try_into().unwrap()) as usize;
            let fst_start = Self::HEADER_SIZE;
            let fst_len = fst_size;
            if buf.len() < fst_start + fst_len {
                return Err(IndexError::InvalidFormat("tags file truncated before fst"));
            }
            let backing = TagFstBacking {
                mmap: mmap.clone(),   
                fst_start,
                fst_len,
            };
            let fst = Map::new(backing.clone()).map_err(|e| {
                IndexError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("fst map error in tags: {}", e),
                ))
            })?;
            
            let data_offset = fst_start + fst_len;
            
            Ok(TagIndex {
                mmap,
                fst,
                data_offset,
            })
        } else {
            return Err(IndexError::InvalidFormat("unsupported tags version"));
        }
    }

    /// Retrieve the RoaringBitmap for a given tag, or None if the tag is not present.
    pub fn get(&self, tag: &str) -> Result<Option<RoaringBitmap>> {
        if let Some(offset) = self.fst.get(tag) {
            self.get_bitmap_at_offset(offset as usize).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Helper method to read bitmap at a given offset.
    /// Data layout: [bitmap_len:u32][string_len:u32][bitmap aligned to 64-bit][string]
    fn get_bitmap_at_offset(&self, offset: usize) -> Result<RoaringBitmap> {
        let buf = self.mmap.as_ref();
        let start = self.data_offset + offset;
        
        if start + 8 > buf.len() {
            return Err(IndexError::InvalidFormat("header out of bounds"));
        }
        
        // Read bitmap_len (u32) and string_len (u32)
        let bitmap_len = u32::from_le_bytes(
            buf[start..start + 4].try_into().unwrap()
        ) as usize;
        
        // Skip string_len (we don't need it for bitmap reading)
        
        // Bitmap starts immediately after 8-byte header (which is 64-bit aligned)
        let bitmap_start = start + 8;
        let bitmap_end = bitmap_start + bitmap_len;
        
        if bitmap_end > buf.len() {
            return Err(IndexError::InvalidFormat("bitmap data out of bounds"));
        }
        
        let bitmap_slice = &buf[bitmap_start..bitmap_end];
        RoaringBitmap::deserialize_from(bitmap_slice).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to deserialize roaring bitmap: {}", e),
            ))
        })
    }

    /// Helper method to read original case string at a given offset.
    /// Data layout: [bitmap_len:u32][string_len:u32][bitmap aligned to 64-bit][string]
    fn get_original_case_at_offset(&self, offset: usize) -> Result<Option<String>> {
        let buf = self.mmap.as_ref();
        let start = self.data_offset + offset;
        
        if start + 8 > buf.len() {
            return Err(IndexError::InvalidFormat("header out of bounds"));
        }
        
        // Read bitmap_len (u32) and string_len (u32)
        let bitmap_len = u32::from_le_bytes(
            buf[start..start + 4].try_into().unwrap()
        ) as usize;
        let string_len = u32::from_le_bytes(
            buf[start + 4..start + 8].try_into().unwrap()
        ) as usize;
        
        if string_len == 0 {
            return Ok(None); // No string data present
        }
        
        // Bitmap starts immediately after 8-byte header (which is 64-bit aligned)
        let bitmap_start = start + 8;
        // String starts after the bitmap
        let string_start = bitmap_start + bitmap_len;
        let string_end = string_start + string_len;
        
        if string_end > buf.len() {
            return Err(IndexError::InvalidFormat("string data out of bounds"));
        }
        
        let string_slice = &buf[string_start..string_end];
        let original_tag = String::from_utf8(string_slice.to_vec()).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to decode UTF-8 string: {}", e),
            ))
        })?;
        
        Ok(Some(original_tag))
    }
}

impl TagIndex {
    /// Stream all tags present in this index, yielding each tag's raw bytes and its
    /// memory offset. Use `get_bitmap` to decode the offset into a RoaringBitmap when needed.
    pub fn list_tags<'a>(&'a self) -> StreamBuilder<'a, StartsWith<Str<'a>>> {
        self.fst.search(Str::new("").starts_with())
    }

    /// Get all original case tags by iterating through the FST.
    pub fn get_original_tags(&self) -> Result<Vec<String>> {
        let mut tags = Vec::new();
        let mut stream = self.fst.stream();
        
        while let Some((_key, offset)) = stream.next() {
            if let Some(original_tag) = self.get_original_case_at_offset(offset as usize)? {
                tags.push(original_tag);
            }
        }
        
        Ok(tags)
    }

    /// Get the original case version of a tag by its lowercase version.
    /// Returns None if the tag is not found.
    pub fn get_original_case(&self, lowercase_tag: &str) -> Result<Option<String>> {
        // Direct lookup - we have the offset, we can read the original case directly
        if let Some(offset) = self.fst.get(lowercase_tag) {
            self.get_original_case_at_offset(offset as usize)
        } else {
            Ok(None)
        }
    }

    /// Retrieve the RoaringBitmap for a memory offset obtained from `list_tags`.
    pub fn get_bitmap(&self, offset: u64) -> Result<RoaringBitmap> {
        self.get_bitmap_at_offset(offset as usize)
    }


    /// Get only the count (length) of a bitmap.
    /// Currently uses full deserialization but isolates this operation for future optimization.
    pub fn get_bitmap_count_at_offset(&self, offset: usize) -> Result<u64> {
        let buf = self.mmap.as_ref();
        let start = self.data_offset + offset;
        
        if start + 8 > buf.len() {
            return Err(IndexError::InvalidFormat("header out of bounds"));
        }
        
        // Read bitmap_len (u32)
        let bitmap_len = u32::from_le_bytes(
            buf[start..start + 4].try_into().unwrap()
        ) as usize;
        
        // Bitmap starts immediately after 8-byte header (which is 64-bit aligned)
        let bitmap_start = start + 8;
        let bitmap_end = bitmap_start + bitmap_len;
        
        if bitmap_end > buf.len() {
            return Err(IndexError::InvalidFormat("bitmap data out of bounds"));
        }
        
        let bitmap_slice = &buf[bitmap_start..bitmap_end];
        
        // TODO: Optimize this by parsing just the RoaringBitmap header for cardinality
        RoaringBitmap::deserialize_from(bitmap_slice)
            .map(|bm| bm.len())
            .map_err(|e| {
                IndexError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("failed to deserialize roaring bitmap for count: {}", e),
                ))
            })
    }

    /// Bulk operation to get bitmap counts for multiple tags without full deserialization.
    /// Much faster than get_bitmaps_bulk when you only need counts.
    pub fn get_bitmap_counts_bulk(&self, tags: &[&str]) -> Result<Vec<Option<u64>>> {
        // Collect tags with their offsets and original indices
        let mut tag_offsets: Vec<_> = tags.iter()
            .enumerate()
            .filter_map(|(idx, tag)| {
                self.fst.get(tag).map(|offset| (idx, offset as usize))
            })
            .collect();
        
        // Sort by offset for sequential access pattern
        tag_offsets.sort_unstable_by_key(|(_, offset)| *offset);
        
        // Read bitmap counts in offset order
        let mut results = vec![None; tags.len()];
        for (original_idx, offset) in tag_offsets {
            if let Ok(count) = self.get_bitmap_count_at_offset(offset) {
                results[original_idx] = Some(count);
            }
        }
        
        Ok(results)
    }

    /// Bulk operation to get multiple bitmaps with optimized access pattern.
    /// Sorts tags by offset for sequential memory access, improving cache locality.
    pub fn get_bitmaps_bulk(&self, tags: &[&str]) -> Result<Vec<Option<RoaringBitmap>>> {
        // Collect tags with their offsets and original indices
        let mut tag_offsets: Vec<_> = tags.iter()
            .enumerate()
            .filter_map(|(idx, tag)| {
                self.fst.get(tag).map(|offset| (idx, offset as usize))
            })
            .collect();
        
        // Sort by offset for sequential access pattern
        tag_offsets.sort_unstable_by_key(|(_, offset)| *offset);
        
        // Read bitmaps in offset order
        let mut results = vec![None; tags.len()];
        for (original_idx, offset) in tag_offsets {
            if let Ok(bitmap) = self.get_bitmap_at_offset(offset) {
                results[original_idx] = Some(bitmap);
            }
        }
        
        Ok(results)
    }
}

/// Builder for creating a variant-C tag index file (`<idx_path>.tags`).
pub struct TagIndexBuilder {
    /// Full path to the `.idx` file that this tag index extends.
    idx_path: PathBuf,
    tag_bitmaps: HashMap<String, (RoaringBitmap, String)>,
    /// Optional hook that gets called after each insert_tags, passing the total count so far.
    on_progress: Option<Box<dyn Fn(u64)>>,
    /// Running total count for efficient progress reporting
    total_count: u64,
}

impl TagIndexBuilder {
    /// Create a new TagIndexBuilder for writing `<idx_path>.tags`.
    pub fn new<P: AsRef<Path>>(idx_path: P) -> Self {
        TagIndexBuilder {
            idx_path: idx_path.as_ref().to_path_buf(),
            tag_bitmaps: HashMap::new(),
            on_progress: None,
            total_count: 0,
        }
    }

    /// Insert a lookup ID and its associated tags into the index.
    pub fn insert_tags<T>(&mut self, id: u32, tags: T)
    where
        T: IntoIterator,
        T::Item: Into<String>,
    {
        let mut new_insertions = 0u64;
        for tag in tags {
            let original_tag = tag.into();
            let lowercase_tag = original_tag.to_lowercase();
            let entry = self.tag_bitmaps
                .entry(lowercase_tag)
                .or_insert_with(|| (RoaringBitmap::new(), original_tag));
            
            // Only count if this is a new insertion
            if entry.0.insert(id) {
                new_insertions += 1;
            }
        }
        
        // Update running total and call progress callback if needed
        self.total_count += new_insertions;
        if let Some(cb) = &self.on_progress {
            cb(self.total_count);
        }
    }

    /// Consume the builder and write out the `.tags` file.
    pub fn finish(self) -> Result<()> {
        let idx_path = self.idx_path;
        // Convert HashMap to sorted BTreeMap for deterministic FST building
        let tag_bitmaps: BTreeMap<String, (RoaringBitmap, String)> = self.tag_bitmaps.into_iter().collect();

        // Serialize each bitmap and build the data entries
        let mut data_entries = Vec::with_capacity(tag_bitmaps.len());
        
        for (_tag, (bitmap, original_case)) in tag_bitmaps.iter() {
            // Serialize bitmap
            let mut bitmap_buf = Vec::new();
            bitmap.serialize_into(&mut bitmap_buf).map_err(IndexError::Io)?;
            
            // Convert original case string to UTF-8 bytes
            let string_bytes = original_case.as_bytes();
            
            // Create data entry: [bitmap_len:u32][string_len:u32][bitmap][string]
            let mut entry = Vec::new();
            
            // Header: bitmap_len and string_len (8 bytes total)
            entry.extend_from_slice(&(bitmap_buf.len() as u32).to_le_bytes());
            entry.extend_from_slice(&(string_bytes.len() as u32).to_le_bytes());
            
            // Bitmap data starts immediately after 8-byte header (will be 64-bit aligned)
            entry.extend_from_slice(&bitmap_buf);
            
            // String data
            entry.extend_from_slice(string_bytes);
            
            data_entries.push(entry);
        }

        // Build FST with memory offsets, accounting for alignment
        let mut fst_section = Vec::new();
        let mut fst_builder = MapBuilder::new(&mut fst_section)?;
        let mut current_offset = 0u64;
        
        for ((tag, _), entry) in tag_bitmaps.iter().zip(data_entries.iter()) {
            // Align offset to 64-bit boundary before this entry
            let aligned_offset = (current_offset + 7) & !7;
            fst_builder.insert(tag, aligned_offset)?;
            current_offset = aligned_offset + entry.len() as u64;
        }
        
        fst_builder.finish()?;

        // Write new format: header, FST, padding, data entries
        let tags_path = idx_path.with_extension("tags");
        let tags_file = File::create(&tags_path)?;
        let mut writer = BufWriter::with_capacity(CHUNK_SIZE, tags_file);
        
        // Header: magic + version + fst_size
        writer.write_all(b"FTGT")?;
        writer.write_all(&2u16.to_le_bytes())?; // Version 2
        writer.write_all(&(fst_section.len() as u64).to_le_bytes())?; // FST_SIZE
        
        // FST section
        writer.write_all(&fst_section)?;
        
        // Data entries with individual alignment
        let mut written_bytes = 0u64;
        for entry in data_entries {
            // Align this entry to 64-bit boundary
            let padding_needed = ((written_bytes + 7) & !7) - written_bytes;
            if padding_needed > 0 {
                writer.write_all(&vec![0u8; padding_needed as usize])?;
                written_bytes += padding_needed;
            }
            
            writer.write_all(&entry)?;
            written_bytes += entry.len() as u64;
        }
        
        Ok(())
    }

    /// Attach a progress‐callback: called with the cumulative tag‐entry count
    /// on every `insert_tags` invocation.
    pub fn with_progress<F>(mut self, cb: F) -> Self
    where
        F: Fn(u64) + 'static,
    {
        self.on_progress = Some(Box::new(cb));
        self
    }

    /// Build a tag index for the given already-open shard.
    fn build_shard(
        shard: &Shard<serde_json::Value>,
        tag_field: &str,
        on_progress: Option<Arc<dyn Fn(u64)>>,
    ) -> Result<()> {
        let idx_path = shard.idx_path();
        let mut builder = TagIndexBuilder::new(idx_path);
        if let Some(cb) = on_progress {
            builder = builder.with_progress(move |n| cb(n));
        }
        let mut stream = shard.fst.stream();
        while let Some((_, weight)) = stream.next() {
            let shard_ptr = weight as u32;
            let lut = shard.lookup.get(shard_ptr)?;
            let payload_ptr = lut.payload_ptr;
            if let Some(val) = shard.payload.get(payload_ptr)? {
                if let Some(arr) = val.get(tag_field).and_then(|v| v.as_array()) {
                    builder.insert_tags(shard_ptr, arr.iter().filter_map(|v| v.as_str()));
                }
            }
        }
        builder.finish()?;
        Ok(())
    }

    /// Scan a shard's .idx/.payload/.lookup for the given JSON field array of tags
    /// and write out `<idx_path>.tags`. The optional `on_progress` callback receives
    /// the cumulative tag-entry count after each record.
    pub fn build<P: AsRef<Path>>(
        idx_path: P,
        tag_field: &str,
        on_progress: Option<Arc<dyn Fn(u64)>>,
    ) -> Result<()> {
        let shard = Shard::<serde_json::Value>::open(idx_path.as_ref())?;
        TagIndexBuilder::build_shard(&shard, tag_field, on_progress)
    }

    /// Build or rebuild the tag index files by scanning existing JSON payloads for the given tag field.
    /// This requires the database to be opened outside and passed as mutable.
    /// It writes a `.tags` file for each shard and attaches the loaded TagIndex to the shard.
    pub fn build_index(
        db: &mut Database<serde_json::Value>,
        tag_field: &str,
        on_progress: Option<Arc<dyn Fn(u64)>>,
    ) -> Result<()> {
        for shard in db.shards_mut() {
            TagIndexBuilder::build_shard(shard, tag_field, on_progress.clone())?;
            shard.tags = Some(TagIndex::open(shard.idx_path())?);
        }
        Ok(())
    }
}

