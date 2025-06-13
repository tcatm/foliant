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
use std::sync::Mutex;
use serde::Deserialize;

/// In-memory handle to a variant-C tag index file (.tags), embedding an FST mapping
/// each tag to a 64-bit memory offset pointing to (bitmap_len, bitmap, cbor).
pub struct TagIndex {
    /// Underlying shared memory-mapped file
    mmap: crate::shard::SharedMmap,
    /// FST map over in-file FST section
    fst: Map<TagFstBacking>,
    /// Byte offset where the data section begins
    data_offset: usize,
    /// Lazy cache for original case lookups (lowercase -> original case)
    original_case_cache: Mutex<Option<Arc<HashMap<String, String>>>>,
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
                original_case_cache: Mutex::new(None),
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
    /// Data layout: (bitmap_len: u32, bitmap: [u8], cbor: [u8])
    fn get_bitmap_at_offset(&self, offset: usize) -> Result<RoaringBitmap> {
        let buf = self.mmap.as_ref();
        let start = self.data_offset + offset;
        
        if start + 4 > buf.len() {
            return Err(IndexError::InvalidFormat("bitmap length out of bounds"));
        }
        
        // Read bitmap_len (u32)
        let bitmap_len = u32::from_le_bytes(
            buf[start..start + 4].try_into().unwrap()
        ) as usize;
        
        let bitmap_start = start + 4;
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
    /// Skips over the bitmap data to read the CBOR string.
    fn get_original_case_at_offset(&self, offset: usize) -> Result<Option<String>> {
        let buf = self.mmap.as_ref();
        let start = self.data_offset + offset;
        
        if start + 4 > buf.len() {
            return Err(IndexError::InvalidFormat("bitmap length out of bounds"));
        }
        
        // Read bitmap_len (u32) and skip over bitmap
        let bitmap_len = u32::from_le_bytes(
            buf[start..start + 4].try_into().unwrap()
        ) as usize;
        
        let cbor_start = start + 4 + bitmap_len;
        
        if cbor_start >= buf.len() {
            return Ok(None); // No CBOR data present
        }
        
        // CBOR is self-describing, parse one value from the stream
        let cbor_slice = &buf[cbor_start..];
        
        // Use the streaming decoder to read exactly one CBOR value  
        let mut de = serde_cbor::Deserializer::from_slice(cbor_slice);
        let original_tag: String = String::deserialize(&mut de).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to deserialize CBOR string: {}", e),
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

    /// Build the original case cache if not already built.
    /// This maps lowercase tags to their original case versions.
    fn ensure_original_case_cache(&self) -> Result<Arc<HashMap<String, String>>> {
        let cache_guard = self.original_case_cache.lock().unwrap();
        
        if let Some(cache) = cache_guard.as_ref() {
            // Cache already built, return cheap Arc clone
            return Ok(cache.clone());
        }
        
        // Need to build cache - drop the read lock and acquire write lock
        drop(cache_guard);
        let mut cache_guard = self.original_case_cache.lock().unwrap();
        
        // Double-check after acquiring write lock (another thread might have built it)
        if let Some(cache) = cache_guard.as_ref() {
            return Ok(cache.clone());
        }
        
        // Build map from lowercase to original case
        let mut map = HashMap::new();
        let mut stream = self.fst.stream();
        
        while let Some((key, offset)) = stream.next() {
            let lowercase_tag = std::str::from_utf8(key).map_err(|e| {
                IndexError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("invalid utf8 in tag key: {}", e),
                ))
            })?;
            
            // Use the helper method that properly handles CBOR parsing
            if let Some(original_tag) = self.get_original_case_at_offset(offset as usize)? {
                map.insert(lowercase_tag.to_string(), original_tag);
            }
        }
        
        let cache_arc = Arc::new(map);
        *cache_guard = Some(cache_arc.clone());
        Ok(cache_arc)
    }

    /// Get all original case tags from the cache.
    pub fn get_original_tags(&self) -> Result<Vec<String>> {
        let cache = self.ensure_original_case_cache()?;
        Ok(cache.values().cloned().collect())
    }

    /// Get the original case version of a tag by its lowercase version.
    /// Returns None if the tag is not found.
    pub fn get_original_case(&self, lowercase_tag: &str) -> Result<Option<String>> {
        // Fast path: try direct lookup if we have the FST entry
        if let Some(offset) = self.fst.get(lowercase_tag) {
            return self.get_original_case_at_offset(offset as usize);
        }
        
        // Fallback to cache if direct lookup fails
        let cache = self.ensure_original_case_cache()?;
        Ok(cache.get(lowercase_tag).cloned())
    }

    /// Retrieve the RoaringBitmap for a memory offset obtained from `list_tags`.
    pub fn get_bitmap(&self, offset: u64) -> Result<RoaringBitmap> {
        self.get_bitmap_at_offset(offset as usize)
    }
}

/// Builder for creating a variant-C tag index file (`<idx_path>.tags`).
pub struct TagIndexBuilder {
    /// Full path to the `.idx` file that this tag index extends.
    idx_path: PathBuf,
    tag_bitmaps: BTreeMap<String, (RoaringBitmap, String)>,
    /// Optional hook that gets called after each insert_tags, passing the total count so far.
    on_progress: Option<Box<dyn Fn(u64)>>,
}

impl TagIndexBuilder {
    /// Create a new TagIndexBuilder for writing `<idx_path>.tags`.
    pub fn new<P: AsRef<Path>>(idx_path: P) -> Self {
        TagIndexBuilder {
            idx_path: idx_path.as_ref().to_path_buf(),
            tag_bitmaps: BTreeMap::new(),
            on_progress: None,
        }
    }

    /// Insert a lookup ID and its associated tags into the index.
    pub fn insert_tags<T>(&mut self, id: u32, tags: T)
    where
        T: IntoIterator,
        T::Item: Into<String>,
    {
        for tag in tags {
            let original_tag = tag.into();
            let lowercase_tag = original_tag.to_lowercase();
            self.tag_bitmaps
                .entry(lowercase_tag)
                .or_insert_with(|| (RoaringBitmap::new(), original_tag.clone()))
                .0
                .insert(id);
        }
        if let Some(cb) = &self.on_progress {
            let total: u64 = self.tag_bitmaps.values().map(|(bm, _)| bm.len() as u64).sum();
            cb(total);
        }
    }

    /// Consume the builder and write out the `.tags` file.
    pub fn finish(self) -> Result<()> {
        let idx_path = self.idx_path;
        let tag_bitmaps = self.tag_bitmaps;

        // Serialize each bitmap and build the data entries
        let mut data_entries = Vec::with_capacity(tag_bitmaps.len());
        
        for (_tag, (bitmap, original_case)) in tag_bitmaps.iter() {
            // Serialize bitmap
            let mut bitmap_buf = Vec::new();
            bitmap.serialize_into(&mut bitmap_buf).map_err(IndexError::Io)?;
            
            // Serialize original case string to CBOR
            let mut cbor_buf = Vec::new();
            serde_cbor::to_writer(&mut cbor_buf, original_case).map_err(|e| {
                IndexError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("failed to serialize tag to CBOR: {}", e),
                ))
            })?;
            
            // Create data entry: (bitmap_len, bitmap, cbor)
            let mut entry = Vec::new();
            entry.extend_from_slice(&(bitmap_buf.len() as u32).to_le_bytes());
            entry.extend_from_slice(&bitmap_buf);
            entry.extend_from_slice(&cbor_buf);
            
            data_entries.push(entry);
        }

        // Build FST with memory offsets
        let mut fst_section = Vec::new();
        let mut fst_builder = MapBuilder::new(&mut fst_section)?;
        let mut current_offset = 0u64;
        
        for ((tag, _), entry) in tag_bitmaps.iter().zip(data_entries.iter()) {
            fst_builder.insert(tag, current_offset)?;
            current_offset += entry.len() as u64;
        }
        
        fst_builder.finish()?;

        // Write new format: header, FST, data entries
        let tags_path = idx_path.with_extension("tags");
        let tags_file = File::create(&tags_path)?;
        let mut writer = BufWriter::with_capacity(CHUNK_SIZE, tags_file);
        
        // Header: magic + version + fst_size
        writer.write_all(b"FTGT")?;
        writer.write_all(&2u16.to_le_bytes())?; // Version 2
        writer.write_all(&(fst_section.len() as u64).to_le_bytes())?; // FST_SIZE
        
        // FST section
        writer.write_all(&fst_section)?;
        
        // Data entries
        for entry in data_entries {
            writer.write_all(&entry)?;
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

