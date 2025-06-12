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

/// In-memory handle to a variant-C tag index file (.tags), embedding an FST mapping
/// each tag to a packed 64-bit value (ordinal | bitmap_offset | bitmap_length),
/// followed by concatenated Roaring bitmap blobs, offset table, and CBOR string stream.
pub struct TagIndex {
    /// Underlying shared memory-mapped file
    mmap: crate::shard::SharedMmap,
    /// FST map over in-file FST section
    fst: Map<TagFstBacking>,
    /// Byte offset where the Roaring bitmap blobs begin
    blob_offset: usize,
    /// Byte offset where the bitmap blobs end
    bitmap_end: usize,
    /// Raw bytes of the offset table (little-endian u32 values)
    offsets_bytes: &'static [u8],
    /// CBOR string stream containing original case tag strings
    str_blob: &'static [u8],
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
    const HEADER_SIZE: usize = 4 + 2 + 8 + 8;
    const MAGIC: &'static [u8; 4] = b"FTGT";
    const VERSION: u16 = 2;
    
    // 64-bit value packing constants: ordinal (12) | offset (28) | length (24)
    const LEN_BITS: u64 = 24;
    const OFF_BITS: u64 = 28;
    const ORD_BITS: u64 = 12;
    
    const LEN_MASK: u64 = (1 << Self::LEN_BITS) - 1;
    const OFF_MASK: u64 = ((1 << Self::OFF_BITS) - 1) << Self::LEN_BITS;
    const ORD_MASK: u64 = ((1 << Self::ORD_BITS) - 1) << (Self::LEN_BITS + Self::OFF_BITS);
    
    // Size limits
    const MAX_TAGS: usize = (1 << Self::ORD_BITS) - 1; // 4095
    const MAX_BITMAP_LEN: usize = (1 << Self::LEN_BITS) - 1; // ~16MB per bitmap
    const MAX_BITMAP_OFFSET: usize = (1 << Self::OFF_BITS) - 1; // ~256MB total space
    
    /// Pack ordinal, bitmap offset, and bitmap length into a 64-bit value
    #[inline]
    fn pack_value(ordinal: u16, bitmap_offset: u32, bitmap_len: u32) -> u64 {
        let ord = (ordinal as u64) << (Self::LEN_BITS + Self::OFF_BITS);
        let off = (bitmap_offset as u64) << Self::LEN_BITS;
        let len = bitmap_len as u64;
        ord | off | len
    }
    
    /// Unpack a 64-bit value into ordinal, bitmap offset, and bitmap length
    #[inline]
    fn unpack_value(packed: u64) -> (u16, u32, u32) {
        let ordinal = ((packed & Self::ORD_MASK) >> (Self::LEN_BITS + Self::OFF_BITS)) as u16;
        let offset = ((packed & Self::OFF_MASK) >> Self::LEN_BITS) as u32;
        let length = (packed & Self::LEN_MASK) as u32;
        (ordinal, offset, length)
    }
    
    /// Read a u32 offset from the offset table at the given index
    #[inline]
    fn get_offset(&self, index: usize) -> Option<u32> {
        let start = index * 4;
        if start + 4 <= self.offsets_bytes.len() {
            let bytes: [u8; 4] = self.offsets_bytes[start..start + 4].try_into().ok()?;
            Some(u32::from_le_bytes(bytes))
        } else {
            None
        }
    }
    
    /// Get the number of offsets in the offset table
    #[inline]
    fn offset_count(&self) -> usize {
        self.offsets_bytes.len() / 4
    }

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
        let mmap_ref = mmap.clone();
        let buf = mmap_ref.as_ref();
        if buf.len() < Self::HEADER_SIZE {
            return Err(IndexError::InvalidFormat("tags file too small"));
        }
        if &buf[0..4] != Self::MAGIC {
            return Err(IndexError::InvalidFormat("invalid tags magic"));
        }
        let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        if version == 1 {
            // Legacy version without original case strings
            let fst_size = u64::from_le_bytes(buf[6..14].try_into().unwrap()) as usize;
            let fst_start = 4 + 2 + 8; // Old header size
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
            
            // Create empty slices for v1 compatibility
            let empty_bytes: &'static [u8] = &[];
            
            Ok(TagIndex {
                mmap,
                fst,
                blob_offset: fst_start + fst_len,
                bitmap_end: buf.len(),
                offsets_bytes: empty_bytes,
                str_blob: empty_bytes,
                original_case_cache: Mutex::new(None),
            })
        } else if version == Self::VERSION {
            let fst_size = u64::from_le_bytes(buf[6..14].try_into().unwrap()) as usize;
            let offtab_size = u64::from_le_bytes(buf[14..22].try_into().unwrap()) as usize;
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
            
            let blob_offset = fst_start + fst_len;
            
            // Calculate bitmap_end by scanning all FST values for max offset+length
            let mut max_bitmap_end = blob_offset;
            let mut stream = fst.stream();
            while let Some((_key, packed)) = stream.next() {
                let (_ordinal, offset, length) = Self::unpack_value(packed);
                let end = blob_offset + offset as usize + length as usize;
                max_bitmap_end = max_bitmap_end.max(end);
            }
            
            // Find aligned start of offset table
            let aligned_pos = (max_bitmap_end + 3) & !3; // Round up to next 4-byte boundary
            let offtab_start = aligned_pos;
            let str_start = offtab_start + offtab_size;
            
            if str_start > buf.len() {
                return Err(IndexError::InvalidFormat("file truncated before string section"));
            }
            
            // Get slices for offset table and string blob - map directly from mmap
            if offtab_size % 4 != 0 {
                return Err(IndexError::InvalidFormat("offset table size not aligned to u32"));
            }
            
            let offsets_bytes = &buf[offtab_start..str_start];
            
            // Check alignment for efficient u32 access
            if offsets_bytes.as_ptr() as usize % 4 != 0 {
                return Err(IndexError::InvalidFormat("offset table not aligned to u32 boundary"));
            }
            
            // Additional safety assert for ARM/32-bit targets
            assert_eq!(offsets_bytes.as_ptr() as usize & 3, 0, "offset table not 4-byte aligned");
            
            // Convert to static lifetime (safe because mmap lives as long as TagIndex)
            let offsets_bytes: &'static [u8] = unsafe { std::mem::transmute(offsets_bytes) };
            let str_blob: &'static [u8] = unsafe { std::mem::transmute(&buf[str_start..]) };
            
            Ok(TagIndex {
                mmap,
                fst,
                blob_offset,
                bitmap_end: max_bitmap_end,
                offsets_bytes,
                str_blob,
                original_case_cache: Mutex::new(None),
            })
        } else {
            return Err(IndexError::InvalidFormat("unsupported tags version"));
        }
    }

    /// Retrieve the RoaringBitmap for a given tag, or None if the tag is not present.
    pub fn get(&self, tag: &str) -> Result<Option<RoaringBitmap>> {
        if let Some(packed) = self.fst.get(tag) {
            let (offset, length) = if self.offsets_bytes.is_empty() {
                // v1 format: packed = (offset<<32 | length)
                let offset = (packed >> 32) as usize;
                let length = (packed & 0xffff_ffff) as usize;
                (offset, length)
            } else {
                // v2 format: packed = (ordinal<<40 | offset<<12 | length)
                let (_ordinal, offset, length) = Self::unpack_value(packed);
                (offset as usize, length as usize)
            };
            
            let start = self.blob_offset + offset;
            let end = start + length;
            if end > self.bitmap_end {
                return Err(IndexError::InvalidFormat("tags blob out of bounds"));
            }
            let slice = &self.mmap.as_ref()[start..end];
            let bmp = RoaringBitmap::deserialize_from(slice).map_err(|e| {
                IndexError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("failed to deserialize roaring bitmap: {}", e),
                ))
            })?;
            Ok(Some(bmp))
        } else {
            Ok(None)
        }
    }
}

impl TagIndex {
    /// Stream all tags present in this index, yielding each tag's raw bytes and its
    /// packed blob pointer (offset<<32 | length).  Use `get_bitmap` to decode the
    /// packed pointer into a RoaringBitmap when needed.
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
        
        let cache = if self.offsets_bytes.is_empty() {
            // Legacy v1 file without strings section
            HashMap::new()
        } else {
            // Build map from lowercase to original case using ordinals
            let mut map = HashMap::new();
            let mut stream = self.fst.stream();
            
            while let Some((key, packed)) = stream.next() {
                let lowercase_tag = std::str::from_utf8(key).map_err(|e| {
                    IndexError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        format!("invalid utf8 in tag key: {}", e),
                    ))
                })?;
                
                let (ordinal, _offset, _length) = Self::unpack_value(packed);
                
                if let Some(original_tag) = self.get_original_case_by_ordinal(ordinal)? {
                    map.insert(lowercase_tag.to_string(), original_tag);
                }
            }
            
            map
        };
        
        let cache_arc = Arc::new(cache);
        *cache_guard = Some(cache_arc.clone());
        Ok(cache_arc)
    }
    
    /// Get original case string by ordinal from CBOR stream
    fn get_original_case_by_ordinal(&self, ordinal: u16) -> Result<Option<String>> {
        if self.offsets_bytes.is_empty() {
            return Ok(None);
        }
        
        let ordinal_idx = ordinal as usize;
        if ordinal_idx >= self.offset_count() {
            return Ok(None);
        }
        
        let cbor_offset = self.get_offset(ordinal_idx).ok_or_else(|| {
            IndexError::InvalidFormat("failed to read offset from table")
        })? as usize;
        if cbor_offset >= self.str_blob.len() {
            return Err(IndexError::InvalidFormat("CBOR offset out of bounds"));
        }
        
        // Read exactly one CBOR value from the specified offset
        let slice = &self.str_blob[cbor_offset..];
        
        // Parse CBOR header to determine the exact length
        if slice.is_empty() {
            return Err(IndexError::InvalidFormat("CBOR data truncated"));
        }
        
        let major_type = slice[0] >> 5;
        let additional_info = slice[0] & 0x1f;
        
        // For text strings (major type 3), decode the length
        if major_type != 3 {
            return Err(IndexError::InvalidFormat("Expected CBOR text string"));
        }
        
        let (header_len, text_len) = if additional_info < 24 {
            (1, additional_info as usize)
        } else if additional_info == 24 {
            if slice.len() < 2 {
                return Err(IndexError::InvalidFormat("CBOR data truncated"));
            }
            (2, slice[1] as usize)
        } else if additional_info == 25 {
            if slice.len() < 3 {
                return Err(IndexError::InvalidFormat("CBOR data truncated"));
            }
            (3, u16::from_be_bytes([slice[1], slice[2]]) as usize)
        } else if additional_info >= 26 {
            return Err(IndexError::InvalidFormat("CBOR strings with 32-bit/64-bit lengths not supported"));
        } else {
            return Err(IndexError::InvalidFormat("Unsupported CBOR string length encoding"));
        };
        
        let total_len = header_len + text_len;
        if slice.len() < total_len {
            return Err(IndexError::InvalidFormat("CBOR string data truncated"));
        }
        
        // Extract just the text portion and convert to String
        let text_bytes = &slice[header_len..total_len];
        let original_tag = String::from_utf8(text_bytes.to_vec()).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("invalid UTF-8 in CBOR string: {}", e),
            ))
        })?;
        
        Ok(Some(original_tag))
    }

    /// Get all original case tags from the cache.
    /// Returns empty Vec if this is a legacy v1 file.
    pub fn get_original_tags(&self) -> Result<Vec<String>> {
        let cache = self.ensure_original_case_cache()?;
        Ok(cache.values().cloned().collect())
    }

    /// Get the original case version of a tag by its lowercase version.
    /// Returns None if the tag is not found or if this is a legacy v1 file.
    pub fn get_original_case(&self, lowercase_tag: &str) -> Result<Option<String>> {
        // Fast path: try direct lookup if we have the FST entry (for v2 files)
        if !self.offsets_bytes.is_empty() {
            if let Some(packed) = self.fst.get(lowercase_tag) {
                let (ordinal, _offset, _length) = Self::unpack_value(packed);
                return self.get_original_case_by_ordinal(ordinal);
            }
        }
        
        // Fallback to cache for v1 files or when direct lookup fails
        let cache = self.ensure_original_case_cache()?;
        Ok(cache.get(lowercase_tag).cloned())
    }

    /// Retrieve the RoaringBitmap for a packed tag pointer obtained from `list_tags`.
    pub fn get_bitmap(&self, packed: u64) -> Result<RoaringBitmap> {
        let (offset, length) = if self.offsets_bytes.is_empty() {
            // v1 format: packed = (offset<<32 | length)
            let offset = (packed >> 32) as usize;
            let length = (packed & 0xffff_ffff) as usize;
            (offset, length)
        } else {
            // v2 format: packed = (ordinal<<40 | offset<<12 | length)
            let (_ordinal, offset, length) = Self::unpack_value(packed);
            (offset as usize, length as usize)
        };
        
        let start = self.blob_offset + offset;
        let end = start + length;
        if end > self.bitmap_end {
            return Err(IndexError::InvalidFormat("tags blob out of bounds"));
        }
        let slice = &self.mmap.as_ref()[start..end];
        let bmp = RoaringBitmap::deserialize_from(slice).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to deserialize roaring bitmap: {}", e),
            ))
        })?;
        Ok(bmp)
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

        // Size validation
        if tag_bitmaps.len() > TagIndex::MAX_TAGS {
            return Err(IndexError::InvalidFormat("too many tags (max 4095)"));
        }

        // Serialize each bitmap into its own Vec<u8>
        let mut blobs = Vec::with_capacity(tag_bitmaps.len());
        for (_tag, (bitmap, _)) in tag_bitmaps.iter() {
            let mut buf = Vec::new();
            bitmap.serialize_into(&mut buf).map_err(IndexError::Io)?;
            
            // Check if length fits in 24 bits
            if buf.len() > TagIndex::MAX_BITMAP_LEN {
                return Err(IndexError::InvalidFormat("bitmap too large (max ~16MB per bitmap)"));
            }
            
            blobs.push(buf);
        }

        // Collect display strings in sorted order (same as BTreeMap iteration)
        let display_strings: Vec<String> = tag_bitmaps.values().map(|(_, original)| original.clone()).collect();
        
        // Build offset table and CBOR stream
        let mut offsets = Vec::<u32>::new();
        let mut cbor_stream = Vec::<u8>::new();
        
        for s in &display_strings {
            offsets.push(cbor_stream.len() as u32);
            serde_cbor::to_writer(&mut cbor_stream, s).map_err(|e| {
                IndexError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("failed to serialize tag to CBOR: {}", e),
                ))
            })?;
        }

        // Build FST with new packed format: ordinal | bitmap_offset | bitmap_length
        let mut fst_section = Vec::new();
        let mut fst_builder = MapBuilder::new(&mut fst_section)?;
        let mut bitmap_offset = 0u32;
        
        for (ordinal, ((tag, _), blob_data)) in tag_bitmaps.iter().zip(blobs.iter()).enumerate() {
            let ordinal = ordinal as u16;
            let blob_len = blob_data.len() as u32;
            
            if bitmap_offset as usize > TagIndex::MAX_BITMAP_OFFSET {
                return Err(IndexError::InvalidFormat("bitmap offset too large (max ~256MB total)"));
            }
            
            let packed = TagIndex::pack_value(ordinal, bitmap_offset, blob_len);
            fst_builder.insert(tag, packed)?;
            bitmap_offset += blob_len as u32;
        }
        
        // Final check that total bitmap size doesn't exceed ~256MB after all increments
        if bitmap_offset as usize > TagIndex::MAX_BITMAP_OFFSET {
            return Err(IndexError::InvalidFormat("bitmap blob > ~256MiB total"));
        }
        
        fst_builder.finish()?;

        // Convert offsets to bytes for writing
        let mut offsets_bytes = Vec::with_capacity(offsets.len() * 4);
        for offset in offsets {
            offsets_bytes.extend_from_slice(&offset.to_le_bytes());
        }

        // Write new format: header, FST, bitmaps, offset table, CBOR stream
        let tags_path = idx_path.with_extension("tags");
        let tags_file = File::create(&tags_path)?;
        let mut writer = BufWriter::with_capacity(CHUNK_SIZE, tags_file);
        
        // Header
        writer.write_all(b"FTGT")?;
        writer.write_all(&2u16.to_le_bytes())?; // Version 2
        writer.write_all(&(fst_section.len() as u64).to_le_bytes())?; // FST_SIZE
        writer.write_all(&(offsets_bytes.len() as u64).to_le_bytes())?; // OFFTAB_SIZE
        
        // FST section
        writer.write_all(&fst_section)?;
        
        // Bitmap blobs
        for blob in blobs {
            writer.write_all(&blob)?;
        }
        
        // Ensure offset table starts at 4-byte aligned position
        let current_pos = TagIndex::HEADER_SIZE + fst_section.len() + bitmap_offset as usize;
        let aligned_pos = (current_pos + 3) & !3; // Round up to next 4-byte boundary
        let padding = aligned_pos - current_pos;
        writer.write_all(&vec![0; padding])?;
        
        // Offset table (no adjustment needed - offsets are already relative to string section)
        writer.write_all(&offsets_bytes)?;
        
        // CBOR stream
        writer.write_all(&cbor_stream)?;
        
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

