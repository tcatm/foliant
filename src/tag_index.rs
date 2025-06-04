use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use crate::shard::Shard;
use fst::automaton::Str;
use fst::map::Map;
use fst::Automaton;
use fst::IntoStreamer;
use fst::Streamer as FstStreamer;
use memmap2::Mmap;
use roaring::RoaringBitmap;
use std::sync::Arc;

use crate::builder::CHUNK_SIZE;
use crate::error::{IndexError, Result};
use crate::Database;
use fst::map::MapBuilder;
use serde_json;
use std::collections::BTreeMap;
use std::io::{BufWriter, Write};

/// In-memory handle to a variant-C tag index file (.tags), embedding an FST mapping
/// each tag to a packed (offset_in_blob<<32 | length) value, followed by concatenated
/// raw Roaring bitmap blobs.
pub struct TagIndex {
    /// Underlying shared memory-mapped file
    mmap: crate::shard::SharedMmap,
    /// FST map over in-file FST section
    fst: Map<TagFstBacking>,
    /// Byte offset where the Roaring bitmap blobs begin
    blob_offset: usize,
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
    const VERSION: u16 = 1;

    /// Open a variant-C tag index at `<base>.tags`, validating header and preparing FST.
    pub fn open<P: AsRef<Path>>(base: P) -> Result<Self> {
        let path = base.as_ref().with_extension("tags");
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
        if version != Self::VERSION {
            return Err(IndexError::InvalidFormat("unsupported tags version"));
        }
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
        Ok(TagIndex {
            mmap,
            fst,
            blob_offset: fst_start + fst_len,
        })
    }

    /// Retrieve the RoaringBitmap for a given tag, or None if the tag is not present.
    pub fn get(&self, tag: &str) -> Result<Option<RoaringBitmap>> {
        if let Some(packed) = self.fst.get(tag) {
            let packed = packed;
            let offset_in_blob = (packed >> 32) as usize;
            let len = (packed & 0xffff_ffff) as usize;
            let start = self.blob_offset + offset_in_blob;
            let end = start + len;
            if end > self.mmap.as_ref().len() {
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
    /// List all tags present in this index.
    pub fn list_tags(&self) -> Result<Vec<String>> {
        let mut tags = Vec::new();
        let mut stream = self.fst.search(Str::new("").starts_with()).into_stream();
        while let Some((tag, _)) = stream.next() {
            tags.push(String::from_utf8_lossy(tag).into_owned());
        }
        Ok(tags)
    }
}

/// Builder for creating a variant-C tag index file (`<base>.tags`).
pub struct TagIndexBuilder {
    base: PathBuf,
    tag_bitmaps: BTreeMap<String, RoaringBitmap>,
    /// Optional hook that gets called after each insert_tags, passing the total count so far.
    on_progress: Option<Box<dyn Fn(u64)>>,
}

impl TagIndexBuilder {
    /// Create a new TagIndexBuilder for writing `<base>.tags`.
    pub fn new<P: AsRef<Path>>(base: P) -> Self {
        TagIndexBuilder {
            base: base.as_ref().to_path_buf(),
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
            self.tag_bitmaps
                .entry(tag.into())
                .or_insert_with(RoaringBitmap::new)
                .insert(id);
        }
        if let Some(cb) = &self.on_progress {
            let total: u64 = self.tag_bitmaps.values().map(|bm| bm.len() as u64).sum();
            cb(total);
        }
    }

    /// Consume the builder and write out the `.tags` file.
    pub fn finish(self) -> Result<()> {
        let base = self.base;
        let tag_bitmaps = self.tag_bitmaps;
        if tag_bitmaps.is_empty() {
            return Ok(());
        }

        // Serialize each bitmap into its own Vec<u8>
        let mut blobs = Vec::with_capacity(tag_bitmaps.len());
        for bitmap in tag_bitmaps.values() {
            let mut buf = Vec::new();
            bitmap.serialize_into(&mut buf).map_err(IndexError::Io)?;
            blobs.push(buf);
        }

        // Build an FST mapping tag -> packed(offset<<32 | length)
        let mut fst_section = Vec::new();
        let mut fst_builder = MapBuilder::new(&mut fst_section)?;
        let mut offset = 0u64;
        for ((tag, _), blob_data) in tag_bitmaps.iter().zip(blobs.iter()) {
            let len = blob_data.len() as u64;
            let packed = (offset << 32) | len;
            fst_builder.insert(tag, packed)?;
            offset += len;
        }
        fst_builder.finish()?;

        // Write header, FST section, and blobs to `<base>.tags`
        let tags_path = base.with_extension("tags");
        let tags_file = File::create(&tags_path)?;
        let mut writer = BufWriter::with_capacity(CHUNK_SIZE, tags_file);
        writer.write_all(b"FTGT")?;
        writer.write_all(&1u16.to_le_bytes())?;
        writer.write_all(&(fst_section.len() as u64).to_le_bytes())?;
        writer.write_all(&fst_section)?;
        for blob in blobs {
            writer.write_all(&blob)?;
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
        let base = shard.base();
        let mut builder = TagIndexBuilder::new(base);
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
    /// and write out `<base>.tags`. The optional `on_progress` callback receives
    /// the cumulative tag-entry count after each record.
    pub fn build<P: AsRef<Path>>(
        base: P,
        tag_field: &str,
        on_progress: Option<Arc<dyn Fn(u64)>>,
    ) -> Result<()> {
        let shard = Shard::<serde_json::Value>::open(base.as_ref())?;
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
            shard.tags = Some(TagIndex::open(shard.base())?);
        }
        Ok(())
    }
}
