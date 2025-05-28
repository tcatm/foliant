use std::fs::File;
use std::io;
use std::path::Path;

use fst::map::Map;
use memmap2::Mmap;
use roaring::RoaringBitmap;

use crate::error::{IndexError, Result};

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
