use memmap2::Mmap;
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;

/// Shared memory map cloneable via reference counting.
#[derive(Clone)]
pub(crate) struct SharedMmap(Arc<Mmap>);

impl From<Mmap> for SharedMmap {
    fn from(m: Mmap) -> Self {
        SharedMmap(Arc::new(m))
    }
}

impl AsRef<[u8]> for SharedMmap {
    fn as_ref(&self) -> &[u8] {
        (&*self.0).as_ref()
    }
}
use fst::Map;
use serde::de::DeserializeOwned;

use crate::error::{IndexError, Result};
use crate::lookup_table_store::LookupTableStore;
use crate::payload_store::PayloadStore;
use crate::tag_index::TagIndex;
use fst::IntoStreamer;
use fst::raw::Fst as RawFst;
use roaring::bitmap::IntoIter;
use roaring::RoaringBitmap;
use fst::automaton::Str;
use fst::Automaton;
use fst::Streamer as FstStreamer;
/// One shard of a database: a memory-mapped FST map and its payload store.
pub struct Shard<V>
where
    V: DeserializeOwned,
{
    /// Memory-mapped raw index data (shared)
    pub(crate) idx_mmap: SharedMmap,
    /// Parsed FST map for lookups (backed by a shared memory map)
    pub(crate) fst: Map<SharedMmap>,
    /// Read-only lookup table for payload indirection
    pub(crate) lookup: LookupTableStore,
    /// Associated payload store
    pub(crate) payload: PayloadStore<V>,
    /// Optional tag index for this shard (variant-C .tags file)
    pub(crate) tags: Option<TagIndex>,
}

impl<V> Shard<V>
where
    V: DeserializeOwned,
{
    /// Open a shard from `<base>.idx` and `<base>.payload` files.
    pub fn open<P: AsRef<Path>>(base: P) -> Result<Self> {
        let base = base.as_ref();
        let idx_path = base.with_extension("idx");
        let payload_path = base.with_extension("payload");
        let idx_file = File::open(&idx_path).map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to open index file {:?}: {}", idx_path, e),
            ))
        })?;
        // Memory-map the index file (Mmap implements Clone cheaply)
        let idx_mmap = unsafe { Mmap::map(&idx_file) }.map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to mmap index file {:?}: {}", idx_path, e),
            ))
        })?;
        let idx_mmap = SharedMmap::from(idx_mmap);
        // Build FST from the memory map
        let fst = Map::new(idx_mmap.clone()).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("fst map error: {}", e),
            ))
        })?;
        let lookup_path = base.with_extension("lookup");
        let lookup = LookupTableStore::open(&lookup_path).map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to open lookup file {:?}: {}", lookup_path, e),
            ))
        })?;
        let payload = PayloadStore::open(&payload_path).map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to open payload file {:?}: {}", payload_path, e),
            ))
        })?;
        // Try loading a variant-C tags index if present
        let tags = match TagIndex::open(base) {
            Ok(idx) => Some(idx),
            Err(IndexError::Io(_)) | Err(IndexError::InvalidFormat(_)) => None,
        };
        Ok(Shard {
            idx_mmap,
            fst,
            lookup,
            payload,
            tags,
        })
    }

    /// Return the longest common prefix of all indexed keys in this shard.
    ///
    /// Walk the underlying FST from the root, following the unique outgoing
    /// transition at each node until a branching point or leaf is reached.
    /// The bytes traversed form the longest common prefix.
    pub fn common_prefix(&self) -> String {
        let raw = self.fst.as_fst();
        let mut node = raw.root();
        let mut prefix_bytes = Vec::new();
        while node.len() == 1 {
            let tr = node.transition(0);
            prefix_bytes.push(tr.inp);
            node = raw.node(tr.addr);
        }
        String::from_utf8_lossy(&prefix_bytes)
            .trim_end_matches('\u{FFFD}')
            .to_string()
    }

    /// Number of keys in this shard.
    pub fn len(&self) -> usize {
        self.fst.len()
    }

    /// Does this shard contain any key starting with `prefix`?
    pub fn has_prefix(&self, prefix: &str) -> bool {
        let starts = Str::new(prefix).starts_with();
        self.fst.search(starts).into_stream().next().is_some()
    }

    /// Try to retrieve the value for `key` from this shard.
    pub fn get_value(&self, key: &str) -> Result<Option<V>> {
        if let Some(weight) = self.fst.get(key) {
            let lut_entry = self.lookup.get(weight as u32)?;
            let v = self.payload.get(lut_entry.payload_ptr).map_err(|e| {
                IndexError::Io(io::Error::new(io::ErrorKind::Other, format!("payload error: {}", e)))
            })?;
            Ok(v)
        } else {
            Ok(None)
        }
    }

    /// Reverse lookup by raw pointer in this shard: retrieve the entry if present.
    pub fn get_entry(&self, ptr: u64) -> Result<Option<crate::Entry<V>>> {
        let raw_fst = RawFst::new(self.idx_mmap.clone()).map_err(IndexError::from)?;
        if let Some(key_bytes) = raw_fst.get_key(ptr) {
            let key = String::from_utf8(key_bytes.to_vec()).map_err(|e| {
                IndexError::Io(io::Error::new(io::ErrorKind::InvalidData, format!("invalid utf8 in key: {}", e)))
            })?;
            let lut_entry = self.lookup.get(ptr as u32)?;
            let value = self.payload.get(lut_entry.payload_ptr)?;
            Ok(Some(crate::Entry::Key(key, ptr, value)))
        } else {
            Ok(None)
        }
    }

    /// Stream entries matching the given tags (AND/OR mode),
    /// optionally restricted to those whose key starts with `prefix`.
    pub fn stream_by_tags<'a>(
        &'a self,
        tags: &[&str],
        mode: crate::TagMode,
        prefix: Option<&'a str>,
    ) -> Result<Box<dyn crate::Streamer<Item = crate::Entry<V>> + 'a>> {
        let mut combined: Option<RoaringBitmap> = None;
        if let Some(idx) = &self.tags {
            for &tag in tags {
                let mut bm = RoaringBitmap::new();
                if let Some(sub) = idx.get(tag)? {
                    bm |= sub;
                }
                combined = Some(match combined {
                    Some(mut acc) if mode == crate::TagMode::And => {
                        acc &= &bm;
                        acc
                    }
                    Some(mut acc) => {
                        acc |= &bm;
                        acc
                    }
                    None => bm,
                });
            }
        }
        let filter_bm = combined.unwrap_or_else(RoaringBitmap::new);

        struct ShardPtrStreamer<'a, V>
        where
            V: DeserializeOwned,
        {
            shard: &'a Shard<V>,
            ptr_iter: IntoIter,
            prefix: Option<&'a str>,
        }
        impl<'a, V> crate::Streamer for ShardPtrStreamer<'a, V>
        where
            V: DeserializeOwned,
        {
            type Item = crate::Entry<V>;
            fn next(&mut self) -> Option<Self::Item> {
                while let Some(ptr) = self.ptr_iter.next() {
                    if let Ok(Some(entry)) = (|| -> Result<Option<crate::Entry<V>>> {
                        let raw_fst = RawFst::new(self.shard.idx_mmap.clone()).map_err(crate::IndexError::from)?;
                        if let Some(key_bytes) = raw_fst.get_key(ptr as u64) {
                            let key = String::from_utf8(key_bytes.to_vec()).map_err(|e| {
                                crate::IndexError::Io(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("invalid utf8 in key: {}", e),
                                ))
                            })?;
                            let lut_entry = self.shard.lookup.get(ptr as u32)?;
                            let value = self.shard.payload.get(lut_entry.payload_ptr)?;
                            return Ok(Some(crate::Entry::Key(key, ptr as u64, value)));
                        }
                        Ok(None)
                    })() {
                        if let Some(pref) = self.prefix {
                            if let crate::Entry::Key(ref k, _, _) = entry {
                                if !k.starts_with(pref) {
                                    continue;
                                }
                            }
                        }
                        return Some(entry);
                    }
                }
                None
            }
        }

        Ok(Box::new(ShardPtrStreamer {
            shard: self,
            ptr_iter: filter_bm.into_iter(),
            prefix,
        }))
    }
}
