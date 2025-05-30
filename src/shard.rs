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
}
