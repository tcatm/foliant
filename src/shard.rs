use std::fs::File;
use std::path::Path;
use std::io;
use memmap2::Mmap;
use fst::Map;
use serde::de::DeserializeOwned;

use crate::payload_store::PayloadStore;
use crate::lookup_table_store::LookupTableStore;
use crate::error::{IndexError, Result};

/// One shard of a database: a memory-mapped FST map, a lookup table store, and its payload store.
pub struct Shard<V>
where
    V: DeserializeOwned,
{
    pub(crate) fst: Map<Mmap>,
    pub(crate) lookup: LookupTableStore,
    pub(crate) payload: PayloadStore<V>,
}

impl<V> Shard<V>
where
    V: DeserializeOwned,
{
    /// Open a shard from `<base>.idx` and `<base>.payload` files.
    pub fn open<P: AsRef<Path>>(base: P) -> Result<Self> {
        let base = base.as_ref();
        let idx_path = base.with_extension("idx");
        let lookup_path = base.with_extension("lookup");
        let payload_path = base.with_extension("payload");
        let idx_file = File::open(&idx_path)
            .map_err(|e| IndexError::Io(io::Error::new(e.kind(), format!("failed to open index file {:?}: {}", idx_path, e))))?;
        let idx_mmap = unsafe { Mmap::map(&idx_file) }
            .map_err(|e| IndexError::Io(io::Error::new(e.kind(), format!("failed to mmap index file {:?}: {}", idx_path, e))))?;
        let fst = Map::new(idx_mmap)
            .map_err(|e| IndexError::Io(io::Error::new(io::ErrorKind::Other, format!("fst map error: {}", e))))?;
        let lookup = LookupTableStore::open(&lookup_path)
            .map_err(|e| IndexError::Io(io::Error::new(e.kind(), format!("failed to open lookup file {:?}: {}", lookup_path, e))))?;
        let payload = PayloadStore::open(&payload_path)
            .map_err(|e| IndexError::Io(io::Error::new(e.kind(), format!("failed to open payload file {:?}: {}", payload_path, e))))?;
        Ok(Shard { fst, lookup, payload })
    }
}
