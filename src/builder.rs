use std::path::{Path, PathBuf};
use std::fs::File;
use std::io;
use std::io::BufWriter;
use fst::MapBuilder;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_cbor::Value;

use crate::payload_store::PayloadStoreBuilder;
use crate::lookup_table_store::LookupTableStoreBuilder;
use crate::error::{IndexError, Result};
use crate::database::Database;

const CHUNK_SIZE: usize = 128 * 1024;

/// Builder for creating a new on-disk database with values of type V.
/// Insert keys with `insert()`, then call `close()` or `into_database()`.
pub struct DatabaseBuilder<V = Value>
where
    V: Serialize,
{
    base: PathBuf,
    fst_builder: MapBuilder<BufWriter<File>>,
    lookup_store: LookupTableStoreBuilder,
    payload_store: PayloadStoreBuilder<V>,
}

impl<V: Serialize> DatabaseBuilder<V> {
    /// Create a new database builder writing to `<base>.idx`, `<base>.lookup`, and `<base>.payload` (truncating existing).
    pub fn new<P: AsRef<Path>>(base: P) -> Result<Self> {
        let base = base.as_ref().to_path_buf();
        let idx_path = base.with_extension("idx");
        let lookup_path = base.with_extension("lookup");
        let payload_path = base.with_extension("payload");

        let fst_file = File::create(&idx_path)
            .map_err(|e| IndexError::Io(io::Error::new(e.kind(), format!("failed to create index file: {}", e))))?;

        let fst_writer = BufWriter::with_capacity(CHUNK_SIZE, fst_file);
        let fst_builder = MapBuilder::new(fst_writer)
            .map_err(|e| IndexError::Io(io::Error::new(io::ErrorKind::Other, format!("failed to create index builder: {}", e))))?;

        let lookup_store = LookupTableStoreBuilder::open(&lookup_path)?;
        let payload_store = PayloadStoreBuilder::<V>::open(&payload_path)?;
        Ok(DatabaseBuilder { base, fst_builder, lookup_store, payload_store })
    }

    /// Insert a key with an optional value `V` into the database.
    pub fn insert(&mut self, key: &str, value: Option<V>) {
        let payload_ptr = self.payload_store.append(value)
            .expect("payload append failed");
        let lut_ptr = self.lookup_store.append(payload_ptr)
            .expect("lookup append failed");
        self.fst_builder.insert(key, lut_ptr)
            .expect("FST insert failed");
    }

    /// Finalize and write index and payload files to disk, batching writes via a buffer.
    pub fn close(self) -> Result<()> {
        self.fst_builder
            .finish()
            .map_err(|e| IndexError::Io(io::Error::new(io::ErrorKind::Other, format!("failed to finalize index builder: {}", e))))?;

        self.lookup_store.close()?;
        self.payload_store.close()?;
        Ok(())
    }

    /// Consume the builder, write files, and open a read-only Database<V> via mmap.
    pub fn into_database(self) -> Result<Database<V>>
    where
        V: DeserializeOwned,
    {
        let base = self.base.clone();
        self.close()?;
        Database::<V>::open(base)
    }
}
