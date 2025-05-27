use fst::map::Map;
use fst::MapBuilder;
use fst::Streamer;
use memmap2::Mmap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_cbor::Value;
use std::convert::TryInto;
use std::fs;
use std::fs::File;
use std::io;
use std::io::{BufWriter};
use std::path::{Path, PathBuf};

use crate::database::Database;
use crate::error::{IndexError, Result};
use crate::lookup_table_store::LookupTableStoreBuilder;
use crate::payload_store::PayloadStoreBuilder;

const CHUNK_SIZE: usize = 128 * 1024;
const INSERT_BATCH_SIZE: usize = 10_000;

/// Builder for creating a new on-disk database with values of type V.
/// Insert keys with `insert()`, then call `close()` or `into_database()`.
pub struct DatabaseBuilder<V = Value>
where
    V: Serialize,
{
    base: PathBuf,
    payload_store: PayloadStoreBuilder<V>,
    lookup_store: LookupTableStoreBuilder,
    buffer: Vec<(Vec<u8>, u32)>,
    partials: Vec<PartialBuilder>,
}

/// Internal partial FST builder segment (on-disk).
struct PartialBuilder {
    idx_path: PathBuf,
    builder: MapBuilder<BufWriter<File>>,
    last_key: Vec<u8>,
}

impl PartialBuilder {
    /// Create a new partial builder writing to `<base>.idx.<seg_idx>`.
    fn new(base: &Path, seg_idx: usize) -> Result<Self> {
        let idx_path = base.with_extension(format!("idx.{}", seg_idx));
        let fst_file = File::create(&idx_path).map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!(
                    "failed to create partial index file {}: {}",
                    idx_path.display(),
                    e
                ),
            ))
        })?;
        let fst_writer = BufWriter::with_capacity(CHUNK_SIZE, fst_file);
        let builder = MapBuilder::new(fst_writer).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "failed to create index builder for {}: {}",
                    idx_path.display(),
                    e
                ),
            ))
        })?;
        Ok(PartialBuilder {
            idx_path,
            builder,
            last_key: Vec::new(),
        })
    }

    fn append_batch(
        &mut self,
        entries: &mut Vec<(Vec<u8>, u32)>,
        lookup: &mut LookupTableStoreBuilder,
    ) {
        for (key, payload_ptr) in entries.drain(..) {
            let lut_ptr = lookup
                .append(payload_ptr)
                .expect("lookup append failed in batch flush");
            self.builder
                .insert(&key, lut_ptr.into())
                .expect("FST insert failed in batch flush");
            self.last_key = key;
        }
    }

    /// Finalize builder, close the FST, and return its file path.
    fn finish(self) -> Result<PathBuf> {
        self.builder.finish().map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "failed to finalize index builder {}: {}",
                    self.idx_path.display(),
                    e
                ),
            ))
        })?;
        Ok(self.idx_path)
    }
}

impl<V: Serialize> DatabaseBuilder<V> {
    /// Create a new database builder writing to `<base>.payload`, buffering keys for sorted insertion.
    pub fn new<P: AsRef<Path>>(base: P) -> Result<Self> {
        let base = base.as_ref().to_path_buf();
        let payload_path = base.with_extension("payload");
        let payload_store = PayloadStoreBuilder::<V>::open(&payload_path)?;
        let lut_path = base.with_extension("lookup");
        let lookup_store = LookupTableStoreBuilder::open(&lut_path).map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to open lookup file {:?}: {}", lut_path.display(), e),
            ))
        })?;
        Ok(DatabaseBuilder {
            base,
            payload_store,
            lookup_store,
            buffer: Vec::with_capacity(INSERT_BATCH_SIZE),
            partials: Vec::new(),
        })
    }

    /// Insert a key with an optional value `V` into the database.
    /// Buffers up to INSERT_BATCH_SIZE entries, then sorts and writes them to partial FST segments.
    pub fn insert(&mut self, key: &str, value: Option<V>) {
        let payload_ptr = self
            .payload_store
            .append(value)
            .expect("payload append failed");
        self.buffer.push((key.as_bytes().to_vec(), payload_ptr));
        if self.buffer.len() >= INSERT_BATCH_SIZE {
            self.flush_buffer().expect("failed to flush batch");
        }
    }

    /// Flush buffered entries into partial FST segment(s).
    fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.buffer.sort_by(|a, b| a.0.cmp(&b.0));
        match self.partials.last_mut() {
            Some(pb) => {
                let first_key = &self.buffer[0].0;
                if first_key >= &pb.last_key {
                    pb.append_batch(&mut self.buffer, &mut self.lookup_store);
                } else {
                    let seg_idx = self.partials.len();
                    let mut new_pb = PartialBuilder::new(&self.base, seg_idx)?;
                    new_pb.append_batch(&mut self.buffer, &mut self.lookup_store);
                    self.partials.push(new_pb);
                }
            }
            None => {
                let mut pb = PartialBuilder::new(&self.base, 0)?;
                pb.append_batch(&mut self.buffer, &mut self.lookup_store);
                self.partials.push(pb);
            }
        }
        Ok(())
    }

    /// Flush buffered entries into partial FST segments, merging with existing segments when possible.
    pub fn flush_fst(&mut self) -> Result<()> {
        self.flush_buffer()
    }

    /// Finalize and write index and payload files to disk.
    pub fn close(mut self) -> Result<()> {
        self.flush_fst()?;
        // Finalize partial segments to disk and merge if needed
        let partial_paths: Vec<PathBuf> = self
            .partials
            .into_iter()
            .map(|pb| pb.finish())
            .collect::<Result<Vec<_>>>()?;
        self.payload_store.close()?;
        match partial_paths.len() {
            0 => {
                // No entries: write an empty .idx
                let idx_path = self.base.with_extension("idx");
                let fst_file = File::create(&idx_path).map_err(IndexError::from)?;
                let fst_writer = BufWriter::with_capacity(CHUNK_SIZE, fst_file);
                let builder = MapBuilder::new(fst_writer).map_err(IndexError::from)?;
                builder.finish().map_err(IndexError::from)?;
                // Close lookup table
                self.lookup_store.close()?;
            }
            1 => {
                // Single segment: rename to <base>.idx
                let src = &partial_paths[0];
                let dst = self.base.with_extension("idx");
                fs::rename(src, &dst).map_err(IndexError::from)?;
                // Close lookup table
                self.lookup_store.close()?;
            }
            _ => {
                // Merge multiple segments into final index, reassign lookup IDs for final keys.
                // Flush and close existing lookup table to ensure old entries are on disk.
                self.lookup_store.close().map_err(IndexError::from)?;

                // Read existing lookup entries.
                let lut_path = self.base.with_extension("lookup");
                let lut_file = File::open(&lut_path).map_err(IndexError::from)?;
                let lut_mmap = unsafe { Mmap::map(&lut_file) }.map_err(IndexError::from)?;
                let mut old_lut = Vec::with_capacity(lut_mmap.len() / 4);
                for chunk in lut_mmap.chunks_exact(4) {
                    old_lut.push(u32::from_le_bytes(chunk.try_into().unwrap()));
                }

                // Re-open lookup table for writing new entries, truncating the old file.
                self.lookup_store =
                    LookupTableStoreBuilder::open(&lut_path).map_err(IndexError::from)?;

                let final_idx = self.base.with_extension("idx");
                let final_file = File::create(&final_idx).map_err(IndexError::from)?;
                let final_writer = BufWriter::with_capacity(CHUNK_SIZE, final_file);
                let mut final_builder = MapBuilder::new(final_writer).map_err(IndexError::from)?;

                // Merge partial FST segments with k-way merge.
                let mut maps = Vec::new();
                for path in &partial_paths {
                    let file = File::open(path).map_err(IndexError::from)?;
                    let mmap = unsafe { Mmap::map(&file) }.map_err(IndexError::from)?;
                    let map = Map::new(mmap).map_err(IndexError::from)?;
                    maps.push(map);
                }
                let mut streams: Vec<_> = maps.iter().map(|m| m.stream()).collect();
                let mut heads: Vec<Option<(Vec<u8>, u64)>> = streams
                    .iter_mut()
                    .map(|s| s.next().map(|(k, v)| (k.to_vec(), v)))
                    .collect();

                // Remove partial files.
                for path in &partial_paths {
                    fs::remove_file(path).map_err(IndexError::from)?;
                }

                // Rebuild lookup table and FST together in sorted order.
                while heads.iter().any(Option::is_some) {
                    // Find index of smallest key.
                    let mut min_idx: Option<usize> = None;
                    let mut min_key: Option<&Vec<u8>> = None;
                    for (i, head) in heads.iter().enumerate() {
                        if let Some((ref key, _)) = head {
                            if min_idx.is_none() || key < min_key.unwrap() {
                                min_idx = Some(i);
                                min_key = Some(key);
                            }
                        }
                    }
                    let i = min_idx.unwrap();
                    let (key_vec, val) = heads[i].take().unwrap();
                    let payload_ptr = old_lut[val as usize - 1];
                    let new_lut_id = self
                        .lookup_store
                        .append(payload_ptr)
                        .map_err(IndexError::from)?;
                    final_builder
                        .insert(&key_vec, new_lut_id.into())
                        .expect("FST insert failed during merge");
                    heads[i] = streams[i].next().map(|(k, v)| (k.to_vec(), v));
                    // Skip duplicates.
                    for (j, head) in heads.iter_mut().enumerate() {
                        if j != i {
                            if let Some((ref other_key, _)) = head {
                                if other_key == &key_vec {
                                    *head = streams[j].next().map(|(k, v)| (k.to_vec(), v));
                                }
                            }
                        }
                    }
                }
                final_builder.finish().map_err(IndexError::from)?;

                // Close the rebuilt lookup table.
                self.lookup_store.close().map_err(IndexError::from)?;
            }
        }
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
