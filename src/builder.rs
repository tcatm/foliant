use fst::map::{Map, OpBuilder};
use fst::IntoStreamer;
use fst::MapBuilder;
use fst::Streamer;
use memmap2::MmapOptions;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_cbor::Value;
use std::fs::{remove_file, rename, File, OpenOptions};
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use crate::database::Database;
use crate::error::{IndexError, Result};
use crate::lookup_table_store::{LookupTableStore, LookupTableStoreBuilder};
use crate::payload_store::{CborPayloadCodec, PayloadCodec, PayloadStoreBuilderV2};

pub(crate) const CHUNK_SIZE: usize = 128 * 1024;
const INSERT_BATCH_SIZE: usize = 10_000;
use crc32fast::Hasher;
use std::io::{Seek, SeekFrom, Write};

/// One 16-byte trailer per sub-FST: [seg_id: u32 | start_off: u64 | crc32: u32]
#[repr(C, packed)]
#[derive(Copy, Clone, Debug)]
struct SegmentTrailer {
    seg_id: u32,
    start_off: u64,
    crc32: u32,
}

/// Statistics for one inline FST segment (before final merge).
#[derive(Debug)]
pub struct SegmentInfo {
    /// 0-based segment ID, as written in the trailer.
    pub seg_id: u32,
    /// Byte length of the segment's FST image (excluding the trailer).
    pub size_bytes: usize,
}

impl SegmentTrailer {
    pub const SIZE: usize = std::mem::size_of::<SegmentTrailer>();

    #[inline]
    pub fn from_bytes(buf: &[u8]) -> SegmentTrailer {
        unsafe { std::ptr::read_unaligned(buf.as_ptr() as *const _) }
    }

    #[inline]
    pub fn compute_crc(&self) -> u32 {
        let mut h = Hasher::new();
        let raw =
            unsafe { std::slice::from_raw_parts((self as *const SegmentTrailer) as *const u8, 12) };
        h.update(raw);
        h.finalize()
    }

    #[inline]
    pub fn validate_crc(&self) -> bool {
        self.crc32 == self.compute_crc()
    }
}

/// Inline builder writing sub-FSTs directly into `<base>.idx` with trailers.
struct IndexedBuilder {
    /// Path to the temporary idx file (inline segments are written here).
    idx_tmp_path: PathBuf,
    /// Path to the temporary lookup file (lookup table is built here).
    lut_tmp_path: PathBuf,
    /// Optional writer for the idx file (owns the file handle across segments).
    writer: Option<BufWriter<File>>,
    /// Active FST builder for the current segment, if any; owns its own writer while building.
    builder: Option<MapBuilder<BufWriter<File>>>,
    lookup: LookupTableStoreBuilder,
    /// Next segment ID to assign for a new FST segment.
    next_seg: u32,
    /// Last key inserted (for lexicographic segment roll-over checks).
    last_key: Vec<u8>,
    /// Byte offset in the idx file where the current segment started.
    current_start: u64,
}

impl IndexedBuilder {
    pub fn new(base: &Path) -> Result<Self> {
        // Prepare temporary paths for the inline index and lookup files.
        let idx_tmp_path = base.with_extension("idx.tmp");
        let lut_tmp_path = base.with_extension("lookup.tmp");
        // Create/truncate the temp idx writer and lookup builder.
        let file = File::create(&idx_tmp_path)?;
        let writer = BufWriter::with_capacity(CHUNK_SIZE, file);
        let lookup = LookupTableStoreBuilder::open(&lut_tmp_path)?;
        Ok(IndexedBuilder {
            idx_tmp_path,
            lut_tmp_path,
            writer: Some(writer),
            builder: None,
            lookup,
            next_seg: 0,
            last_key: Vec::new(),
            current_start: 0,
        })
    }

    /// Start a fresh inline FST segment at the current end of the idx file.
    fn start_new_segment(&mut self) -> Result<()> {
        let mut writer = self.writer.take().unwrap();
        let start = writer.seek(SeekFrom::End(0))?;
        let mb = MapBuilder::new(writer)?;
        self.current_start = start;
        self.builder = Some(mb);
        Ok(())
    }

    /// Finish the current FST segment, write its trailer, and advance segment counter.
    fn finish_current_segment(&mut self) -> Result<()> {
        let mb = self.builder.take().unwrap();
        let mut writer = mb.into_inner()?;

        let mut trailer = SegmentTrailer {
            seg_id: self.next_seg,
            start_off: self.current_start,
            crc32: 0,
        };
        trailer.crc32 = trailer.compute_crc();
        let bytes = unsafe {
            std::slice::from_raw_parts(
                (&trailer as *const SegmentTrailer) as *const u8,
                SegmentTrailer::SIZE,
            )
        };
        writer.write_all(bytes)?;
        writer.flush()?;
        self.next_seg += 1;
        self.writer = Some(writer);
        Ok(())
    }

    pub fn append_batch(&mut self, entries: &mut Vec<(Vec<u8>, u64)>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        // If first batch ever, or if this batch's first key would break the sort order,
        // finalize the previous segment (if any) and start a new one.
        let first_key = &entries[0].0;
        if let Some(_) = self.builder {
            if first_key < &self.last_key {
                self.finish_current_segment()?;
            }
        }
        if self.builder.is_none() {
            self.start_new_segment()?;
        }

        // Insert all entries into the active FST builder.
        let mb = self.builder.as_mut().unwrap();
        for (key, payload_ptr) in entries.drain(..) {
            let lut_id = self.lookup.append(payload_ptr)?;
            mb.insert(&key, lut_id.into())?;
            self.last_key = key;
        }
        Ok(())
    }

    /// Finalize any in-flight segment, close lookup store,
    /// and return number of segments written plus temp file paths.
    pub fn finish(mut self) -> Result<(u32, PathBuf, PathBuf)> {
        if self.builder.is_some() {
            self.finish_current_segment()?;
        }
        self.lookup.close()?;
        Ok((self.next_seg, self.idx_tmp_path, self.lut_tmp_path))
    }
}

/// Builder for creating a new on-disk database with values of type `V`, encoded via `C`.
/// Insert keys with `insert()`, then call `close()` or `into_database()`.
pub struct DatabaseBuilder<V = Value, C: PayloadCodec = CborPayloadCodec>
where
    V: Serialize,
    C: PayloadCodec,
{
    base: PathBuf,
    payload_store: PayloadStoreBuilderV2<V, C>,
    buffer: Vec<(Vec<u8>, u64)>,
    idx_builder: Option<IndexedBuilder>,
    /// Optional callback for each key merged during final index write.
    write_cb: Box<dyn FnMut()>,
    /// Optional callback invoked once before merging segments, with per-segment stats.
    before_merge_cb: Box<dyn FnMut(&[SegmentInfo])>,
}

impl<V, C> DatabaseBuilder<V, C>
where
    V: Serialize,
    C: PayloadCodec,
{
    /// Create a new database builder writing to `<base>.payload`, buffering keys for sorted insertion.
    pub fn new<P: AsRef<Path>>(base: P) -> Result<Self> {
        let base = base.as_ref().to_path_buf();
        let payload_path = base.with_extension("payload");
        let payload_store = PayloadStoreBuilderV2::<V, C>::open(&payload_path)?;

        Ok(DatabaseBuilder::<V, C> {
            base,
            payload_store,
            buffer: Vec::with_capacity(INSERT_BATCH_SIZE),
            idx_builder: None,
            write_cb: Box::new(|| {}),
            before_merge_cb: Box::new(|_stats| {}),
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

    /// Flush buffered entries into the inline index, creating sub-FST segments as needed.
    pub fn flush_fst(&mut self) -> Result<()> {
        self.flush_buffer()
    }

    /// Flush buffered entries into inline idx builder.
    fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.buffer.sort_by(|a, b| a.0.cmp(&b.0));

        if self.idx_builder.is_none() {
            self.idx_builder = Some(IndexedBuilder::new(&self.base)?);
        }
        self.idx_builder
            .as_mut()
            .unwrap()
            .append_batch(&mut self.buffer)?;
        Ok(())
    }

    /// Finalize the index file, merging inline sub-FSTs if any; then close payloads.
    pub fn close(mut self) -> Result<()> {
        self.flush_fst()?;

        if let Some(ib) = self.idx_builder.take() {
            let (seg_count, tmp_idx, tmp_lut) = ib.finish()?;
            let infos = load_submaps_raw(&tmp_idx)?;
            let stats: Vec<SegmentInfo> = infos
                .iter()
                .map(|(trailer, bytes)| SegmentInfo {
                    seg_id: trailer.seg_id,
                    size_bytes: bytes.len(),
                })
                .collect();
            (self.before_merge_cb)(&stats);

            let final_idx = self.base.with_extension("idx");
            let final_lut = self.base.with_extension("lookup");

            if seg_count > 1 {
                let old_lut_store = LookupTableStore::open(&tmp_lut)?;

                let maps = infos
                    .into_iter()
                    .map(|(_, bytes)| Map::new(bytes).map_err(IndexError::from))
                    .collect::<Result<Vec<_>>>()?;

                multi_way_merge(
                    maps,
                    &old_lut_store,
                    &final_idx,
                    &final_lut,
                    &mut *self.write_cb,
                )?;

                drop(old_lut_store);
                remove_file(&tmp_lut)?;
                remove_file(&tmp_idx)?;
            } else {
                let f = OpenOptions::new().write(true).open(&tmp_idx)?;
                let new_len = f
                    .metadata()?
                    .len()
                    .saturating_sub(SegmentTrailer::SIZE as u64);
                f.set_len(new_len)?;
                f.sync_all()?;
                rename(&tmp_idx, &final_idx)?;
                rename(&tmp_lut, &final_lut)?;
            }
        } else {
            // No entries: write empty FST
            let final_idx = self.base.with_extension("idx");
            let final_lut = self.base.with_extension("lookup");
            let file = File::create(&final_idx)?;
            let writer = BufWriter::with_capacity(CHUNK_SIZE, file);
            let builder = MapBuilder::new(writer)?;
            builder.finish()?;
            LookupTableStoreBuilder::open(&final_lut)?.close()?;
        }

        self.payload_store.close()?;
        Ok(())
    }

    /// Set a callback to be invoked for each key merged during index write.
    pub fn with_write_progress<F>(mut self, cb: F) -> Self
    where
        F: FnMut() + 'static,
    {
        self.write_cb = Box::new(cb);
        self
    }
    /// Set a callback invoked once before merging segments, receiving per-segment stats.
    pub fn with_segment_stats<F>(mut self, cb: F) -> Self
    where
        F: FnMut(&[SegmentInfo]) + 'static,
    {
        self.before_merge_cb = Box::new(cb);
        self
    }
    /// Consume the builder, write files, and open a read-only `Database<V, C>` via mmap.
    pub fn into_database(self) -> Result<Database<V, C>>
    where
        V: DeserializeOwned,
    {
        let base = self.base.clone();
        self.close()?;
        Database::<V, C>::open(base)
    }
}

/// Read inline sub‑FST segments and their trailers by walking trailers backward from the end of `<base>.idx`.
/// Returns each segment's `SegmentTrailer` and owned bytes for further processing.
fn load_submaps_raw(idx_path: &Path) -> Result<Vec<(SegmentTrailer, Vec<u8>)>> {
    let file = File::open(idx_path)?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let buf = &mmap[..];
    let mut end = buf.len();
    let mut infos = Vec::new();
    while end >= SegmentTrailer::SIZE {
        let ts = end - SegmentTrailer::SIZE;
        let trailer = SegmentTrailer::from_bytes(&buf[ts..end]);
        if !trailer.validate_crc() {
            return Err(IndexError::InvalidFormat("CRC mismatch in idx trailer"));
        }
        let start = trailer.start_off as usize;
        let seg_bytes = buf[start..ts].to_vec();
        infos.push((trailer, seg_bytes));
        end = start;
    }
    infos.reverse();
    Ok(infos)
}

/// Multi-way merge inline sub-FSTs into a final `<base>.idx`, rebuilding the lookup store.
/// Streaming merge of an `OpBuilder::union()` of maps into a new FST at `final_idx`,
/// using `old_lut` for payload remapping and writing into `lookup_store`.
/// Merge all segments via an FST union into `final_idx`, rebuilding the lookup table in `final_lut`.
fn multi_way_merge<F: AsRef<[u8]>, Cb: FnMut() + ?Sized>(
    maps: Vec<Map<F>>,
    old_lut: &LookupTableStore,
    final_idx: &Path,
    final_lut: &Path,
    cb: &mut Cb,
) -> Result<()> {
    let mut lut_builder = LookupTableStoreBuilder::open(final_lut)?;
    let final_file = File::create(final_idx)?;
    let writer = BufWriter::with_capacity(CHUNK_SIZE, final_file);
    let mut builder = MapBuilder::new(writer)?;

    let mut opb = OpBuilder::new();
    for m in &maps {
        opb = opb.add(m.stream());
    }
    let mut stream = opb.union().into_stream();
    while let Some((key, vals)) = stream.next() {
        let iv = &vals[0];
        let entry = old_lut.get(iv.value as u32)?;
        let new_id = lut_builder.append(entry.payload_ptr)?;
        builder.insert(&key, new_id.into())?;
        cb();
    }
    builder.finish()?;
    lut_builder.close()?;
    Ok(())
}
