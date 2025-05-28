use memmap2::Mmap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::ptr;
use std::sync::Arc;

const CHUNK_SIZE: usize = 128 * 1024;

/// One entry in the lookup table, mapping to a payload pointer.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct LookupEntry {
    /// 32-bit pointer into the payload file (offset+1); zero means no payload.
    pub payload_ptr: u32,
}

const RECSZ: usize = std::mem::size_of::<LookupEntry>();

/// Builder for a flat lookup table storing `LookupEntry`s sequentially.
pub struct LookupTableStoreBuilder {
    writer: BufWriter<File>,
    count: u32,
}

impl LookupTableStoreBuilder {
    /// Open a disk-backed lookup table store for writing, truncating any existing file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::create(path)?;
        let writer = BufWriter::with_capacity(CHUNK_SIZE, file);
        Ok(LookupTableStoreBuilder { writer, count: 0 })
    }

    /// Append a `LookupEntry` with the given payload pointer; returns an identifier (index+1).
    pub fn append(&mut self, payload_ptr: u32) -> io::Result<u32> {
        let idx = self.count;
        let entry = LookupEntry { payload_ptr };
        self.writer.write_all(&entry.payload_ptr.to_le_bytes())?;
        self.count += 1;
        Ok(idx + 1)
    }

    /// Flush buffered writes and sync to disk.
    pub fn close(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }
}

/// Read-only lookup table store backed by a memory-mapped file.
pub struct LookupTableStore {
    buf: Arc<Mmap>,
}

impl LookupTableStore {
    /// Open a read-only lookup table store by memory-mapping the file at `path`.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(LookupTableStore {
            buf: Arc::new(mmap),
        })
    }

    /// Retrieve the `LookupEntry` for a given identifier.
    pub fn get(&self, id: u32) -> io::Result<LookupEntry> {
        let idx = (id - 1) as usize;
        let start = idx
            .checked_mul(RECSZ)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "lookup index overflow"))?;
        let end = start + RECSZ;
        if end > self.buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated lookup table entry",
            ));
        }
        let slice = &self.buf[start..end];
        let raw = unsafe { ptr::read_unaligned(slice.as_ptr() as *const LookupEntry) };
        let payload_ptr = u32::from_le(raw.payload_ptr);
        Ok(LookupEntry { payload_ptr })
    }
}

impl Clone for LookupTableStore {
    fn clone(&self) -> Self {
        LookupTableStore {
            buf: self.buf.clone(),
        }
    }
}
