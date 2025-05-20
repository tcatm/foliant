use std::fs::File;
use std::io::{self, Write, Seek};
use std::path::Path;
use std::sync::Arc;
use memmap2::Mmap;

/// Builder for appending payloads to a file-backed store.
pub struct PayloadStoreBuilder {
    file: File,
}

impl PayloadStoreBuilder {
    /// Open a disk-backed payload store for writing, truncating any existing file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::create(path)?;
        Ok(PayloadStoreBuilder { file })
    }

    /// Append a payload, returning an identifier (offset+1). Zero means no payload.
    pub fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        let offset = self.file.stream_position()?;
        let len = data.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(data)?;
        Ok(offset + 1)
    }

    /// Close the payload store, ensuring all data is flushed.
    pub fn close(self) -> io::Result<()> {
        self.file.sync_all()
    }
}

/// Read-only payload store backed by a memory-mapped file.
pub struct PayloadStore {
    buf: Arc<Mmap>,
}

impl PayloadStore {
    /// Open a read-only payload store by memory-mapping the file at `path`.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(PayloadStore { buf: Arc::new(mmap) })
    }

    /// Retrieve the payload for a given identifier as a byte slice.
    pub fn get(&self, ptr_val: u64) -> io::Result<Option<&[u8]>> {
        if ptr_val == 0 {
            return Ok(None);
        }
        let rel = (ptr_val - 1) as usize;
        let buf = &self.buf;
        if rel + 4 > buf.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated payload length"));
        }
        let len_bytes: [u8; 4] = buf[rel..rel + 4]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid payload length"))?;
        let val_len = u32::from_le_bytes(len_bytes) as usize;
        let start = rel + 4;
        if start + val_len > buf.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated payload data"));
        }
        Ok(Some(&buf[start..start + val_len]))
    }
}

impl Clone for PayloadStore {
    fn clone(&self) -> Self {
        PayloadStore { buf: self.buf.clone() }
    }
}