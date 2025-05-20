use std::fs::File;
use std::io::{self, Write, IoSlice, BufWriter};
use std::path::Path;
use std::sync::Arc;
use memmap2::Mmap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::convert::TryInto;

const CHUNK_SIZE: usize = 128 * 1024;
/// Builder for appending payloads of type V to a file-backed store, buffering writes in 128KiB.
pub struct PayloadStoreBuilder<V: Serialize> {
    writer: BufWriter<File>,
    offset: u64,
    phantom: std::marker::PhantomData<V>,
}

impl<V: Serialize> PayloadStoreBuilder<V> {
    /// Open a disk-backed payload store for writing, truncating any existing file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::create(path)?;
        let writer = BufWriter::with_capacity(CHUNK_SIZE, file);
        Ok(PayloadStoreBuilder { writer, offset: 0, phantom: std::marker::PhantomData })
    }

    /// Append an optional payload; returns an identifier (offset+1), 0 means no payload.
    pub fn append(&mut self, value: Option<V>) -> io::Result<u64> {
        if let Some(val) = value {
            let data = serde_cbor::to_vec(&val)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "CBOR serialize failed"))?;
            let current = self.offset;
            let len_bytes = (data.len() as u32).to_le_bytes();
            let bufs = [IoSlice::new(&len_bytes), IoSlice::new(&data)];
            self.writer.write_vectored(&bufs)?;
            self.offset += 4 + data.len() as u64;
            Ok(current + 1)
        } else {
            Ok(0)
        }
    }

    /// Flush buffered writes and sync to disk.
    pub fn close(mut self) -> io::Result<()> {
        self.writer.flush()?;
        let file = self.writer.into_inner()?;
        file.sync_all()
    }
}

/// Read-only payload store backed by a memory-mapped file, deserializing into V.
pub struct PayloadStore<V: DeserializeOwned> {
    buf: Arc<Mmap>,
    phantom: std::marker::PhantomData<V>,
}

impl<V: DeserializeOwned> PayloadStore<V> {
    /// Open a read-only payload store by memory-mapping the file at `path`.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(PayloadStore { buf: Arc::new(mmap), phantom: std::marker::PhantomData })
    }

    /// Retrieve the payload for a given identifier, deserializing to V.
    pub fn get(&self, ptr_val: u64) -> io::Result<Option<V>> {
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
        let slice = &buf[start..start + val_len];
        let val: V = serde_cbor::from_slice(slice)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "CBOR deserialize failed"))?;
        Ok(Some(val))
    }
}

impl<V: DeserializeOwned> Clone for PayloadStore<V> {
    fn clone(&self) -> Self {
        PayloadStore { buf: self.buf.clone(), phantom: std::marker::PhantomData }
    }
}