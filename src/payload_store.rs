use std::fs::File;
use std::io::{self, Write, Seek};
use std::path::Path;
use std::sync::Arc;
use memmap2::Mmap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::convert::TryInto;

/// Builder for appending payloads of type V to a file-backed store.
pub struct PayloadStoreBuilder<V: Serialize> {
    file: File,
    phantom: std::marker::PhantomData<V>,
}

impl<V: Serialize> PayloadStoreBuilder<V> {
    /// Open a disk-backed payload store for writing, truncating any existing file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::create(path)?;
        Ok(PayloadStoreBuilder { file, phantom: std::marker::PhantomData })
    }

    /// Append a payload, returning an identifier (offset+1). Zero means no payload.
    pub fn append(&mut self, value: Option<V>) -> io::Result<u64> {
        if let Some(val) = value {
            let data = serde_cbor::to_vec(&val)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "CBOR serialize failed"))?;
            let offset = self.file.stream_position()?;
            let len = data.len() as u32;
            self.file.write_all(&len.to_le_bytes())?;
            self.file.write_all(&data)?;
            Ok(offset + 1)
        } else {
            Ok(0)
        }
    }

    /// Close the payload store, ensuring all data is flushed.
    pub fn close(self) -> io::Result<()> {
        self.file.sync_all()
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