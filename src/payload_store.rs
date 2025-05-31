use memmap2::Mmap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::convert::TryInto;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::ptr;
use std::sync::Arc;

const CHUNK_SIZE: usize = 128 * 1024;
/// Magic header for payload store files.
const PAYLOAD_STORE_MAGIC: &[u8; 4] = b"FPAY";
/// Format version (v1 == uncompressed).
const PAYLOAD_STORE_VERSION: u16 = 1;
/// Size of the payload store file header (magic + version).
const PAYLOAD_STORE_HEADER_SIZE: usize = PAYLOAD_STORE_MAGIC.len() + std::mem::size_of::<u16>();
/// Header for each payload entry: 2-byte little-endian length prefix.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PayloadEntryHeader {
    /// little-endian length of the CBOR-encoded payload
    pub len: u16,
}

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
        let mut writer = BufWriter::with_capacity(CHUNK_SIZE, file);
        writer.write_all(PAYLOAD_STORE_MAGIC)?;
        writer.write_all(&PAYLOAD_STORE_VERSION.to_le_bytes())?;
        Ok(PayloadStoreBuilder {
            writer,
            offset: 0,
            phantom: std::marker::PhantomData,
        })
    }

    /// Append an optional payload; returns an identifier (offset+1), 0 means no payload.
    pub fn append(&mut self, value: Option<V>) -> io::Result<u32> {
        if let Some(val) = value {
            let data = serde_cbor::to_vec(&val)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "CBOR serialize failed"))?;
            let current = self.offset;
            let header = PayloadEntryHeader {
                len: (data.len() as u16).to_le(),
            };
            self.writer.write_all(&header.len.to_le_bytes())?;
            self.writer.write_all(&data)?;
            self.offset += std::mem::size_of::<PayloadEntryHeader>() as u64 + data.len() as u64;
            Ok((current + 1) as u32)
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
    /// Offset where payload entries begin (just after the file header).
    data_start: usize,
    phantom: std::marker::PhantomData<V>,
}

impl<V: DeserializeOwned> PayloadStore<V> {
    /// Open a read-only payload store by memory-mapping the file at `path`.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let buf = Arc::new(mmap);
        let raw = buf.as_ref();
        if raw.len() < PAYLOAD_STORE_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "payload file too small for header",
            ));
        }
        if &raw[..PAYLOAD_STORE_MAGIC.len()] != PAYLOAD_STORE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid payload store magic",
            ));
        }
        let version = u16::from_le_bytes(
            raw[PAYLOAD_STORE_MAGIC.len()..PAYLOAD_STORE_HEADER_SIZE]
                .try_into()
                .unwrap(),
        );
        if version != PAYLOAD_STORE_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported payload store version",
            ));
        }
        Ok(PayloadStore {
            buf,
            data_start: PAYLOAD_STORE_HEADER_SIZE,
            phantom: std::marker::PhantomData,
        })
    }

    /// Retrieve the payload for a given identifier, deserializing to V.
    pub fn get(&self, ptr_val: u32) -> io::Result<Option<V>> {
        if ptr_val == 0 {
            return Ok(None);
        }
        let rel = (ptr_val - 1) as usize;
        let buf = self.buf.as_ref();
        let header_off = self.data_start;
        let available = buf.len().saturating_sub(header_off);
        if rel + std::mem::size_of::<PayloadEntryHeader>() > available {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated payload length",
            ));
        }
        let header = unsafe {
            ptr::read_unaligned(
                buf.as_ptr()
                    .add(header_off + rel) as *const PayloadEntryHeader,
            )
        };
        let val_len = u16::from_le(header.len) as usize;
        let start = header_off + rel + std::mem::size_of::<PayloadEntryHeader>();
        if start + val_len > buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated payload data",
            ));
        }
        let slice = &buf[start..start + val_len];
        let val: V = serde_cbor::from_slice(slice)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "CBOR deserialize failed"))?;
        Ok(Some(val))
    }
}

impl<V: DeserializeOwned> Clone for PayloadStore<V> {
    fn clone(&self) -> Self {
        PayloadStore {
            buf: self.buf.clone(),
            data_start: self.data_start,
            phantom: std::marker::PhantomData,
        }
    }
}
