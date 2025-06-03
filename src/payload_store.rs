use memmap2::Mmap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_cbor;
use std::convert::TryInto;
use std::fs::File;
use std::io::{self, BufWriter, Read, Write};
use std::path::Path;
use std::ptr;
use std::sync::{Arc, Mutex};
use std::num::NonZeroUsize;
use lru::LruCache;
use zstd::block;

const CHUNK_SIZE: usize = 64 * 1024;
/// Magic header for payload store files.
const PAYLOAD_STORE_MAGIC: &[u8; 4] = b"FPAY";
/// Format version (v1 == uncompressed).
const PAYLOAD_STORE_VERSION: u16 = 1;
/// Size of the payload store file header (magic + version).
const PAYLOAD_STORE_HEADER_SIZE: usize = PAYLOAD_STORE_MAGIC.len() + std::mem::size_of::<u16>();
/// Format version (v2 == compressed with zstd blocks of 64KiB).
const PAYLOAD_STORE_VERSION_V2: u16 = 2;
/// Max total bytes of uncompressed blocks kept in the V2 cache (64 MiB).
const V2_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;
/// Maximum number of cached chunks = V2_CACHE_MAX_BYTES / CHUNK_SIZE.
const V2_CACHE_CAPACITY: usize = V2_CACHE_MAX_BYTES / CHUNK_SIZE;
/// Trait for payload encoding and decoding.
pub trait PayloadCodec {
    /// Encode a serializable value to bytes.
    fn encode<V: Serialize>(value: &V) -> io::Result<Vec<u8>>;
    /// Decode a deserializable value from bytes.
    fn decode<V: DeserializeOwned>(bytes: &[u8]) -> io::Result<V>;
}

/// Default CBOR-based payload codec.
pub struct CborPayloadCodec;

impl PayloadCodec for CborPayloadCodec {
    fn encode<V: Serialize>(value: &V) -> io::Result<Vec<u8>> {
        serde_cbor::to_vec(value)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "CBOR serialize failed"))
    }

    fn decode<V: DeserializeOwned>(bytes: &[u8]) -> io::Result<V> {
        serde_cbor::from_slice(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "CBOR deserialize failed"))
    }
}
/// Header for each payload entry: 2-byte little-endian length prefix.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PayloadEntryHeader {
    /// little-endian length of the CBOR-encoded payload
    pub len: u16,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Verify that direct calls to get_chunk correctly evict the oldest blocks when capacity is exceeded.
    #[test]
    fn v2_chunk_cache_eviction() {
        // Compose an on-disk payload-store with (capacity + 1) raw blocks.
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();
        let mut f = File::create(path).unwrap();
        f.write_all(PAYLOAD_STORE_MAGIC).unwrap();
        f.write_all(&PAYLOAD_STORE_VERSION_V2.to_le_bytes()).unwrap();

        for i in 0..(V2_CACHE_CAPACITY + 1) {
            let raw = vec![i as u8; CHUNK_SIZE];
            let compressed = block::compress(&raw, 0).unwrap();
            f.write_all(&(raw.len() as u32).to_le_bytes()).unwrap();
            f.write_all(&(compressed.len() as u32).to_le_bytes()).unwrap();
            f.write_all(&compressed).unwrap();
        }
        f.sync_all().unwrap();

        let store = PayloadStoreV2::<Vec<u8>>::open(path).unwrap();
        // Decompress each chunk to populate the cache.
        for idx in 0..(V2_CACHE_CAPACITY + 1) {
            let block = store.get_chunk(idx).unwrap();
            assert_eq!(&*block, &vec![idx as u8; CHUNK_SIZE]);
        }
        // Cache size must not exceed capacity (oldest evicted).
        assert_eq!(store.cache_len(), V2_CACHE_CAPACITY);
    }
}

/// Version 1 (uncompressed) builder for appending payloads of type V to a file-backed store, buffering writes in 64KiB.
/// The payload values are encoded using the given `PayloadCodec`.
pub struct PayloadStoreBuilderV1<V: Serialize, C: PayloadCodec = CborPayloadCodec> {
    writer: BufWriter<File>,
    offset: u64,
    phantom: std::marker::PhantomData<(V, C)>,
}

impl<V: Serialize, C: PayloadCodec> PayloadStoreBuilderV1<V, C> {
    /// Open a disk-backed payload store for writing, truncating any existing file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::with_capacity(CHUNK_SIZE, file);
        writer.write_all(PAYLOAD_STORE_MAGIC)?;
        writer.write_all(&PAYLOAD_STORE_VERSION.to_le_bytes())?;
        Ok(PayloadStoreBuilderV1 {
            writer,
            offset: 0,
            phantom: std::marker::PhantomData,
        })
    }

    /// Append an optional payload; returns an identifier (offset+1), 0 means no payload.
    pub fn append(&mut self, value: Option<V>) -> io::Result<u64> {
        if let Some(val) = value {
            let data = C::encode(&val)?;
            let current = self.offset;
            let header = PayloadEntryHeader {
                len: (data.len() as u16).to_le(),
            };
            self.writer.write_all(&header.len.to_le_bytes())?;
            self.writer.write_all(&data)?;
            self.offset += std::mem::size_of::<PayloadEntryHeader>() as u64 + data.len() as u64;
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

/// Version 2 (compressed with zstd blocks of 64KiB) builder for appending payloads of type V to a file-backed store.
/// The payload values are encoded using the given `PayloadCodec`.
pub struct PayloadStoreBuilderV2<V: Serialize, C: PayloadCodec = CborPayloadCodec> {
    writer: BufWriter<File>,
    uncompressed_buf: Vec<u8>,
    /// Index of the current uncompressed chunk.
    chunk_idx: u32,
    phantom: std::marker::PhantomData<(V, C)>,
}

impl<V: Serialize, C: PayloadCodec> PayloadStoreBuilderV2<V, C> {
    /// Open a disk-backed payload store for writing, truncating any existing file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::with_capacity(CHUNK_SIZE, file);
        writer.write_all(PAYLOAD_STORE_MAGIC)?;
        writer.write_all(&PAYLOAD_STORE_VERSION_V2.to_le_bytes())?;
        Ok(PayloadStoreBuilderV2 {
            writer,
            uncompressed_buf: Vec::with_capacity(CHUNK_SIZE),
            chunk_idx: 0,
            phantom: std::marker::PhantomData,
        })
    }

    /// Append an optional payload (CBOR-serialized); returns a packed identifier (offset+1),
    /// where 0 means no payload. Nonzero IDs are laid out as:
    ///   ┌────────────┬──────────────┬───────────┐
    ///   │ reserved   │ chunk index  │ intra off │
    ///   │  (16 bits) │   (32 bits)  │ (16 bits) │
    ///   └────────────┴──────────────┴───────────┘
    pub fn append(&mut self, value: Option<V>) -> io::Result<u64> {
        if let Some(val) = value {
            let data = C::encode(&val)?;
            // intra-chunk offset before writing header/data
            let intra = self.uncompressed_buf.len() as u16;
            // pack reserved upper 16 bits (zero), chunk index (next 32 bits), intra-chunk offset (lower 16 bits)
            let id = ((self.chunk_idx as u64) << 16) | (intra as u64);
            let header = PayloadEntryHeader { len: (data.len() as u16).to_le() };
            self.uncompressed_buf.extend_from_slice(&header.len.to_le_bytes());
            self.uncompressed_buf.extend_from_slice(&data);
            if self.uncompressed_buf.len() >= CHUNK_SIZE {
                let compressed = block::compress(&self.uncompressed_buf, 0)
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "zstd compress failed"))?;
                let uncompressed_len = self.uncompressed_buf.len() as u32;
                let compressed_len = compressed.len() as u32;
                self.writer.write_all(&uncompressed_len.to_le_bytes())?;
                self.writer.write_all(&compressed_len.to_le_bytes())?;
                self.writer.write_all(&compressed)?;
                self.uncompressed_buf.clear();
                self.chunk_idx += 1;
            }
            Ok(id + 1)
        } else {
            Ok(0)
        }
    }

    /// Flush any remaining buffered writes and sync to disk.
    pub fn close(mut self) -> io::Result<()> {
        if !self.uncompressed_buf.is_empty() {
            let compressed = block::compress(&self.uncompressed_buf, 0)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "zstd compress failed"))?;
            let uncompressed_len = self.uncompressed_buf.len() as u32;
            let compressed_len = compressed.len() as u32;
            self.writer.write_all(&uncompressed_len.to_le_bytes())?;
            self.writer.write_all(&compressed_len.to_le_bytes())?;
            self.writer.write_all(&compressed)?;
            self.uncompressed_buf.clear();
            self.chunk_idx += 1;
        }
        self.writer.flush()?;
        let file = self.writer.into_inner()?;
        file.sync_all()
    }
}

/// Version 1 (uncompressed) read-only payload store backed by a memory-mapped file, deserializing into V.
/// The payload values are decoded using the given `PayloadCodec`.
pub struct PayloadStoreV1<V: DeserializeOwned, C: PayloadCodec = CborPayloadCodec> {
    buf: Arc<Mmap>,
    /// Offset where payload entries begin (just after the file header).
    data_start: usize,
    phantom: std::marker::PhantomData<(V, C)>,
}

impl<V: DeserializeOwned, C: PayloadCodec> PayloadStoreV1<V, C> {
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
        Ok(PayloadStoreV1 {
            buf,
            data_start: PAYLOAD_STORE_HEADER_SIZE,
            phantom: std::marker::PhantomData,
        })
    }

    /// Retrieve the payload for a given identifier, deserializing to V.
    pub fn get(&self, ptr_val: u64) -> io::Result<Option<V>> {
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
            ptr::read_unaligned(buf.as_ptr().add(header_off + rel) as *const PayloadEntryHeader)
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
        let val: V = C::decode(slice)?;
        Ok(Some(val))
    }
}

impl<V: DeserializeOwned, C: PayloadCodec> Clone for PayloadStoreV1<V, C> {
    fn clone(&self) -> Self {
        PayloadStoreV1 {
            buf: self.buf.clone(),
            data_start: self.data_start,
            phantom: std::marker::PhantomData,
        }
    }
}
/// Default payload store builder (version 2: compressed with zstd blocks).
pub type PayloadStoreBuilder<V> = PayloadStoreBuilderV2<V>;

/// Version 2 (compressed with zstd blocks of 64KiB) read-only payload store backed by a memory-mapped file, deserializing into V.
/// The payload values are decoded using the given `PayloadCodec`.
pub struct PayloadStoreV2<V: DeserializeOwned, C: PayloadCodec = CborPayloadCodec> {
    buf: Arc<Mmap>,
    data_start: usize,
    /// LRU cache of recently decompressed blocks, keyed by chunk index.
    /// Offsets of each compressed block header from the file start (after the payload-store header).
    chunk_offsets: Vec<usize>,
    /// LRU cache of recently decompressed blocks, keyed by chunk index.
    cache: Arc<Mutex<LruCache<usize, Arc<Vec<u8>>>>>,
    phantom: std::marker::PhantomData<(V, C)>,
}

impl<V: DeserializeOwned, C: PayloadCodec> PayloadStoreV2<V, C> {
    /// Open a read-only payload store by memory-mapping the file at `path`.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let buf = Arc::new(mmap);
        let raw = buf.as_ref();
        if raw.len() < PAYLOAD_STORE_HEADER_SIZE {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "payload file too small for header"));
        }
        if &raw[..PAYLOAD_STORE_MAGIC.len()] != PAYLOAD_STORE_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid payload store magic"));
        }
        let version = u16::from_le_bytes(
            raw[PAYLOAD_STORE_MAGIC.len()..PAYLOAD_STORE_HEADER_SIZE]
                .try_into()
                .unwrap(),
        );
        if version != PAYLOAD_STORE_VERSION_V2 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "unsupported payload store version"));
        }
        // Build an index of block-header offsets so we can seek directly to chunk N.
        let mut chunk_offsets = Vec::new();
        let raw = buf.as_ref();
        let mut off = PAYLOAD_STORE_HEADER_SIZE;
        while off + 8 <= raw.len() {
            chunk_offsets.push(off);
            // read lengths to skip this block
            let compressed_len = u32::from_le_bytes(
                raw[off + 4..off + 8].try_into().unwrap()
            ) as usize;
            off = off + 8 + compressed_len;
        }
        Ok(PayloadStoreV2 {
            buf,
            data_start: PAYLOAD_STORE_HEADER_SIZE,
            chunk_offsets,
            cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(V2_CACHE_CAPACITY).unwrap()
            ))),
            phantom: std::marker::PhantomData,
        })
    }

    /// Retrieve the payload for a given identifier, deserializing to V.
    pub fn get(&self, ptr_val: u64) -> io::Result<Option<V>> {
        if ptr_val == 0 {
            return Ok(None);
        }
        
        // decode chunk index (bits 16..47) and intra-chunk offset (lower 16 bits)
        let id = ptr_val - 1;
        let chunk_idx = ((id >> 16) & 0xFFFF_FFFF) as usize;
        let intra = (id & 0xFFFF) as usize;
        
        let block_data = self.get_chunk(chunk_idx)?;
        self.read_payload_from_chunk(&block_data, intra)
    }

    fn get_chunk(&self, chunk_idx: usize) -> io::Result<Arc<Vec<u8>>> {
        // First, check if we already have this chunk decompressed in the cache.
        {
            let mut cache = self.cache.lock().unwrap();
            if let Some(data) = cache.get(&chunk_idx) {
                return Ok(Arc::clone(data));
            }
        }
        
        // locate the target chunk via precomputed offsets
        let off = *self.chunk_offsets.get(chunk_idx).ok_or_else(||
            io::Error::new(io::ErrorKind::UnexpectedEof, "missing block header for chunk")
        )?;
        
        let buf = self.buf.as_ref();
        if off + 8 > buf.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated block header"));
        }
        
        let uncompressed_len = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
        let compressed_len = u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap()) as usize;
        let comp_off = off + 8;
        
        if comp_off + compressed_len > buf.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated block data"));
        }
        
        let comp = &buf[comp_off..comp_off + compressed_len];
        let data = Arc::new(
            block::decompress(comp, uncompressed_len)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "zstd decompress failed"))?
        );
        
        {
            let mut cache = self.cache.lock().unwrap();
            cache.put(chunk_idx, Arc::clone(&data));
        }
        
        Ok(data)
    }

    fn read_payload_from_chunk(&self, block_data: &[u8], intra: usize) -> io::Result<Option<V>> {
        if intra + std::mem::size_of::<PayloadEntryHeader>() > block_data.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated payload length"));
        }
        
        let header = unsafe {
            ptr::read_unaligned(block_data.as_ptr().add(intra) as *const PayloadEntryHeader)
        };
        let val_len = u16::from_le(header.len) as usize;
        let start = intra + std::mem::size_of::<PayloadEntryHeader>();
        
        if start + val_len > block_data.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated payload data"));
        }
        
        let val = C::decode(&block_data[start..start + val_len])?;
        Ok(Some(val))
    }

    /// Test-only: return the current number of cached chunks.
    #[cfg(test)]
    pub fn cache_len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }
}

impl<V: DeserializeOwned, C: PayloadCodec> Clone for PayloadStoreV2<V, C> {
    fn clone(&self) -> Self {
        PayloadStoreV2 {
            buf: self.buf.clone(),
            data_start: self.data_start,
            chunk_offsets: self.chunk_offsets.clone(),
            cache: self.cache.clone(),
            phantom: std::marker::PhantomData,
        }
    }
}

/// A versioned read-only payload store that auto-detects the file version
/// and delegates to the appropriate payload store implementation.
pub enum PayloadStore<V: DeserializeOwned, C: PayloadCodec = CborPayloadCodec> {
    /// Version 1 (uncompressed) payload store.
    V1(PayloadStoreV1<V, C>),
    /// Version 2 (compressed with zstd blocks) payload store.
    V2(PayloadStoreV2<V, C>),
}

impl<V: DeserializeOwned, C: PayloadCodec> PayloadStore<V, C> {
    /// Open a read-only payload store by memory-mapping the file at `path`
    /// and auto-detecting the payload store version.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(&path)?;
        let mut header = [0u8; PAYLOAD_STORE_HEADER_SIZE];
        (&file).read_exact(&mut header)?;
        if &header[..PAYLOAD_STORE_MAGIC.len()] != PAYLOAD_STORE_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid payload store magic"));
        }
        let version = u16::from_le_bytes(header[PAYLOAD_STORE_MAGIC.len()..].try_into().unwrap());
        match version {
            PAYLOAD_STORE_VERSION => PayloadStoreV1::open(path).map(PayloadStore::V1),
            PAYLOAD_STORE_VERSION_V2 => PayloadStoreV2::open(path).map(PayloadStore::V2),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported payload store version",
            )),
        }
    }

    /// Retrieve the payload for a given identifier, deserializing to V.
    pub fn get(&self, ptr_val: u64) -> io::Result<Option<V>> {
        match self {
            PayloadStore::V1(store) => store.get(ptr_val),
            PayloadStore::V2(store) => store.get(ptr_val),
        }
    }
}

impl<V: DeserializeOwned, C: PayloadCodec> Clone for PayloadStore<V, C> {
    fn clone(&self) -> Self {
        match self {
            PayloadStore::V1(store) => PayloadStore::V1((*store).clone()),
            PayloadStore::V2(store) => PayloadStore::V2((*store).clone()),
        }
    }
}
