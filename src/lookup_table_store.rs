use std::fs::File;
use std::io::{self, BufWriter};
use std::path::Path;
use std::sync::Arc;

use memmap2::Mmap;
use fst::{Map, MapBuilder};

/// Wrapper around an atomically-reference-counted memory map for FST data.
#[derive(Clone)]
struct MmapArc(Arc<Mmap>);

impl AsRef<[u8]> for MmapArc {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        <Mmap as AsRef<[u8]>>::as_ref(&*self.0)
    }
}

/// One entry in the lookup table mapping to a payload pointer.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LookupEntry {
    /// 64-bit pointer into the payload file (offset+1); zero means no payload.
    pub payload_ptr: u64,
}

/// Builder for a lookup table mapping 32-bit identifiers to 64-bit payload pointers.
pub struct LookupTableStoreBuilder {
    inner: MapBuilder<BufWriter<File>>,
    count: u32,
}

impl LookupTableStoreBuilder {
    /// Open a disk-backed lookup table store for writing, truncating any existing file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        let inner = MapBuilder::new(writer)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(LookupTableStoreBuilder { inner, count: 0 })
    }

    /// Append a payload pointer and return a new 32-bit identifier (starting from 1).
    pub fn append(&mut self, payload_ptr: u64) -> io::Result<u32> {
        let id = self.count.wrapping_add(1);
        self.inner.insert(&id.to_be_bytes(), payload_ptr)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.count = id;
        Ok(id)
    }

    /// Finish writing and flush the lookup FST to disk.
    pub fn close(self) -> io::Result<()> {
        self.inner.finish()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(())
    }
}

/// Read-only lookup table store backed by a memory-mapped FST mapping identifiers to payload pointers.
#[derive(Clone)]
pub struct LookupTableStore {
    map: Map<MmapArc>,
}


impl LookupTableStore {
    /// Open a read-only lookup table store by memory-mapping the FST at `path`.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = MmapArc(Arc::new(mmap));
        let map = Map::new(data.clone())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(LookupTableStore { map })
    }

    /// Retrieve the `LookupEntry` for a given identifier.
    pub fn get(&self, id: u32) -> io::Result<LookupEntry> {
        match self.map.get(&id.to_be_bytes()) {
            Some(payload_ptr) => Ok(LookupEntry { payload_ptr }),
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "lookup id not found",
            )),
        }
    }
}