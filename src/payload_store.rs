use std::io::{self, Write, Seek};

/// Append-only payload store for on-disk serialized payloads.
pub struct PayloadStore {
    buf: Vec<u8>,
}

impl PayloadStore {
    /// Create a new, empty PayloadStore.
    pub fn new() -> Self {
        PayloadStore { buf: Vec::new() }
    }

    /// Append a payload, returning an identifier (offset+1). Zero means no payload.
    pub fn append(&mut self, data: &[u8]) -> u64 {
        let offset = self.buf.len() as u64;
        // Store length prefix (u32 LE) and data
        self.buf.extend(&(data.len() as u32).to_le_bytes());
        self.buf.extend(data);
        offset + 1
    }

    /// Write all payloads to writer at current position, returning start offset.
    pub fn finish<W: Write + Seek>(&self, w: &mut W) -> io::Result<u64> {
        let payload_offset = w.stream_position()?;
        w.write_all(&self.buf)?;
        Ok(payload_offset)
    }
}

/// Mmap-backed payload store reader.
pub struct MmapPayloadStore<'a> {
    buf: &'a [u8],
    offset: usize,
}

impl<'a> MmapPayloadStore<'a> {
    /// Construct from mmap buffer and payload section offset.
    pub fn new(buf: &'a [u8], offset: usize) -> Self {
        MmapPayloadStore { buf, offset }
    }

    /// Retrieve the payload for a given identifier.
    pub fn get(&self, ptr_val: u64) -> io::Result<Option<&'a [u8]>> {
        if ptr_val == 0 {
            return Ok(None);
        }
        // Stored id = rel_offset + 1
        let rel = (ptr_val - 1) as usize;
        let mut p = self.offset + rel;
        // Read length prefix
        if p + 4 > self.buf.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated payload length"));
        }
        let len_bytes: [u8; 4] = self.buf[p..p + 4]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid payload length"))?;
        let val_len = u32::from_le_bytes(len_bytes) as usize;
        p += 4;
        if p + val_len > self.buf.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated payload data"));
        }
        Ok(Some(&self.buf[p..p + val_len]))
    }
}