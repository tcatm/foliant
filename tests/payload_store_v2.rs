use foliant::{PayloadStoreBuilderV2, PayloadStoreV2};
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::convert::TryInto;
use tempfile::NamedTempFile;
use zstd::block;

/// Round-trip basic V2 entries via the raw payload store builder and reader.
#[test]
fn v2_roundtrip_simple() -> Result<(), Box<dyn Error>> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path();
    let mut builder = PayloadStoreBuilderV2::<Vec<u8>>::open(path)?;
    let id1 = builder.append(Some(vec![1, 2, 3]))?;
    let id2 = builder.append(None)?;
    let id3 = builder.append(Some(vec![4, 5, 6]))?;
    builder.close()?;

    let store = PayloadStoreV2::<Vec<u8>>::open(path)?;
    assert_eq!(store.get(id1)?, Some(vec![1, 2, 3]));
    assert_eq!(store.get(id2)?, None);
    assert_eq!(store.get(id3)?, Some(vec![4, 5, 6]));
    Ok(())
}

/// Ensure the on-disk header for a V2 payload store is correctly written.
#[test]
fn payload_file_header_v2() -> Result<(), Box<dyn Error>> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path();
    let mut builder = PayloadStoreBuilderV2::<Vec<u8>>::open(path)?;
    // no entries appended; finalize to flush header only
    builder.close()?;
    let data = std::fs::read(path)?;
    assert!(data.starts_with(b"FPAY"));
    assert_eq!(&data[4..6], &[2, 0]);
    Ok(())
}

/// Test V2 behavior over multiple chunks and inspect raw block headers and decompressed data.
#[test]
fn v2_multi_chunk_inspection() -> Result<(), Box<dyn Error>> {
    // Prepare three entries to force two full chunks and one partial.
    const CHUNK_SIZE: usize = 64 * 1024;
    let tmp = NamedTempFile::new()?;
    let path = tmp.path();
    let mut builder = PayloadStoreBuilderV2::<Vec<u8>>::open(path)?;
    let id1 = builder.append(Some(vec![42; CHUNK_SIZE]))?;
    let id2 = builder.append(Some(vec![43; CHUNK_SIZE]))?;
    let id3 = builder.append(Some(vec![44; 1000]))?;
    builder.close()?;

    // Read raw on-disk payload store file and prepare to walk blocks.
    let mut data = Vec::new();
    File::open(path)?.read_to_end(&mut data)?;
    assert!(data.starts_with(b"FPAY"));
    let mut pos = b"FPAY".len() + 2;
    let store = PayloadStoreV2::<Vec<u8>>::open(path)?;

    eprintln!("=== V2 multi-chunk block inspection ===");
    for (i, &id) in [id1, id2, id3].iter().enumerate() {
        let uncompressed_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let compressed_len = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize;
        pos += 8;
        let comp = &data[pos..pos + compressed_len];
        let block_data = block::decompress(comp, uncompressed_len)?;
        let header_len = u16::from_le_bytes(block_data[0..2].try_into().unwrap()) as usize;
        eprint!(
            "chunk {}: uncompressed={}, compressed={}, header.len={}",
            i, uncompressed_len, compressed_len, header_len
        );
        match store.get(id) {
            Ok(Some(v)) => eprintln!(", get.len={}", v.len()),
            Ok(None) => eprintln!(", get=None"),
            Err(e) => eprintln!(", get error={:?}", e),
        }
        pos += compressed_len;
    }
    Ok(())
}

/// Demonstrate that very large payload lengths may overflow the u16 header length.
#[test]
fn v2_header_length_truncation() -> Result<(), Box<dyn Error>> {
    // Create a payload > 65535 bytes to expose u16 overflow in header.len.
    let huge = (1 << 16) + 10;
    let tmp = NamedTempFile::new()?;
    let path = tmp.path();
    let mut builder = PayloadStoreBuilderV2::<Vec<u8>>::open(path)?;
    let _id = builder.append(Some(vec![99; huge]))?;
    builder.close()?;

    let mut data = Vec::new();
    File::open(path)?.read_to_end(&mut data)?;
    // Skip magic + version header
    let pos = b"FPAY".len() + 2;
    let uncompressed_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    let compressed_len = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap()) as usize;
    let comp = &data[pos + 8..pos + 8 + compressed_len];
    let block_data = block::decompress(comp, uncompressed_len)?;
    // Read the truncated header.len value
    let stored_len = u16::from_le_bytes(block_data[0..2].try_into().unwrap()) as usize;
    eprintln!("stored header.len = {}, actual payload bytes = {}", stored_len, huge);
    assert!(stored_len != huge, "header.len did not overflow/truncate");
    Ok(())
}