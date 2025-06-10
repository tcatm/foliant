use std::convert::TryInto;
use std::fs;
use std::io::Read;
use tempfile::NamedTempFile;

use foliant::{convert_v2_to_v3_inplace, PayloadStoreBuilderV2, PayloadStoreV2};

const PAYLOAD_MAGIC: &[u8] = b"FPAY";

/// Test in-place conversion of a simple V2 payload store to V3 and verify trailer and version bump.
#[test]
fn v3_conversion_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path();
    // Build a minimal V2 payload store with one entry
    let mut builder = PayloadStoreBuilderV2::<Vec<u8>>::open(path)?;
    let id = builder.append(Some(vec![7, 8, 9]))?;
    builder.close()?;

    // Raw file should start with v2 header
    let mut data = Vec::new();
    fs::File::open(path)?.read_to_end(&mut data)?;
    assert!(data.starts_with(PAYLOAD_MAGIC));
    assert_eq!(&data[4..6], &[2, 0]);

    // Convert in-place to V3
    convert_v2_to_v3_inplace(path)?;

    // After conversion, version should be bumped to 3
    let mut data2 = Vec::new();
    fs::File::open(path)?.read_to_end(&mut data2)?;
    assert!(data2.starts_with(PAYLOAD_MAGIC));
    assert_eq!(&data2[4..6], &[3, 0]);

    // Trailer magic must appear at end
    assert!(data2.ends_with(b"FPXI"));

    // Count of indexed chunks should be 1
    let len = data2.len();
    let count_pos = len - b"FPXI".len() - std::mem::size_of::<u64>();
    let count = u64::from_le_bytes(
        data2[count_pos..count_pos + std::mem::size_of::<u64>()]
            .try_into()
            .unwrap(),
    );
    assert_eq!(count, 1);

    // The single offset should point to the first block header (6 bytes in)
    let offs_pos = count_pos - std::mem::size_of::<u64>();
    let off0 = u64::from_le_bytes(
        data2[offs_pos..offs_pos + std::mem::size_of::<u64>()]
            .try_into()
            .unwrap(),
    );
    assert_eq!(off0 as usize, PAYLOAD_MAGIC.len() + 2);

    // New reader should load index and return correct payload
    let store = PayloadStoreV2::<Vec<u8>>::open(path)?;
    assert_eq!(store.get(id)?, Some(vec![7, 8, 9]));
    Ok(())
}
