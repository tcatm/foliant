//! Test for bug where multi_list returns bogus data when no bitmap matches
//! instead of returning empty results.

use foliant::multi_list::MultiShardListStreamer;
use foliant::payload_store::{CborPayloadCodec, PayloadStoreVersion};
use foliant::{DatabaseBuilder, Streamer};
use tempfile::tempdir;

/// Helper: build an on-disk database with the given keys (no payloads) and return its shards.
fn make_shards(keys: &[&str]) -> Vec<foliant::Shard<serde_cbor::Value, CborPayloadCodec>> {
    let dir = tempdir().unwrap();
    let idx_path = dir.path().join("testdb");
    let mut builder: DatabaseBuilder<serde_cbor::Value, CborPayloadCodec> =
        DatabaseBuilder::new(&idx_path, PayloadStoreVersion::V2).unwrap();
    for &k in keys {
        builder.insert(k, None);
    }
    // finalize index files
    builder.close().unwrap();
    // open the single shard from disk
    let idx_file = idx_path.with_extension("idx");
    vec![foliant::Shard::open(&idx_file).unwrap()]
}

/// Extract the string keys or common-prefixes from a stream.
fn collect_strs(
    stream: &mut MultiShardListStreamer<'_, serde_cbor::Value, CborPayloadCodec>,
) -> Vec<String> {
    let mut v = Vec::new();
    while let Some(e) = stream.next() {
        v.push(e.as_str().to_string());
    }
    v
}

#[test]
fn test_no_prefix_match_returns_empty() {
    // Create database with keys that don't start with "aaaaa"
    let keys = ["aaaab", "b", "c", "d"];
    let shards = make_shards(&keys);
    
    // Search for prefix "aaaaa" which doesn't match any key
    let prefix = b"aaaaa".to_vec();
    let mut st = MultiShardListStreamer::new(&shards, prefix, None);
    
    // Should return empty results, not the next available entry
    let results = collect_strs(&mut st);
    assert!(results.is_empty(), "Expected empty results for non-matching prefix 'aaaaa', but got: {:?}", results);
}

#[test]
fn test_no_prefix_match_with_delimiter_returns_empty() {
    // Create database with keys that don't start with "zzz"
    let keys = ["a/1", "a/2", "b/1", "b/2"];
    let shards = make_shards(&keys);
    
    // Search for prefix "zzz" which doesn't match any key
    let prefix = b"zzz".to_vec();
    let mut st = MultiShardListStreamer::new(&shards, prefix, Some(b'/'));
    
    // Should return empty results, not the next available entry
    let results = collect_strs(&mut st);
    assert!(results.is_empty(), "Expected empty results for non-matching prefix 'zzz' with delimiter, but got: {:?}", results);
}

#[test]
fn test_no_prefix_match_edge_cases() {
    let keys = ["apple", "banana", "cherry"];
    let shards = make_shards(&keys);
    
    // Test prefix that would be lexicographically before all keys
    let mut st1 = MultiShardListStreamer::new(&shards, b"aaa".to_vec(), None);
    let results1 = collect_strs(&mut st1);
    assert!(results1.is_empty(), "Expected empty results for prefix 'aaa', but got: {:?}", results1);
    
    // Test prefix that would be lexicographically after all keys
    let mut st2 = MultiShardListStreamer::new(&shards, b"zzz".to_vec(), None);
    let results2 = collect_strs(&mut st2);
    assert!(results2.is_empty(), "Expected empty results for prefix 'zzz', but got: {:?}", results2);
    
    // Test prefix that's in the middle range but doesn't match
    let mut st3 = MultiShardListStreamer::new(&shards, b"bbb".to_vec(), None);
    let results3 = collect_strs(&mut st3);
    assert!(results3.is_empty(), "Expected empty results for prefix 'bbb', but got: {:?}", results3);
}