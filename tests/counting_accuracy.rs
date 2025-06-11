//! Tests to verify that child counting in common prefixes is always accurate.
//! 
//! These tests ensure that the `count` field in `Entry::CommonPrefix` entries
//! accurately reflects the number of descendant keys under each prefix.

use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, Streamer};
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

/// Count the actual number of descendant keys under a prefix by listing without delimiter.
fn count_actual_descendants(db: &Database<Value>, prefix: &str) -> usize {
    let all_entries: Vec<Entry<Value>> = db.list(prefix, None).unwrap().collect();
    all_entries.len()
}

/// Get all common prefix entries and their reported child counts from a delimited listing.
fn get_reported_counts(db: &Database<Value>, prefix: &str, delimiter: char) -> Vec<(String, usize)> {
    let entries: Vec<Entry<Value>> = db.list(prefix, Some(delimiter)).unwrap().collect();
    
    let mut result = Vec::new();
    for entry in entries {
        if let Entry::CommonPrefix(prefix, Some(count)) = entry {
            result.push((prefix, count));
        }
    }
    result
}

#[test]
fn test_counting_accuracy_simple() -> Result<(), Box<dyn Error>> {
    let td = tempdir()?;
    let idx = td.path().join("test.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&idx, PAYLOAD_STORE_VERSION_V3)?;
    
    // Simple nested structure
    let keys = &[
        "a/1", "a/2/x", "a/2/y", "a/3/z",
        "b/1", "b/2",
        "c/deep/nested/file",
    ];
    
    for key in keys {
        builder.insert(key, None);
    }
    builder.close()?;

    let mut db = Database::<Value>::new();
    db.add_shard(&idx)?;

    // Verify all common prefix counts are accurate
    let reported_root = get_reported_counts(&db, "", '/');
    for (prefix, count) in &reported_root {
        let actual_descendants = count_actual_descendants(&db, prefix);
        assert_eq!(*count, actual_descendants, 
            "Count mismatch for prefix '{}': reported={}, actual={}", prefix, count, actual_descendants);
    }

    let reported_a = get_reported_counts(&db, "a/", '/');
    for (prefix, count) in &reported_a {
        let actual_descendants = count_actual_descendants(&db, prefix);
        assert_eq!(*count, actual_descendants, 
            "Count mismatch for prefix '{}': reported={}, actual={}", prefix, count, actual_descendants);
    }

    Ok(())
}

#[test]
fn test_counting_accuracy_multi_shard() -> Result<(), Box<dyn Error>> {
    let td = tempdir()?;
    
    // Shard 1
    let idx1 = td.path().join("s1.idx");
    let mut b1 = DatabaseBuilder::<Value>::new(&idx1, PAYLOAD_STORE_VERSION_V3)?;
    for key in &["prefix/a/1", "prefix/a/2", "prefix/b/1", "other/x"] {
        b1.insert(key, None);
    }
    b1.close()?;
    
    // Shard 2
    let idx2 = td.path().join("s2.idx");
    let mut b2 = DatabaseBuilder::<Value>::new(&idx2, PAYLOAD_STORE_VERSION_V3)?;
    for key in &["prefix/a/3", "prefix/c/1", "prefix/c/2", "different/y"] {
        b2.insert(key, None);
    }
    b2.close()?;

    let mut db = Database::<Value>::new();
    db.add_shard(&idx1)?;
    db.add_shard(&idx2)?;

    // Verify all common prefix counts are accurate across multiple shards
    let all_prefixes = get_reported_counts(&db, "", '/');
    for (prefix, count) in &all_prefixes {
        let actual_descendants = count_actual_descendants(&db, prefix);
        assert_eq!(*count, actual_descendants, 
            "Multi-shard count mismatch for prefix '{}': reported={}, actual={}", prefix, count, actual_descendants);
    }

    Ok(())
}

#[test]
fn test_counting_accuracy_sparse_ids() -> Result<(), Box<dyn Error>> {
    let td = tempdir()?;
    let idx = td.path().join("sparse.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&idx, PAYLOAD_STORE_VERSION_V3)?;
    
    // Create keys that might result in sparse FST output IDs
    let keys = &[
        "aaa/1", "aaa/2", 
        "zzz/1", "zzz/2", "zzz/3",  // Far apart lexicographically
        "mmm/deep/nested/1",
    ];
    
    for key in keys {
        builder.insert(key, None);
    }
    builder.close()?;

    let mut db = Database::<Value>::new();
    db.add_shard(&idx)?;

    // Verify counting accuracy with lexicographically distant keys (potentially sparse FST IDs)
    let all_prefixes = get_reported_counts(&db, "", '/');
    for (prefix, count) in &all_prefixes {
        let actual_descendants = count_actual_descendants(&db, prefix);
        assert_eq!(*count, actual_descendants, 
            "Sparse FST count mismatch for prefix '{}': reported={}, actual={}", prefix, count, actual_descendants);
    }

    Ok(())
}