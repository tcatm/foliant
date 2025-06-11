use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, Streamer};
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

/// Helper to collect (prefix, child_count) of CommonPrefix entries under a given prefix.
fn common_prefix_counts(
    db: &Database<Value>,
    prefix: &str,
    delim: char,
) -> Vec<(String, usize)> {
    let entries: Vec<Entry<Value>> = db.list(prefix, Some(delim)).unwrap().collect();
    let mut counts = entries
        .into_iter()
        .filter_map(|e| match e {
            Entry::CommonPrefix(p, Some(n)) => Some((p.to_string(), n as usize)),
            _ => None,
        })
        .collect::<Vec<_>>();
    counts.sort_unstable();
    counts
}

/// Test children-count (unfiltered) at various levels for a single shard.
#[test]
fn children_count_single_shard_various_levels() -> Result<(), Box<dyn Error>> {
    let td = tempdir()?;
    let idx = td.path().join("shard.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&idx, PAYLOAD_STORE_VERSION_V3)?;
    let keys = &[
        "a/1", "a/2/x", "a/2/y", "a/2/z",
        "b/1", "b/2",
    ];
    for key in keys {
        builder.insert(key, None);
    }
    builder.close()?;

    let mut db = Database::<Value>::new();
    db.add_shard(&idx)?;

    // Root level grouping by '/'
    let root_counts = common_prefix_counts(&db, "", '/');
    assert_eq!(root_counts, vec![
        ("a/".to_string(), 4),
        ("b/".to_string(), 2),
    ]);

    // Group under prefix "a/"
    let a_counts = common_prefix_counts(&db, "a/", '/');
    assert_eq!(a_counts, vec![
        ("a/2/".to_string(), 3),
    ]);

    // Under deeper prefix "a/2/": expect leaf keys a/2/x, a/2/y, a/2/z
    let entries = db.list("a/2/", Some('/'))?.collect();
    let mut leaf_keys = entries
        .into_iter()
        .filter_map(|e| match e {
            Entry::Key(k, _, _) => Some(k.to_string()),
            _ => None,
        })
        .collect::<Vec<_>>();
    leaf_keys.sort_unstable();
    assert_eq!(leaf_keys, vec!["a/2/x".to_string(), "a/2/y".to_string(), "a/2/z".to_string()]);
    Ok(())
}

/// Test children-count (unfiltered) combining two shards.
#[test]
fn children_count_multi_shard_combined() -> Result<(), Box<dyn Error>> {
    let td = tempdir()?;
    // Shard 1
    let idx1 = td.path().join("s1.idx");
    let mut b1 = DatabaseBuilder::<Value>::new(&idx1, PAYLOAD_STORE_VERSION_V3)?;
    for key in &["a/1", "a/2", "b/1"] {
        b1.insert(key, None);
    }
    b1.close()?;
    // Shard 2
    let idx2 = td.path().join("s2.idx");
    let mut b2 = DatabaseBuilder::<Value>::new(&idx2, PAYLOAD_STORE_VERSION_V3)?;
    for key in &["a/1", "a/3", "c/1"] {
        b2.insert(key, None);
    }
    b2.close()?;

    let mut db = Database::<Value>::new();
    db.add_shard(&idx1)?;
    db.add_shard(&idx2)?;

    // Root level grouping by '/'
    let root_counts = common_prefix_counts(&db, "", '/');
    assert_eq!(root_counts, vec![
        ("a/".to_string(), 4), // shard1:2 + shard2:2
        ("b/".to_string(), 1), // shard1:1
        ("c/".to_string(), 1), // shard2:1
    ]);

    // Group under prefix "a/": should return individual keys since they're leaf nodes
    let a_entries: Vec<Entry<Value>> = db.list("a/", Some('/')).unwrap().collect();
    let mut a_keys: Vec<String> = a_entries
        .into_iter()
        .filter_map(|e| match e {
            Entry::Key(k, _, _) => Some(k),
            _ => None,
        })
        .collect();
    a_keys.sort_unstable();
    assert_eq!(a_keys, vec!["a/1".to_string(), "a/2".to_string(), "a/3".to_string()]);
    Ok(())
}