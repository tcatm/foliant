use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, Streamer};
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

/// Helper to collect (prefix, child_count) of CommonPrefix entries under a given prefix,
/// but with detailed debugging of the counting process.
fn debug_common_prefix_counts(
    db: &Database<Value>,
    prefix: &str,
    delim: char,
) -> Vec<(String, usize)> {
    println!("=== Debugging common_prefix_counts for prefix '{}' ===", prefix);
    
    let entries: Vec<Entry<Value>> = db.list(prefix, Some(delim)).unwrap().collect();
    println!("Total entries found: {}", entries.len());
    
    for (i, entry) in entries.iter().enumerate() {
        match entry {
            Entry::CommonPrefix(p, count_opt) => {
                println!("  [{}] CommonPrefix: '{}' -> count: {:?}", i, p, count_opt);
            },
            Entry::Key(k, id, _) => {
                println!("  [{}] Key: '{}' -> id: {}", i, k, id);
            }
        }
    }
    
    let mut counts = entries
        .into_iter()
        .filter_map(|e| match e {
            Entry::CommonPrefix(p, Some(n)) => Some((p.to_string(), n as usize)),
            _ => None,
        })
        .collect::<Vec<_>>();
    counts.sort_unstable();
    
    println!("Final result: {:?}", counts);
    counts
}

#[test]
fn debug_simple_case_bounds() -> Result<(), Box<dyn Error>> {
    let td = tempdir()?;
    let idx = td.path().join("shard.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&idx, PAYLOAD_STORE_VERSION_V3)?;
    
    // Minimal case to understand the counting issue
    let keys = &["a/1", "a/2"];
    for key in keys {
        builder.insert(key, None);
    }
    builder.close()?;

    let mut db = Database::<Value>::new();
    db.add_shard(&idx)?;

    println!("=== Simple case: {} ===", keys.join(", "));
    
    let counts = debug_common_prefix_counts(&db, "", '/');
    
    // Expected: a/ should have exactly 2 children (a/1, a/2)
    println!("Expected: a/ -> 2");
    println!("Actual: a/ -> {}", 
        counts.iter().find(|(p, _)| p == "a/").map(|(_, n)| *n).unwrap_or(0)
    );
    
    Ok(())
}

#[test] 
fn debug_medium_case_bounds() -> Result<(), Box<dyn Error>> {
    let td = tempdir()?;
    let idx = td.path().join("shard.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&idx, PAYLOAD_STORE_VERSION_V3)?;
    
    // Medium case
    let keys = &["a/1", "a/2", "a/3"];
    for key in keys {
        builder.insert(key, None);
    }
    builder.close()?;

    let mut db = Database::<Value>::new();
    db.add_shard(&idx)?;

    println!("=== Medium case: {} ===", keys.join(", "));
    
    let counts = debug_common_prefix_counts(&db, "", '/');
    
    // Expected: a/ should have exactly 3 children (a/1, a/2, a/3)
    println!("Expected: a/ -> 3");
    println!("Actual: a/ -> {}", 
        counts.iter().find(|(p, _)| p == "a/").map(|(_, n)| *n).unwrap_or(0)
    );
    
    Ok(())
}