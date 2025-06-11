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
fn debug_exact_failing_case() -> Result<(), Box<dyn Error>> {
    let td = tempdir()?;
    let idx = td.path().join("shard.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&idx, PAYLOAD_STORE_VERSION_V3)?;
    
    // This is the exact case that fails in the original test
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

    println!("=== Exact failing case: {} ===", keys.join(", "));
    
    let counts = debug_common_prefix_counts(&db, "", '/');
    
    // Original test expects: a/ -> 4, b/ -> 2
    // But we get: a/ -> 6, b/ -> 2
    println!("Test expects: a/ -> 4, b/ -> 2");
    println!("We get: a/ -> {}, b/ -> {}", 
        counts.iter().find(|(p, _)| p == "a/").map(|(_, n)| *n).unwrap_or(0),
        counts.iter().find(|(p, _)| p == "b/").map(|(_, n)| *n).unwrap_or(0)
    );
    
    // Now let's drill down into what's under "a/"
    println!("\n=== Analyzing what's under 'a/' ===");
    let entries_under_a: Vec<Entry<Value>> = db.list("a/", Some('/')).unwrap().collect();
    println!("Entries under 'a/': {} total", entries_under_a.len());
    
    let mut total_children = 0;
    for (i, entry) in entries_under_a.iter().enumerate() {
        match entry {
            Entry::CommonPrefix(p, count_opt) => {
                let count = count_opt.unwrap_or(0) as usize;
                total_children += count;
                println!("  [{}] CommonPrefix: '{}' -> {} children", i, p, count);
            },
            Entry::Key(k, id, _) => {
                total_children += 1;
                println!("  [{}] Key: '{}' -> id: {} (counts as 1 child)", i, k, id);
            }
        }
    }
    
    println!("Total children under 'a/': {}", total_children);
    println!("Expected children under 'a/': 4 (a/1 + 3 under a/2/)");
    
    if total_children == 4 {
        println!("✓ The drill-down count is correct!");
        println!("❌ But the root-level count is wrong. This suggests the issue is in bounds calculation.");
    } else {
        println!("❌ Even the drill-down count is wrong.");
    }
    
    Ok(())
}