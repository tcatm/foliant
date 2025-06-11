use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, Streamer};
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn debug_simple_bounds_calculation() -> Result<(), Box<dyn Error>> {
    let td = tempdir()?;
    let idx = td.path().join("shard.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&idx, PAYLOAD_STORE_VERSION_V3)?;
    
    // Very simple test case: just 3 keys to make bounds analysis easier
    let keys = &["a/1", "a/2", "b/1"];
    for key in keys {
        builder.insert(key, None);
    }
    builder.close()?;

    let mut db = Database::<Value>::new();
    db.add_shard(&idx)?;

    // Test root level grouping by '/'
    println!("=== Testing simple case: keys = {:?} ===", keys);
    
    let entries: Vec<Entry<Value>> = db.list("", Some('/')).unwrap().collect();
    println!("Raw entries: {:?}", entries);
    
    let mut counts = entries
        .into_iter()
        .filter_map(|e| match e {
            Entry::CommonPrefix(p, Some(n)) => {
                println!("CommonPrefix: '{}' with {} children", p, n);
                Some((p.to_string(), n as usize))
            },
            Entry::Key(k, id, _) => {
                println!("Key: '{}' with id {}", k, id);
                None
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    counts.sort_unstable();
    
    println!("Final counts: {:?}", counts);
    
    // Expected: a/ should have 2 children (a/1, a/2), b/ should have 1 child (b/1)
    assert_eq!(counts, vec![
        ("a/".to_string(), 2),
        ("b/".to_string(), 1),
    ]);
    
    Ok(())
}

#[test]
fn debug_original_failing_case() -> Result<(), Box<dyn Error>> {
    let td = tempdir()?;
    let idx = td.path().join("shard.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&idx, PAYLOAD_STORE_VERSION_V3)?;
    
    // Original failing test case
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

    println!("=== Testing original case: keys = {:?} ===", keys);
    
    let entries: Vec<Entry<Value>> = db.list("", Some('/')).unwrap().collect();
    println!("Raw entries: {:?}", entries);
    
    let mut counts = entries
        .into_iter()
        .filter_map(|e| match e {
            Entry::CommonPrefix(p, Some(n)) => {
                println!("CommonPrefix: '{}' with {} children", p, n);
                Some((p.to_string(), n as usize))
            },
            Entry::Key(k, id, _) => {
                println!("Key: '{}' with id {}", k, id);
                None
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    counts.sort_unstable();
    
    println!("Final counts: {:?}", counts);
    println!("Expected: a/ -> 4, b/ -> 2");
    println!("Actual: a/ -> {}, b/ -> {}", 
        counts.iter().find(|(p, _)| p == "a/").map(|(_, n)| *n).unwrap_or(0),
        counts.iter().find(|(p, _)| p == "b/").map(|(_, n)| *n).unwrap_or(0)
    );
    
    Ok(())
}

#[test] 
fn debug_step_by_step_a_prefix() -> Result<(), Box<dyn Error>> {
    let td = tempdir()?;
    let idx = td.path().join("shard.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&idx, PAYLOAD_STORE_VERSION_V3)?;
    
    let keys = &["a/1", "a/2/x", "a/2/y", "a/2/z"];
    for key in keys {
        builder.insert(key, None);
    }
    builder.close()?;

    let mut db = Database::<Value>::new();
    db.add_shard(&idx)?;

    println!("=== Step by step analysis for 'a/' prefix ===");
    println!("Keys under 'a/': {:?}", keys);
    
    // First, let's see what happens when we list with prefix "a/"
    let entries_under_a: Vec<Entry<Value>> = db.list("a/", Some('/')).unwrap().collect();
    println!("Entries under 'a/': {:?}", entries_under_a);
    
    // Count the direct children vs grouped children
    let mut direct_keys = 0;
    let mut grouped_children = 0;
    
    for entry in entries_under_a {
        match entry {
            Entry::Key(_, _, _) => {
                direct_keys += 1;
                println!("  Direct key found");
            },
            Entry::CommonPrefix(p, Some(n)) => {
                grouped_children += n as usize;
                println!("  CommonPrefix '{}' with {} children", p, n);
            },
            _ => {}
        }
    }
    
    println!("Under 'a/': {} direct keys + {} grouped children = {} total", 
             direct_keys, grouped_children, direct_keys + grouped_children);
    
    // Now let's see what the root level thinks
    let root_entries: Vec<Entry<Value>> = db.list("", Some('/')).unwrap().collect();
    for entry in root_entries {
        if let Entry::CommonPrefix(p, Some(n)) = entry {
            if p == "a/" {
                println!("Root level says 'a/' has {} children", n);
                break;
            }
        }
    }
    
    Ok(())
}