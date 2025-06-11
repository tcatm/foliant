use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, Streamer};
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn debug_what_are_the_5_children_under_a2() -> Result<(), Box<dyn Error>> {
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

    println!("=== What are the 5 children under 'a/2/'? ===");
    println!("Expected: a/2/x, a/2/y, a/2/z (3 children)");
    
    // Let's drill down to see what's actually under "a/2/"
    let entries_under_a2: Vec<Entry<Value>> = db.list("a/2/", Some('/')).unwrap().collect();
    println!("Entries under 'a/2/': {} total", entries_under_a2.len());
    
    for (i, entry) in entries_under_a2.iter().enumerate() {
        match entry {
            Entry::CommonPrefix(p, count_opt) => {
                println!("  [{}] CommonPrefix: '{}' -> count: {:?}", i, p, count_opt);
            },
            Entry::Key(k, id, _) => {
                println!("  [{}] Key: '{}' -> id: {}", i, k, id);
            }
        }
    }
    
    // Let's also try without delimiter to see all keys under a/2/
    println!("\n=== All keys under 'a/2/' (no delimiter) ===");
    let all_keys_under_a2: Vec<Entry<Value>> = db.list("a/2/", None).unwrap().collect();
    println!("All keys under 'a/2/': {} total", all_keys_under_a2.len());
    
    for (i, entry) in all_keys_under_a2.iter().enumerate() {
        if let Entry::Key(k, id, _) = entry {
            println!("  [{}] Key: '{}' -> id: {}", i, k, id);
        }
    }
    
    Ok(())
}