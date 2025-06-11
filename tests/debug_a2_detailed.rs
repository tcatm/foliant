use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, Streamer};
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn debug_a2_detailed_bounds() -> Result<(), Box<dyn Error>> {
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

    println!("=== Detailed analysis of 'a/2/' bounds ===");
    println!("Keys: {:?}", keys);
    println!("Expected under 'a/2/': a/2/x (id=2), a/2/y (id=3), a/2/z (id=4) = 3 children");
    
    // Let's manually trace what should happen:
    // 1. From root, take 'a' transition -> should have ub=4 (we confirmed this works)
    // 2. From 'a', take '/' transition -> should inherit ub=4
    // 3. From 'a/', take '2' transition -> should have range [2, 4] intersect with whatever bounds
    // 4. From 'a/2', take '/' transition -> should only cover a/2/x, a/2/y, a/2/z (ids 2,3,4)
    
    // Step 1: Root level - this should work now
    println!("\n--- Step 1: Root level transitions ---");
    let root_entries: Vec<Entry<Value>> = db.list("", Some('/')).unwrap().collect();
    for entry in &root_entries {
        if let Entry::CommonPrefix(p, Some(n)) = entry {
            println!("Root CommonPrefix: '{}' -> {} children", p, n);
        }
    }
    
    // Step 2: Under 'a/' - see what the bounds look like
    println!("\n--- Step 2: Under 'a/' transitions ---");
    let a_entries: Vec<Entry<Value>> = db.list("a/", Some('/')).unwrap().collect();
    for entry in &a_entries {
        match entry {
            Entry::CommonPrefix(p, Some(n)) => {
                println!("Under 'a/' CommonPrefix: '{}' -> {} children", p, n);
                if p == "a/2/" {
                    println!("^^^ This should be 3, not {}", n);
                }
            },
            Entry::CommonPrefix(p, None) => {
                println!("Under 'a/' CommonPrefix: '{}' -> None children", p);
            },
            Entry::Key(k, id, _) => {
                println!("Under 'a/' Key: '{}' -> id: {}", k, id);
            }
        }
    }
    
    // Step 3: Under 'a/2/' - see the actual keys
    println!("\n--- Step 3: Under 'a/2/' actual keys ---");
    let a2_entries: Vec<Entry<Value>> = db.list("a/2/", None).unwrap().collect();
    for (i, entry) in a2_entries.iter().enumerate() {
        if let Entry::Key(k, id, _) = entry {
            println!("[{}] Key: '{}' -> id: {}", i, k, id);
        }
    }
    
    Ok(())
}