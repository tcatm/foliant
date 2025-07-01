use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry};
use serde_json::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn test_val_lookup_with_shard() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    
    // Create first shard
    let shard1_path = dir.path().join("shard1.idx");
    let mut builder1 = DatabaseBuilder::<Value>::new(&shard1_path, PAYLOAD_STORE_VERSION_V3)?;
    builder1.insert("apple", Some(Value::String("fruit".to_string())));
    builder1.insert("banana", Some(Value::String("yellow fruit".to_string())));
    builder1.close()?;
    
    // Create second shard
    let shard2_path = dir.path().join("shard2.idx");
    let mut builder2 = DatabaseBuilder::<Value>::new(&shard2_path, PAYLOAD_STORE_VERSION_V3)?;
    builder2.insert("carrot", Some(Value::String("vegetable".to_string())));
    builder2.insert("daikon", Some(Value::String("radish".to_string())));
    builder2.close()?;
    
    // Load both shards into a database
    let mut db = Database::<Value>::new();
    db.add_shard(&shard1_path)?;
    db.add_shard(&shard2_path)?;
    
    // Get some entries to find their IDs
    let apple_entry = db.list("apple", None)?.next().unwrap();
    let carrot_entry = db.list("carrot", None)?.next().unwrap();
    
    let apple_id = match &apple_entry {
        Entry::Key(key, id, _) => {
            println!("Apple entry: {} -> {}", key, id);
            *id
        },
        _ => panic!("Expected key entry"),
    };
    
    let carrot_id = match &carrot_entry {
        Entry::Key(key, id, _) => {
            println!("Carrot entry: {} -> {}", key, id);
            *id
        },
        _ => panic!("Expected key entry"),
    };
    
    // Test 1: With multiple shards, we must specify which shard to look in
    // This prevents ambiguity when IDs collide across shards
    
    // Test 2: Lookup from specific shard - correct shard
    let lookup_apple_shard0 = db.get_key_from_shard(0, apple_id)?;
    assert!(lookup_apple_shard0.is_some());
    if let Some(Entry::Key(key, _, _)) = &lookup_apple_shard0 {
        assert_eq!(key, "apple");
    }
    
    let lookup_carrot_shard1 = db.get_key_from_shard(1, carrot_id)?;
    assert!(lookup_carrot_shard1.is_some());
    if let Some(Entry::Key(key, _, _)) = &lookup_carrot_shard1 {
        assert_eq!(key, "carrot");
    }
    
    // Test 3: Lookup from wrong shard
    // Since IDs might collide (both apple and carrot might have ID 1),
    // looking up in the "wrong" shard might still return something, but it won't be the right key
    let lookup_wrong_shard = db.get_key_from_shard(1, apple_id)?;
    if apple_id == carrot_id {
        // If IDs collide, we'll get carrot when looking for apple in shard 1
        if let Some(Entry::Key(key, _, _)) = &lookup_wrong_shard {
            assert_ne!(key, "apple", "Should not find apple in shard 1");
        }
    } else {
        // If IDs don't collide, we shouldn't find anything
        assert!(lookup_wrong_shard.is_none());
    }
    
    // Test 4: Invalid shard index - should return None
    let lookup_invalid_shard = db.get_key_from_shard(99, apple_id)?;
    assert!(lookup_invalid_shard.is_none());
    
    Ok(())
}