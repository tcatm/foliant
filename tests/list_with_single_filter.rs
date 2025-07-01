use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Streamer};
use serde_json::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn test_list_with_filter_simple() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build database with test data
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("hello", Some(Value::String("world".to_string())));
    builder.insert("goodbye", Some(Value::String("world".to_string())));
    builder.close()?;
    
    let db = Database::<Value>::open(&base)?;
    
    // Test 1: List without filter
    let all_results: Vec<String> = db.list("", None)?
        .collect()
        .into_iter()
        .map(|e| e.as_str().to_string())
        .collect();
    
    assert_eq!(all_results.len(), 2);
    assert!(all_results.contains(&"hello".to_string()));
    assert!(all_results.contains(&"goodbye".to_string()));
    
    Ok(())
}