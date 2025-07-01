use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Streamer, SearchIndexBuilder};
use foliant::multi_list::{LazySearchFilter, LazyShardFilter};
use serde_json::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn test_single_letter_search() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("apple", Some(Value::String("fruit".to_string())));
    builder.insert("application", Some(Value::String("software".to_string()))); 
    builder.insert("banana", Some(Value::String("fruit".to_string())));
    builder.insert("band", Some(Value::String("music".to_string())));
    builder.insert("zebra", Some(Value::String("animal".to_string())));
    builder.close()?;
    
    // Build search index
    {
        let mut db = Database::<Value>::open(&base)?;
        SearchIndexBuilder::build_index(&mut db, None)?;
    }
    
    let db = Database::<Value>::open(&base)?;
    
    // Test single letter 'a' - should find all words containing 'a'
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::new("a".to_string())
    );
    let mut results: Vec<_> = db.list_with_filter("", None, filter)?.collect();
    results.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    
    println!("Found {} results for 'a':", results.len());
    for r in &results {
        println!("  - {}", r.as_str());
    }
    assert_eq!(results.len(), 5); // apple, application, banana, band, zebra all contain 'a'
    
    // Test single letter 'z' - should only find zebra
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::new("z".to_string())
    );
    let results: Vec<_> = db.list_with_filter("", None, filter)?.collect();
    
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "zebra");
    
    Ok(())
}

#[test] 
fn test_two_letter_search() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("apple", Some(Value::String("fruit".to_string())));
    builder.insert("application", Some(Value::String("software".to_string())));
    builder.insert("apricot", Some(Value::String("fruit".to_string())));
    builder.insert("banana", Some(Value::String("fruit".to_string())));
    builder.close()?;
    
    // Build search index
    {
        let mut db = Database::<Value>::open(&base)?;
        SearchIndexBuilder::build_index(&mut db, None)?;
    }
    
    let db = Database::<Value>::open(&base)?;
    
    // Test two letters 'ap' - should find apple, application, apricot
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::new("ap".to_string())
    );
    let mut results: Vec<_> = db.list_with_filter("", None, filter)?.collect();
    results.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_str(), "apple");
    assert_eq!(results[1].as_str(), "application");
    assert_eq!(results[2].as_str(), "apricot");
    
    // Test two letters 'na' - should only find banana
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::new("na".to_string())
    );
    let results: Vec<_> = db.list_with_filter("", None, filter)?.collect();
    
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "banana");
    
    Ok(())
}