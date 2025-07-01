use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, Streamer, SearchIndexBuilder};
use foliant::multi_list::{LazySearchFilter, LazyShardFilter};
use serde_json::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn test_search_advanced_api() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index with test data
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("rust_programming_book", Some(Value::String("Rust guide".to_string())));
    builder.insert("rust_web_development", Some(Value::String("Web with Rust".to_string())));
    builder.insert("python_programming_book", Some(Value::String("Python guide".to_string())));
    builder.insert("rust_game_development", Some(Value::String("Games with Rust".to_string())));
    builder.insert("rust_programming_tutorial_old", Some(Value::String("Outdated Rust".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Test 1: Search for entries with "rust" AND "programming", excluding "old"
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::with_terms(
            vec!["rust".to_string(), "programming".to_string()],
            vec!["old".to_string()]
        )
    );
    let results: Vec<Entry<Value>> = db.list_with_filter("", None, filter)?.collect();
    
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "rust_programming_book");
    
    // Test 2: Search for entries with "rust" AND "development"
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::with_terms(
            vec!["rust".to_string(), "development".to_string()],
            vec![]
        )
    );
    let results: Vec<Entry<Value>> = db.list_with_filter("", None, filter)?.collect();
    
    let mut keys: Vec<&str> = results.iter().map(|e| e.as_str()).collect();
    keys.sort();
    assert_eq!(keys, vec!["rust_game_development", "rust_web_development"]);
    
    // Test 3: Search for "programming" excluding "python"
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::with_terms(
            vec!["programming".to_string()],
            vec!["python".to_string()]
        )
    );
    let results: Vec<Entry<Value>> = db.list_with_filter("", None, filter)?.collect();
    
    let mut keys: Vec<&str> = results.iter().map(|e| e.as_str()).collect();
    keys.sort();
    assert_eq!(keys, vec!["rust_programming_book", "rust_programming_tutorial_old"]);
    
    Ok(())
}

#[test]
fn test_search_advanced_with_prefix() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index with test data
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("app/frontend/main.js", Some(Value::String("JS".to_string())));
    builder.insert("app/frontend/style.css", Some(Value::String("CSS".to_string())));
    builder.insert("app/backend/main.py", Some(Value::String("Python".to_string())));
    builder.insert("docs/frontend/guide.md", Some(Value::String("Guide".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Search for "frontend" with prefix "app/"
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::new("frontend".to_string())
    );
    let results: Vec<Entry<Value>> = db.list_with_filter("app/", None, filter)?.collect();
    
    let mut keys: Vec<&str> = results.iter().map(|e| e.as_str()).collect();
    keys.sort();
    assert_eq!(keys, vec!["app/frontend/main.js", "app/frontend/style.css"]);
    
    Ok(())
}

#[test]
fn test_search_advanced_unicode() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index with Unicode test data
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("café_français_bon", Some(Value::String("Good French coffee".to_string())));
    builder.insert("café_français_mauvais", Some(Value::String("Bad French coffee".to_string())));
    builder.insert("thé_français_bon", Some(Value::String("Good French tea".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Search for "café" AND "français", excluding "mauvais"
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::with_terms(
            vec!["café".to_string(), "français".to_string()],
            vec!["mauvais".to_string()]
        )
    );
    let results: Vec<Entry<Value>> = db.list_with_filter("", None, filter)?.collect();
    
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "café_français_bon");
    
    Ok(())
}