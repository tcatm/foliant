use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Streamer, SearchIndexBuilder};
use foliant::multi_list::{LazySearchFilter, LazyShardFilter};
use serde_json::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn test_file_extension_search() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index with various file names
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("image001.jpg", Some(Value::String("JPEG image".to_string())));
    builder.insert("image002.jpg", Some(Value::String("JPEG image".to_string())));
    builder.insert("photo.png", Some(Value::String("PNG image".to_string())));
    builder.insert("document.pdf", Some(Value::String("PDF document".to_string())));
    builder.insert("script.js", Some(Value::String("JavaScript file".to_string())));
    builder.insert("style.css", Some(Value::String("CSS file".to_string())));
    builder.close()?;
    
    // Build search index
    {
        let mut db = Database::<Value>::open(&base)?;
        SearchIndexBuilder::build_index(&mut db, None)?;
    }
    
    let db = Database::<Value>::open(&base)?;
    
    // Test searching for "pg" - should find JPG files
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::new("pg".to_string())
    );
    let mut results: Vec<_> = db.list_with_filter("", None, filter)?.collect();
    results.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    
    assert_eq!(results.len(), 2, "Should find 2 JPG files");
    assert_eq!(results[0].as_str(), "image001.jpg");
    assert_eq!(results[1].as_str(), "image002.jpg");
    
    // Test searching for "jp" - should find JPG files
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::new("jp".to_string())
    );
    let results: Vec<_> = db.list_with_filter("", None, filter)?.collect();
    
    assert_eq!(results.len(), 2, "Should find 2 JPG files");
    
    // Test searching for "ng" - should find PNG file
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::new("ng".to_string())
    );
    let results: Vec<_> = db.list_with_filter("", None, filter)?.collect();
    
    assert_eq!(results.len(), 1, "Should find 1 PNG file");
    assert_eq!(results[0].as_str(), "photo.png");
    
    // Test searching for single letter "j" - should find JPG and JS files
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::new("j".to_string())
    );
    let mut results: Vec<_> = db.list_with_filter("", None, filter)?.collect();
    results.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    
    assert_eq!(results.len(), 3, "Should find 2 JPG files and 1 JS file");
    assert!(results.iter().any(|r| r.as_str().ends_with(".jpg")));
    assert!(results.iter().any(|r| r.as_str().ends_with(".js")));
    
    Ok(())
}