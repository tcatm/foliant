use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Streamer, SearchIndexBuilder};
use foliant::multi_list::{LazySearchFilter, LazyShardFilter};
use serde_json::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn test_search_filter_multiple_included_terms() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index with test data
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("hello_world_program", Some(Value::String("greeting".to_string())));
    builder.insert("hello_universe", Some(Value::String("big greeting".to_string())));
    builder.insert("world_peace", Some(Value::String("hope".to_string())));
    builder.insert("hello_world_and_universe", Some(Value::String("all".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Test: Search for entries containing both "hello" AND "world"
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::with_terms(
            vec!["hello".to_string(), "world".to_string()],
            vec![]
        )
    );
    
    let results: Vec<String> = db.list_with_filter("", None, filter)?
        .collect()
        .into_iter()
        .map(|e| e.as_str().to_string())
        .collect();
    
    let mut sorted_results = results;
    sorted_results.sort();
    assert_eq!(sorted_results, vec!["hello_world_and_universe", "hello_world_program"]);
    
    Ok(())
}

#[test]
fn test_search_filter_with_exclusions() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index with test data
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("good_apple", Some(Value::String("fruit".to_string())));
    builder.insert("bad_apple", Some(Value::String("rotten".to_string())));
    builder.insert("apple_pie", Some(Value::String("dessert".to_string())));
    builder.insert("apple_juice_bad_taste", Some(Value::String("drink".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Test: Search for "apple" but exclude anything with "bad"
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::new("apple".to_string())
            .exclude("bad".to_string())
    );
    
    let results: Vec<String> = db.list_with_filter("", None, filter)?
        .collect()
        .into_iter()
        .map(|e| e.as_str().to_string())
        .collect();
    
    let mut sorted_results = results;
    sorted_results.sort();
    assert_eq!(sorted_results, vec!["apple_pie", "good_apple"]);
    
    Ok(())
}

#[test]
fn test_search_filter_unicode_with_exclusions() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index with Unicode test data
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("café_good", Some(Value::String("nice coffee".to_string())));
    builder.insert("café_terrible", Some(Value::String("bad coffee".to_string())));
    builder.insert("résumé_café", Some(Value::String("cv at coffee shop".to_string())));
    builder.insert("terrible_résumé", Some(Value::String("bad cv".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Test: Search for "café" but exclude "terrible"
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::new("café".to_string())
            .exclude("terrible".to_string())
    );
    
    let results: Vec<String> = db.list_with_filter("", None, filter)?
        .collect()
        .into_iter()
        .map(|e| e.as_str().to_string())
        .collect();
    
    let mut sorted_results = results;
    sorted_results.sort();
    assert_eq!(sorted_results, vec!["café_good", "résumé_café"]);
    
    Ok(())
}

#[test]
fn test_search_filter_builder_pattern() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index with test data
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("rust_programming_book", Some(Value::String("tech".to_string())));
    builder.insert("rust_game_engine", Some(Value::String("gaming".to_string())));
    builder.insert("c_programming_book", Some(Value::String("tech".to_string())));
    builder.insert("rust_programming_tutorial_old", Some(Value::String("outdated".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Test: Build a complex filter using builder pattern
    let filter: Box<dyn LazyShardFilter<Value, _>> = Box::new(
        LazySearchFilter::new("rust".to_string())
            .include("programming".to_string())
            .exclude("old".to_string())
    );
    
    let results: Vec<String> = db.list_with_filter("", None, filter)?
        .collect()
        .into_iter()
        .map(|e| e.as_str().to_string())
        .collect();
    
    assert_eq!(results, vec!["rust_programming_book"]);
    
    Ok(())
}