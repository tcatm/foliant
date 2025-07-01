use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Streamer, SearchIndexBuilder, TagMode, TagIndexBuilder};
use foliant::multi_list::{LazySearchFilter, LazyTagFilter, LazyShardFilter, ComposedFilter};
use serde_json::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn test_list_with_search_filter() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build database with test data
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("users/alice/profile", Some(Value::String("Alice's profile".to_string())));
    builder.insert("users/alice/settings", Some(Value::String("Alice's settings".to_string())));
    builder.insert("users/bob/profile", Some(Value::String("Bob's profile".to_string())));
    builder.insert("users/bob/settings", Some(Value::String("Bob's settings".to_string())));
    builder.insert("admin/alice/permissions", Some(Value::String("Admin perms".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Test 1: List all users with "alice" in the key
    let search_filter: Box<dyn LazyShardFilter<Value, _>> = 
        Box::new(LazySearchFilter::new("alice".to_string()));
    
    let results: Vec<String> = db.list_with_filter(
        "users/",
        None,
        search_filter
    )?.collect().into_iter().map(|e| e.as_str().to_string()).collect();
    
    assert_eq!(results.len(), 2);
    assert!(results.contains(&"users/alice/profile".to_string()));
    assert!(results.contains(&"users/alice/settings".to_string()));
    
    Ok(())
}

#[test]
fn test_list_with_multiple_filters() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build database with test data
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("docs/public/readme.md", Some(Value::String("Public readme".to_string())));
    builder.insert("docs/public/guide.md", Some(Value::String("Public guide".to_string())));
    builder.insert("docs/private/readme.md", Some(Value::String("Private readme".to_string())));
    builder.insert("docs/private/secrets.md", Some(Value::String("Secrets".to_string())));
    builder.close()?;
    
    // Build indices
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Build tag indices manually
    let shard = &db.shards()[0];
    let mut tag_builder = TagIndexBuilder::new(shard.idx_path());
    tag_builder.insert_tags(1, vec!["private"]);        // docs/private/readme.md
    tag_builder.insert_tags(2, vec!["private", "sensitive"]);  // docs/private/secrets.md
    tag_builder.insert_tags(3, vec!["public", "tutorial"]);   // docs/public/guide.md
    tag_builder.insert_tags(4, vec!["public"]);         // docs/public/readme.md
    tag_builder.finish()?;
    
    // Reload database to pick up tag index
    let mut db = Database::<Value>::open(&base)?;
    db.load_tag_index()?;
    
    // Test: List docs that contain "readme" AND have tag "public"
    let search_filter: Box<dyn LazyShardFilter<Value, _>> = 
        Box::new(LazySearchFilter::new("readme".to_string()));
    
    let tag_filter: Box<dyn LazyShardFilter<Value, _>> = 
        Box::new(LazyTagFilter::new(
            vec!["public".to_string()],
            vec![],
            TagMode::And
        ));
    
    let composed_filter: Box<dyn LazyShardFilter<Value, _>> = 
        Box::new(ComposedFilter::new(vec![search_filter, tag_filter]));
    
    let results: Vec<String> = db.list_with_filter(
        "docs/",
        None,
        composed_filter
    )?.collect().into_iter().map(|e| e.as_str().to_string()).collect();
    
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], "docs/public/readme.md");
    
    Ok(())
}

#[test]
fn test_list_with_filters_and_delimiter() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build database with test data
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("projects/rust/server/main.rs", Some(Value::String("Main server".to_string())));
    builder.insert("projects/rust/client/main.rs", Some(Value::String("Main client".to_string())));
    builder.insert("projects/python/server/app.py", Some(Value::String("Python server".to_string())));
    builder.insert("projects/python/client/ui.py", Some(Value::String("Python UI".to_string())));
    builder.close()?;
    
    // Build indices
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Build tag indices manually
    let shard = &db.shards()[0];
    let mut tag_builder = TagIndexBuilder::new(shard.idx_path());
    tag_builder.insert_tags(1, vec!["frontend"]);  // projects/python/client/ui.py
    tag_builder.insert_tags(2, vec!["backend"]);   // projects/python/server/app.py
    tag_builder.insert_tags(3, vec!["frontend"]);  // projects/rust/client/main.rs
    tag_builder.insert_tags(4, vec!["backend"]);   // projects/rust/server/main.rs
    tag_builder.finish()?;
    
    // Reload database to pick up tag index
    let mut db = Database::<Value>::open(&base)?;
    db.load_tag_index()?;
    
    // Test: List project directories containing "rust" with delimiter
    let search_filter: Box<dyn LazyShardFilter<Value, _>> = 
        Box::new(LazySearchFilter::new("rust".to_string()));
    
    let mut results: Vec<String> = db.list_with_filter(
        "projects/",
        Some('/'),
        search_filter
    )?.collect().into_iter().map(|e| e.as_str().to_string()).collect();
    
    results.sort();
    
    // Should get the common prefix for rust directory since we're using delimiter
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], "projects/rust/");
    
    Ok(())
}

#[test]
fn test_list_with_search_exclude_filter() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build database with test data
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("config/app.json", Some(Value::String("App config".to_string())));
    builder.insert("config/app.backup.json", Some(Value::String("Backup config".to_string())));
    builder.insert("config/database.json", Some(Value::String("DB config".to_string())));
    builder.insert("config/database.backup.json", Some(Value::String("Backup DB".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Test: List configs containing "json" but not "backup"
    let search_filter: Box<dyn LazyShardFilter<Value, _>> = 
        Box::new(LazySearchFilter::new("json".to_string()).exclude("backup".to_string()));
    
    let mut results: Vec<String> = db.list_with_filter(
        "config/",
        None,
        search_filter
    )?.collect().into_iter().map(|e| e.as_str().to_string()).collect();
    
    results.sort();
    
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], "config/app.json");
    assert_eq!(results[1], "config/database.json");
    
    Ok(())
}