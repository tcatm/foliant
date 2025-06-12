use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, TagIndexBuilder, Entry, TagMode};
use foliant::multi_list::{MultiShardListStreamer, TagFilterConfig, TagFilterBitmap};
use foliant::Streamer;
use serde_json::{json, Value};
use tempfile::tempdir;

#[test]
fn test_case_insensitive_tag_operations() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_db.idx");
    
    // Build database with tag index
    {
        let mut builder = DatabaseBuilder::<Value>::new(&db_path, PAYLOAD_STORE_VERSION_V3)?;
        builder.insert("entry1", Some(json!({"tags": ["Technology", "RUST", "programming"]})));
        builder.insert("entry2", Some(json!({"tags": ["technology", "Python", "PROGRAMMING"]})));
        builder.insert("entry3", Some(json!({"tags": ["TECHNOLOGY", "rust", "Testing"]})));
        builder.close()?;
        
        let mut db = Database::<Value>::open(&db_path)?;
        TagIndexBuilder::build_index(&mut db, "tags", None)?;
    }
    
    // Test case-insensitive tag filtering
    {
        let mut db = Database::<Value>::new();
        db.add_shard(&db_path)?;
        db.load_tag_index()?;
        
        // Helper function to search with tags
        let search_with_tags = |include_tags: &[&str], exclude_tags: &[&str], mode: TagMode| -> Result<Vec<Entry<Value>>, Box<dyn std::error::Error>> {
            let config = TagFilterConfig {
                include_tags: include_tags.iter().map(|s| s.to_string()).collect(),
                exclude_tags: exclude_tags.iter().map(|s| s.to_string()).collect(),
                mode,
            };
            let filter = TagFilterBitmap::new(&db.shards(), &config.include_tags, &config.exclude_tags, config.mode)?;
            let stream = MultiShardListStreamer::new_with_filter(
                &db.shards(),
                Vec::new(),
                None,
                Some(filter),
            )?;
            Ok(stream.collect())
        };
        
        // Test lowercase search finds all variants
        let results = search_with_tags(&["technology"], &[], TagMode::And)?;
        assert_eq!(results.len(), 3, "Should find all 3 entries with 'technology' variants");
        
        // Test uppercase search finds all variants  
        let results = search_with_tags(&["RUST"], &[], TagMode::And)?;
        assert_eq!(results.len(), 2, "Should find 2 entries with 'rust' variants");
        
        // Test mixed case search
        let results = search_with_tags(&["Programming"], &[], TagMode::And)?;
        assert_eq!(results.len(), 2, "Should find 2 entries with 'programming' variants");
        
        // Test multiple mixed case tags with AND
        let results = search_with_tags(&["TECHNOLOGY", "rust"], &[], TagMode::And)?;
        assert_eq!(results.len(), 2, "Should find 2 entries with both tags");
        
        // Test multiple mixed case tags with OR
        let results = search_with_tags(&["python", "TESTING"], &[], TagMode::Or)?;
        assert_eq!(results.len(), 2, "Should find 2 entries with either tag");
    }
    
    Ok(())
}

#[test] 
fn test_original_case_preservation() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_db.idx");
    
    // Build database with tag index
    {
        let mut builder = DatabaseBuilder::<Value>::new(&db_path, PAYLOAD_STORE_VERSION_V3)?;
        builder.insert("entry1", Some(json!({"tags": ["JavaScript", "WebDev", "FrontEnd"]})));
        builder.insert("entry2", Some(json!({"tags": ["BACKEND", "DATABASE", "API"]})));
        builder.insert("entry3", Some(json!({"tags": ["testing", "automation", "ci/cd"]})));
        builder.close()?;
        
        let mut db = Database::<Value>::open(&db_path)?;
        TagIndexBuilder::build_index(&mut db, "tags", None)?;
    }
    
    // Test original case retrieval
    {
        let mut db = Database::<Value>::new();
        db.add_shard(&db_path)?;
        db.load_tag_index()?;
        
        let shard = &db.shards()[0];
        let tag_index = shard.tag_index().unwrap();
        
        // Test getting original case for various tags
        let original_tags = tag_index.get_original_tags()?;
        
        // Verify original cases are preserved
        assert!(original_tags.contains(&"JavaScript".to_string()));
        assert!(original_tags.contains(&"WebDev".to_string())); 
        assert!(original_tags.contains(&"FrontEnd".to_string()));
        assert!(original_tags.contains(&"BACKEND".to_string()));
        assert!(original_tags.contains(&"DATABASE".to_string()));
        assert!(original_tags.contains(&"API".to_string()));
        assert!(original_tags.contains(&"testing".to_string()));
        assert!(original_tags.contains(&"automation".to_string()));
        assert!(original_tags.contains(&"ci/cd".to_string()));
        
        // Test individual tag case lookup
        assert_eq!(
            tag_index.get_original_case("javascript")?,
            Some("JavaScript".to_string())
        );
        assert_eq!(
            tag_index.get_original_case("backend")?, 
            Some("BACKEND".to_string())
        );
        assert_eq!(
            tag_index.get_original_case("testing")?,
            Some("testing".to_string())
        );
        
        // Test non-existent tag
        assert_eq!(
            tag_index.get_original_case("nonexistent")?,
            None
        );
    }
    
    Ok(())
}

#[test]
fn test_tag_filtering_with_mixed_case() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_db.idx");
    
    // Build database with tag index
    {
        let mut builder = DatabaseBuilder::<Value>::new(&db_path, PAYLOAD_STORE_VERSION_V3)?;
        builder.insert("entry1", Some(json!({"tags": ["Rust", "Backend", "Performance"]})));
        builder.insert("entry2", Some(json!({"tags": ["RUST", "frontend", "JavaScript"]})));
        builder.insert("entry3", Some(json!({"tags": ["rust", "BACKEND", "database"]})));
        builder.insert("entry4", Some(json!({"tags": ["Python", "backend", "API"]})));
        builder.close()?;
        
        let mut db = Database::<Value>::open(&db_path)?;
        TagIndexBuilder::build_index(&mut db, "tags", None)?;
    }
    
    {
        let mut db = Database::<Value>::new();
        db.add_shard(&db_path)?;
        db.load_tag_index()?;
        
        // Helper function to search with tags
        let search_with_tags = |include_tags: &[&str], exclude_tags: &[&str], mode: TagMode| -> Result<Vec<Entry<Value>>, Box<dyn std::error::Error>> {
            let config = TagFilterConfig {
                include_tags: include_tags.iter().map(|s| s.to_string()).collect(),
                exclude_tags: exclude_tags.iter().map(|s| s.to_string()).collect(),
                mode,
            };
            let filter = TagFilterBitmap::new(&db.shards(), &config.include_tags, &config.exclude_tags, config.mode)?;
            let stream = MultiShardListStreamer::new_with_filter(
                &db.shards(),
                Vec::new(),
                None,
                Some(filter),
            )?;
            Ok(stream.collect())
        };
        
        // Test include filters with mixed case
        let results = search_with_tags(&["RUST"], &[], TagMode::And)?;
        assert_eq!(results.len(), 3, "Should find all rust entries regardless of case");
        
        let results = search_with_tags(&["backend"], &[], TagMode::And)?;
        assert_eq!(results.len(), 3, "Should find all backend entries regardless of case");
        
        // Test exclude filters with mixed case - we can't easily test "all except frontend" 
        // because the filter needs include tags, so let's test a different scenario
        let results = search_with_tags(&["rust"], &["FRONTEND"], TagMode::And)?;
        assert_eq!(results.len(), 2, "Should find rust entries but exclude frontend");
        
        // Test complex filtering: include rust AND backend, exclude frontend
        let results = search_with_tags(&["rust", "BACKEND"], &["frontend"], TagMode::And)?;
        assert_eq!(results.len(), 2, "Should find entry1 and entry3 (both have rust AND backend, neither has frontend)");
        
        // Test OR mode with mixed case
        let results = search_with_tags(&["Python", "JAVASCRIPT"], &[], TagMode::Or)?;
        assert_eq!(results.len(), 2, "Should find entries with either Python or JavaScript");
    }
    
    Ok(())
}

#[test]
fn test_duplicate_tags_different_cases() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_db.idx");
    
    // Build database with tag index
    {
        let mut builder = DatabaseBuilder::<Value>::new(&db_path, PAYLOAD_STORE_VERSION_V3)?;
        builder.insert("entry1", Some(json!({"tags": ["Rust", "RUST", "rust", "RuSt"]})));
        builder.close()?;
        
        let mut db = Database::<Value>::open(&db_path)?;
        TagIndexBuilder::build_index(&mut db, "tags", None)?;
    }
    
    {
        let mut db = Database::<Value>::new();
        db.add_shard(&db_path)?;
        db.load_tag_index()?;
        
        let shard = &db.shards()[0];
        let tag_index = shard.tag_index().unwrap();
        
        // Should only have one entry in the bitmap for "rust" (lowercase)
        let bitmap = tag_index.get("rust")?.unwrap();
        assert_eq!(bitmap.len(), 1, "Should have exactly one entry for rust tag");
        
        // Original case should be the first one encountered
        let original = tag_index.get_original_case("rust")?.unwrap();
        assert_eq!(original, "Rust", "Should preserve first encountered case");
        
        // Search should work with any case variation
        let config = TagFilterConfig {
            include_tags: vec!["RUST".to_string()],
            exclude_tags: vec![],
            mode: TagMode::And,
        };
        let filter = TagFilterBitmap::new(&db.shards(), &config.include_tags, &config.exclude_tags, config.mode)?;
        let stream = MultiShardListStreamer::new_with_filter(
            &db.shards(),
            Vec::new(),
            None,
            Some(filter),
        )?;
        let results: Vec<Entry<Value>> = stream.collect();
        assert_eq!(results.len(), 1, "Should find the entry with any case variation");
    }
    
    Ok(())
}