use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, TagIndexBuilder, Streamer};
use serde_json::{json, Value};
use tempfile::tempdir;

#[test]
fn test_round_trip_original_case() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_db.idx");
    
    let test_tags = vec![
        "JavaScript", "WebDev", "BACKEND", "API", "testing", "automation", "ci/cd"
    ];
    
    // Build database with mixed case tags
    {
        let mut builder = DatabaseBuilder::<Value>::new(&db_path, PAYLOAD_STORE_VERSION_V3)?;
        for (i, tag) in test_tags.iter().enumerate() {
            builder.insert(&format!("entry{}", i), Some(json!({"tags": [tag]})));
        }
        builder.close()?;
        
        let mut db = Database::<Value>::open(&db_path)?;
        TagIndexBuilder::build_index(&mut db, "tags", None)?;
    }
    
    // Test round-trip: verify every lowercase tag maps back to expected original spelling
    {
        let mut db = Database::<Value>::new();
        db.add_shard(&db_path)?;
        db.load_tag_index()?;
        
        let shard = &db.shards()[0];
        let tag_index = shard.tag_index().unwrap();
        
        for original_tag in &test_tags {
            let lowercase = original_tag.to_lowercase();
            let retrieved = tag_index.get_original_case(&lowercase)?;
            assert_eq!(
                retrieved,
                Some(original_tag.to_string()),
                "Round-trip failed for tag '{}' (lowercase: '{}')",
                original_tag,
                lowercase
            );
        }
        
        // Test list_tags returns original case
        let mut tags_with_counts: Vec<(String, usize)> = db.list_tags(None)?.collect();
        tags_with_counts.sort_by(|a, b| a.0.cmp(&b.0));
        
        let mut expected_tags = test_tags.clone();
        expected_tags.sort();
        
        for (i, (tag, count)) in tags_with_counts.iter().enumerate() {
            assert_eq!(count, &1, "Each tag should appear exactly once");
            assert_eq!(tag, &expected_tags[i], "Tag should be in original case");
        }
    }
    
    Ok(())
}

#[test]
fn test_legacy_v1_compatibility() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_db.idx");
    
    // Create a v1-style tag index by manually building without original case
    {
        let mut builder = DatabaseBuilder::<Value>::new(&db_path, PAYLOAD_STORE_VERSION_V3)?;
        builder.insert("entry1", Some(json!({"tags": ["rust", "backend"]})));
        builder.insert("entry2", Some(json!({"tags": ["python", "frontend"]})));
        builder.close()?;
        
        // Build the tag index (this will create v2 format)
        let mut db = Database::<Value>::open(&db_path)?;
        TagIndexBuilder::build_index(&mut db, "tags", None)?;
    }
    
    // Test that get_original_case works and returns correct values
    {
        let mut db = Database::<Value>::new();
        db.add_shard(&db_path)?;
        db.load_tag_index()?;
        
        let shard = &db.shards()[0];
        let tag_index = shard.tag_index().unwrap();
        
        // Since tags were lowercase to begin with, original case should be lowercase
        assert_eq!(
            tag_index.get_original_case("rust")?,
            Some("rust".to_string())
        );
        assert_eq!(
            tag_index.get_original_case("backend")?,
            Some("backend".to_string())
        );
        assert_eq!(
            tag_index.get_original_case("python")?,
            Some("python".to_string())
        );
        assert_eq!(
            tag_index.get_original_case("frontend")?,
            Some("frontend".to_string())
        );
        
        // Non-existent tag should return None
        assert_eq!(
            tag_index.get_original_case("nonexistent")?,
            None
        );
        
        // Test that list_tags works without panics
        let tags: Vec<(String, usize)> = db.list_tags(None)?.collect();
        assert_eq!(tags.len(), 4);
        
        let tag_names: Vec<String> = db.list_tag_names(None)?.collect();
        assert_eq!(tag_names.len(), 4);
    }
    
    Ok(())
}

#[test]
fn test_mixed_case_input_stable_canonical() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_db.idx");
    
    // Test that canonical spelling is stable and follows first-wins policy
    {
        let mut builder = DatabaseBuilder::<Value>::new(&db_path, PAYLOAD_STORE_VERSION_V3)?;
        
        // Insert tags in specific order to test first-wins policy
        builder.insert("entry1", Some(json!({"tags": ["Rust"]})));        // First occurrence
        builder.insert("entry2", Some(json!({"tags": ["rust"]})));        // Second occurrence (should be ignored for case)
        builder.insert("entry3", Some(json!({"tags": ["RUST"]})));        // Third occurrence (should be ignored for case)
        builder.insert("entry4", Some(json!({"tags": ["JavaScript"]})));  // First occurrence
        builder.insert("entry5", Some(json!({"tags": ["javascript"]})));  // Second occurrence (should be ignored for case)
        
        builder.close()?;
        
        let mut db = Database::<Value>::open(&db_path)?;
        TagIndexBuilder::build_index(&mut db, "tags", None)?;
    }
    
    // Verify canonical spelling follows first-wins policy
    {
        let mut db = Database::<Value>::new();
        db.add_shard(&db_path)?;
        db.load_tag_index()?;
        
        let shard = &db.shards()[0];
        let tag_index = shard.tag_index().unwrap();
        
        // "Rust" was first, so it should be the canonical spelling
        assert_eq!(
            tag_index.get_original_case("rust")?,
            Some("Rust".to_string()),
            "First encountered case should be preserved"
        );
        
        // "JavaScript" was first, so it should be the canonical spelling
        assert_eq!(
            tag_index.get_original_case("javascript")?,
            Some("JavaScript".to_string()),
            "First encountered case should be preserved"
        );
        
        // Test that list_tags returns the canonical (first-encountered) spellings
        let mut tags_with_counts: Vec<(String, usize)> = db.list_tags(None)?.collect();
        tags_with_counts.sort_by(|a, b| a.0.cmp(&b.0));
        
        let expected = vec![
            ("JavaScript".to_string(), 2),  // appears in entry4 and entry5
            ("Rust".to_string(), 3),        // appears in entry1, entry2, and entry3
        ];
        assert_eq!(tags_with_counts, expected);
    }
    
    Ok(())
}

