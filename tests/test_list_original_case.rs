use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, TagIndexBuilder, Streamer};
use serde_json::{json, Value};
use tempfile::tempdir;

#[test]
fn test_list_tags_returns_original_case() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_db.idx");
    
    // Build database with mixed case tags
    {
        let mut builder = DatabaseBuilder::<Value>::new(&db_path, PAYLOAD_STORE_VERSION_V3)?;
        builder.insert("entry1", Some(json!({"tags": ["JavaScript", "WebDev"]})));
        builder.insert("entry2", Some(json!({"tags": ["BACKEND", "API"]})));
        builder.insert("entry3", Some(json!({"tags": ["testing", "automation"]})));
        builder.close()?;
        
        let mut db = Database::<Value>::open(&db_path)?;
        TagIndexBuilder::build_index(&mut db, "tags", None)?;
    }
    
    // Test that list_tags returns original case
    {
        let db = Database::<Value>::open(&db_path)?;
        
        let mut tags_with_counts: Vec<(String, usize)> = db.list_tags(None)?.collect();
        tags_with_counts.sort_by(|a, b| a.0.cmp(&b.0));
        
        // Should return original case, not lowercase
        let expected = vec![
            ("API".to_string(), 1),
            ("BACKEND".to_string(), 1),
            ("JavaScript".to_string(), 1),
            ("WebDev".to_string(), 1),
            ("automation".to_string(), 1),
            ("testing".to_string(), 1),
        ];
        assert_eq!(tags_with_counts, expected);
        
        // Test list_tag_names also returns original case
        let mut tag_names: Vec<String> = db.list_tag_names(None)?.collect();
        tag_names.sort();
        
        let expected_names = vec![
            "API".to_string(),
            "BACKEND".to_string(),
            "JavaScript".to_string(),
            "WebDev".to_string(),
            "automation".to_string(),
            "testing".to_string(),
        ];
        assert_eq!(tag_names, expected_names);
    }
    
    Ok(())
}

#[test]
fn test_list_tags_with_duplicate_cases() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_db.idx");
    
    // Build database with duplicate tags in different cases
    {
        let mut builder = DatabaseBuilder::<Value>::new(&db_path, PAYLOAD_STORE_VERSION_V3)?;
        builder.insert("entry1", Some(json!({"tags": ["Rust", "JavaScript"]})));
        builder.insert("entry2", Some(json!({"tags": ["RUST", "python"]})));
        builder.insert("entry3", Some(json!({"tags": ["rust", "Python"]})));
        builder.close()?;
        
        let mut db = Database::<Value>::open(&db_path)?;
        TagIndexBuilder::build_index(&mut db, "tags", None)?;
    }
    
    // Test that list_tags returns first encountered case for duplicates
    {
        let db = Database::<Value>::open(&db_path)?;
        
        let mut tags_with_counts: Vec<(String, usize)> = db.list_tags(None)?.collect();
        tags_with_counts.sort_by(|a, b| a.0.cmp(&b.0));
        
        // Should return first encountered case for each tag
        // "Rust" appears first, "JavaScript" appears first, "python" appears first
        let expected = vec![
            ("JavaScript".to_string(), 1),
            ("Rust".to_string(), 3),   // rust appears in all 3 entries in various cases
            ("python".to_string(), 2), // python appears in entry2 and entry3 (as Python)
        ];
        assert_eq!(tags_with_counts, expected);
    }
    
    Ok(())
}