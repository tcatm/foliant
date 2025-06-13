use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, TagMode};
use foliant::multi_list::{LazyTagFilter, TagFilterConfig, LazyShardFilter};
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

/// Test that demonstrates the early-return optimization in LazyTagFilter.
/// This test verifies that the filter returns None early for cases that would
/// result in empty bitmaps, avoiding unnecessary bitmap construction and cloning.
#[test]
fn test_lazy_filter_early_return_optimization() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create a shard with specific tag structure
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        
        // Add some entries
        b1.insert("item1", Some(Value::Text("content1".into())));
        b1.insert("item2", Some(Value::Text("content2".into())));
        b1.insert("item3", Some(Value::Text("content3".into())));
        
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        // Only item1 has 'important' tag, others have 'normal'
        tag_builder.insert_tags(1, vec!["important"]);  // item1
        tag_builder.insert_tags(2, vec!["normal"]);     // item2  
        tag_builder.insert_tags(3, vec!["normal"]);     // item3
        tag_builder.finish()?;
    }

    // Create a shard with NO tag index at all
    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        
        b2.insert("item4", Some(Value::Text("content4".into())));
        b2.insert("item5", Some(Value::Text("content5".into())));
        
        b2.close()?;
        // Note: No tag index created for this shard
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;
    db.load_tag_index()?;

    // Test case 1: Search for non-existent tag should return None early
    let config1 = TagFilterConfig {
        include_tags: vec!["nonexistent".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    let filter1 = LazyTagFilter::from_config(&config1);
    
    // This should return None for shard1 (tag not found in AND mode)
    let result1 = filter1.compute_bitmap(&db.shards()[0])?;
    assert!(result1.is_none(), "Should return None for non-existent tag in AND mode");
    
    // This should return None for shard2 (no tag index + include filters)
    let result2 = filter1.compute_bitmap(&db.shards()[1])?;
    assert!(result2.is_none(), "Should return None for shard without tag index when include filters exist");

    // Test case 2: Search for existing tag should work normally
    let config2 = TagFilterConfig {
        include_tags: vec!["important".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    let filter2 = LazyTagFilter::from_config(&config2);
    
    // This should return Some bitmap for shard1
    let result3 = filter2.compute_bitmap(&db.shards()[0])?;
    assert!(result3.is_some(), "Should return bitmap for existing tag");
    assert_eq!(result3.unwrap().len(), 1, "Should have exactly one item tagged 'important'");
    
    // This should return None for shard2 (no tag index)
    let result4 = filter2.compute_bitmap(&db.shards()[1])?;
    assert!(result4.is_none(), "Should return None for shard without tag index");

    // Test case 3: AND mode with multiple tags where one doesn't exist
    let config3 = TagFilterConfig {
        include_tags: vec!["important".to_string(), "nonexistent".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    let filter3 = LazyTagFilter::from_config(&config3);
    
    // This should return None early when 'nonexistent' tag is not found
    let result5 = filter3.compute_bitmap(&db.shards()[0])?;
    assert!(result5.is_none(), "Should return None early when any required tag is missing in AND mode");

    // Test case 4: Empty shard should return None
    {
        let base_empty = db_dir.join("empty_shard.idx");
        let b_empty = DatabaseBuilder::<Value>::new(&base_empty, PAYLOAD_STORE_VERSION_V3)?;
        b_empty.close()?; // Empty shard
        
        let mut db_with_empty = Database::<Value>::new();
        db_with_empty.add_shard(&base_empty)?;
        db_with_empty.load_tag_index()?;
        
        let config4 = TagFilterConfig {
            include_tags: vec![], // No include filters = accept all
            exclude_tags: vec![],
            mode: TagMode::And,
        };
        let filter4 = LazyTagFilter::from_config(&config4);
        
        let result6 = filter4.compute_bitmap(&db_with_empty.shards()[0])?;
        assert!(result6.is_none(), "Should return None for empty shard even with no filters");
    }

    println!("✓ All early-return optimization tests passed!");
    Ok(())
}