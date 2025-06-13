use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, TagMode};
use foliant::multi_list::{MultiShardListStreamer, TagFilterConfig, LazyTagFilter};
use foliant::Streamer;
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

/// Test cursor resumption with active tag filtering across multiple shards.
/// This test verifies that filtering is consistently applied across cursor resumption.
#[test]
fn multi_shard_tag_filter_resume_with_filtering() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Shard 1: mix of "keep" and "exclude" tagged items
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        b1.insert("alpha/item1", Some(Value::Text("content1".into())));  // keep
        b1.insert("alpha/item2", Some(Value::Text("content2".into())));  // exclude  
        b1.insert("beta/item3", Some(Value::Text("content3".into())));   // keep
        b1.insert("beta/item4", Some(Value::Text("content4".into())));   // exclude
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        // Keys in alphabetical order: alpha/item1(1), alpha/item2(2), beta/item3(3), beta/item4(4)
        tag_builder.insert_tags(1, vec!["keep"]);
        tag_builder.insert_tags(2, vec!["exclude"]);
        tag_builder.insert_tags(3, vec!["keep"]);
        tag_builder.insert_tags(4, vec!["exclude"]);
        tag_builder.finish()?;
    }

    // Shard 2: different mix of tagged items
    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        b2.insert("gamma/item5", Some(Value::Text("content5".into())));  // exclude
        b2.insert("gamma/item6", Some(Value::Text("content6".into())));  // keep
        b2.insert("delta/item7", Some(Value::Text("content7".into())));  // keep
        b2.insert("delta/item8", Some(Value::Text("content8".into())));  // exclude
        b2.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base2);
        // Keys in alphabetical order: delta/item7(1), delta/item8(2), gamma/item5(3), gamma/item6(4)
        tag_builder.insert_tags(1, vec!["keep"]);
        tag_builder.insert_tags(2, vec!["exclude"]);
        tag_builder.insert_tags(3, vec!["exclude"]);
        tag_builder.insert_tags(4, vec!["keep"]);
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;
    db.load_tag_index()?;

    // Filter to only include items tagged with "keep"
    let tag_config = TagFilterConfig {
        include_tags: vec!["keep".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };

    // Get first 2 filtered items
    let mut stream1 = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        None,
        Some(Box::new(LazyTagFilter::from_config(&tag_config))),
    )?;
    
    let item1 = stream1.next().unwrap();
    let item2 = stream1.next().unwrap(); 
    let cursor = stream1.cursor();
    
    // Expected filtered items (only "keep" tagged items): alpha/item1, beta/item3, delta/item7, gamma/item6
    assert_eq!(item1.as_str(), "alpha/item1");
    assert_eq!(item2.as_str(), "beta/item3");

    // Resume from cursor with the same filter (create a new instance)
    let mut stream2 = MultiShardListStreamer::resume_with_filter(
        &db.shards(),
        Vec::new(),
        None,
        cursor,
        Some(Box::new(LazyTagFilter::from_config(&tag_config))),
    )?;
    
    let item3 = stream2.next().unwrap();
    let item4 = stream2.next().unwrap();
    let no_more = stream2.next();
    
    // Should get remaining filtered items
    assert_eq!(item3.as_str(), "delta/item7");
    assert_eq!(item4.as_str(), "gamma/item6");
    assert!(no_more.is_none(), "Should be no more items after resumption");

    Ok(())
}

/// Test cursor resumption with prefix restriction and filtering across multiple shards
#[test]
fn multi_shard_tag_filter_resume_with_prefix() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Shard 1
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        b1.insert("docs/alpha/file1", Some(Value::Text("content1".into())));
        b1.insert("docs/alpha/file2", Some(Value::Text("content2".into())));
        b1.insert("docs/beta/file3", Some(Value::Text("content3".into())));
        b1.insert("other/file4", Some(Value::Text("content4".into())));
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        // In alphabetical order: docs/alpha/file1(1), docs/alpha/file2(2), docs/beta/file3(3), other/file4(4)
        tag_builder.insert_tags(1, vec!["important"]);
        tag_builder.insert_tags(2, vec!["draft"]);
        tag_builder.insert_tags(3, vec!["important"]);
        tag_builder.insert_tags(4, vec!["important"]);
        tag_builder.finish()?;
    }

    // Shard 2
    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        b2.insert("docs/alpha/file5", Some(Value::Text("content5".into())));
        b2.insert("docs/gamma/file6", Some(Value::Text("content6".into())));
        b2.insert("other/file7", Some(Value::Text("content7".into())));
        b2.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base2);
        // In alphabetical order: docs/alpha/file5(1), docs/gamma/file6(2), other/file7(3)
        tag_builder.insert_tags(1, vec!["draft"]);
        tag_builder.insert_tags(2, vec!["important"]);
        tag_builder.insert_tags(3, vec!["draft"]);
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;
    db.load_tag_index()?;

    // Filter for "important" items under "docs/" prefix
    let tag_config = TagFilterConfig {
        include_tags: vec!["important".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };

    // Get first item from docs/ with important tag
    let mut stream1 = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        "docs/".as_bytes().to_vec(),
        None,
        Some(Box::new(LazyTagFilter::from_config(&tag_config))),
    )?;
    
    let item1 = stream1.next().unwrap();
    let cursor = stream1.cursor();
    
    // Should get docs/alpha/file1 (important)
    assert_eq!(item1.as_str(), "docs/alpha/file1");

    // Resume from cursor with same filter and prefix (create new instance)
    let mut stream2 = MultiShardListStreamer::resume_with_filter(
        &db.shards(),
        "docs/".as_bytes().to_vec(),
        None,
        cursor,
        Some(Box::new(LazyTagFilter::from_config(&tag_config))),
    )?;
    
    let item2 = stream2.next().unwrap();
    let item3 = stream2.next().unwrap();
    let no_more = stream2.next();
    
    // Should get remaining important items in docs/: docs/beta/file3, docs/gamma/file6
    assert_eq!(item2.as_str(), "docs/beta/file3");
    assert_eq!(item3.as_str(), "docs/gamma/file6");
    assert!(no_more.is_none(), "Should be no more items");

    Ok(())
}

/// Test cursor resumption with delimiter grouping and filtering across multiple shards
#[test]
fn multi_shard_tag_filter_resume_with_delimiter() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Shard 1
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        b1.insert("alpha/important1", Some(Value::Text("content1".into())));
        b1.insert("alpha/draft1", Some(Value::Text("content2".into())));
        b1.insert("beta/important2", Some(Value::Text("content3".into())));
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        // In alphabetical order: alpha/draft1(1), alpha/important1(2), beta/important2(3)
        tag_builder.insert_tags(1, vec!["draft"]);
        tag_builder.insert_tags(2, vec!["important"]);
        tag_builder.insert_tags(3, vec!["important"]);
        tag_builder.finish()?;
    }

    // Shard 2
    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        b2.insert("gamma/important3", Some(Value::Text("content4".into())));
        b2.insert("gamma/draft2", Some(Value::Text("content5".into())));
        b2.insert("delta/draft3", Some(Value::Text("content6".into())));
        b2.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base2);
        // In alphabetical order: delta/draft3(1), gamma/draft2(2), gamma/important3(3)
        tag_builder.insert_tags(1, vec!["draft"]);
        tag_builder.insert_tags(2, vec!["draft"]);
        tag_builder.insert_tags(3, vec!["important"]);
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;
    db.load_tag_index()?;

    // Filter for "important" items with delimiter grouping
    let tag_config = TagFilterConfig {
        include_tags: vec!["important".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };

    // Get first prefix group with important items
    let mut stream1 = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        Some(b'/'),
        Some(Box::new(LazyTagFilter::from_config(&tag_config))),
    )?;
    
    let entry1 = stream1.next().unwrap();
    let cursor = stream1.cursor();
    
    // Should get alpha/ prefix (contains important items)
    match entry1 {
        Entry::CommonPrefix(prefix, count) => {
            assert_eq!(prefix, "alpha/");
            assert!(count.is_some() && count.unwrap() > 0);
        },
        _ => panic!("Expected CommonPrefix for alpha/"),
    }

    // Resume from cursor with same filter (create new instance)
    let mut stream2 = MultiShardListStreamer::resume_with_filter(
        &db.shards(),
        Vec::new(),
        Some(b'/'),
        cursor,
        Some(Box::new(LazyTagFilter::from_config(&tag_config))),
    )?;
    
    let entry2 = stream2.next().unwrap();
    let entry3 = stream2.next().unwrap();
    let no_more = stream2.next();
    
    // Should get remaining prefixes with important items: beta/, gamma/
    match entry2 {
        Entry::CommonPrefix(prefix, count) => {
            assert_eq!(prefix, "beta/");
            assert!(count.is_some() && count.unwrap() > 0);
        },
        _ => panic!("Expected CommonPrefix for beta/"),
    }
    
    match entry3 {
        Entry::CommonPrefix(prefix, count) => {
            assert_eq!(prefix, "gamma/");
            assert!(count.is_some() && count.unwrap() > 0);
        },
        _ => panic!("Expected CommonPrefix for gamma/"),
    }
    
    assert!(no_more.is_none(), "Should be no more prefixes");

    Ok(())
}

/// Test cursor resumption with exclusion filters across multiple shards
#[test]
fn multi_shard_tag_filter_resume_with_exclusion() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Shard 1
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        b1.insert("doc1", Some(Value::Text("content1".into())));
        b1.insert("doc2", Some(Value::Text("content2".into())));
        b1.insert("doc3", Some(Value::Text("content3".into())));
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        tag_builder.insert_tags(1, vec!["published", "important"]);
        tag_builder.insert_tags(2, vec!["published", "deprecated"]);
        tag_builder.insert_tags(3, vec!["published"]);
        tag_builder.finish()?;
    }

    // Shard 2
    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        b2.insert("doc4", Some(Value::Text("content4".into())));
        b2.insert("doc5", Some(Value::Text("content5".into())));
        b2.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base2);
        tag_builder.insert_tags(1, vec!["published"]);
        tag_builder.insert_tags(2, vec!["published", "deprecated"]);
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;
    db.load_tag_index()?;

    // Filter for published items, excluding deprecated ones
    let tag_config = TagFilterConfig {
        include_tags: vec!["published".to_string()],
        exclude_tags: vec!["deprecated".to_string()],
        mode: TagMode::And,
    };

    // Get first 2 non-deprecated published items
    let mut stream1 = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        None,
        Some(Box::new(LazyTagFilter::from_config(&tag_config))),
    )?;
    
    let item1 = stream1.next().unwrap();
    let item2 = stream1.next().unwrap();
    let cursor = stream1.cursor();
    
    // Should get doc1 (published+important), doc3 (published only)
    assert_eq!(item1.as_str(), "doc1");
    assert_eq!(item2.as_str(), "doc3");

    // Resume from cursor with same exclusion filter (create new instance)
    let mut stream2 = MultiShardListStreamer::resume_with_filter(
        &db.shards(),
        Vec::new(),
        None,
        cursor,
        Some(Box::new(LazyTagFilter::from_config(&tag_config))),
    )?;
    
    let item3 = stream2.next().unwrap();
    let no_more = stream2.next();
    
    // Should get doc4 (published only), but NOT doc2 or doc5 (both deprecated)
    assert_eq!(item3.as_str(), "doc4");
    assert!(no_more.is_none(), "Should be no more items - doc2 and doc5 should be excluded");

    Ok(())
}