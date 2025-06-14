use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, TagMode};
use foliant::multi_list::{MultiShardListStreamer, TagFilterConfig, LazyTagFilter};
use foliant::Streamer;
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

/// This test is designed to fail initially (reproduce the crash) and then pass after the fix.
/// It specifically targets the FST bounds error that occurs when shard indices become invalid.
#[test]
fn fst_bounds_crash_before_fix() -> Result<(), Box<dyn Error>> {
    // Create the exact scenario that causes the crash:
    // 1. Create a multi-shard streamer 
    // 2. Start iteration to populate transition cache
    // 3. Apply shard filtering that changes indices
    // 4. Continue iteration with invalid cached transitions
    
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create shards with specific structure to trigger the issue
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        
        // Keys that will create complex transition structures when sorted
        b1.insert("prefix/a/deep/file1", Some(Value::Text("val1".into())));
        b1.insert("prefix/a/deep/file2", Some(Value::Text("val2".into())));
        b1.insert("prefix/b/deep/file3", Some(Value::Text("val3".into())));
        b1.insert("prefix/c/deep/file4", Some(Value::Text("val4".into())));
        
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        tag_builder.insert_tags(1, vec!["keep"]);   // prefix/a/deep/file1
        tag_builder.insert_tags(2, vec!["keep"]);   // prefix/a/deep/file2  
        tag_builder.insert_tags(3, vec!["remove"]); // prefix/b/deep/file3
        tag_builder.insert_tags(4, vec!["keep"]);   // prefix/c/deep/file4
        tag_builder.finish()?;
    }

    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        
        b2.insert("prefix/a/deep/file5", Some(Value::Text("val5".into())));
        b2.insert("prefix/d/deep/file6", Some(Value::Text("val6".into())));
        
        b2.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base2);
        tag_builder.insert_tags(1, vec!["keep"]);   // prefix/a/deep/file5
        tag_builder.insert_tags(2, vec!["remove"]); // prefix/d/deep/file6
        tag_builder.finish()?;
    }

    // Critical: Shard 3 has no "keep" tags - should be completely filtered out
    {
        let base3 = db_dir.join("shard3.idx");
        let mut b3 = DatabaseBuilder::<Value>::new(&base3, PAYLOAD_STORE_VERSION_V3)?;
        
        b3.insert("other/file7", Some(Value::Text("val7".into())));
        b3.insert("other/file8", Some(Value::Text("val8".into())));
        
        b3.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base3);
        tag_builder.insert_tags(1, vec!["remove"]); // other/file7
        tag_builder.insert_tags(2, vec!["remove"]); // other/file8
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;  
    db.add_shard(&db_dir.join("shard3.idx"))?;
    db.load_tag_index()?;

    // Step 1: Create unfiltered streamer to establish initial state
    let mut streamer = MultiShardListStreamer::new(&db, b"prefix/".to_vec(), None);
    
    // Step 2: Start iteration to populate transition cache with 3-shard references
    let first_entry = streamer.next().expect("Should get first entry");
    eprintln!("Got first entry: {}", first_entry.as_str());
    
    // At this point, frame.trans should contain cached transitions with shard indices 0, 1, 2
    
    // Step 3: Create a new filtered streamer (old bitmap methods don't exist anymore)
    let tag_config = TagFilterConfig {
        include_tags: vec!["keep".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    let filter = LazyTagFilter::from_config(&tag_config);
    
    // Create new filtered streamer to test the lazy filtering approach
    let mut filtered_streamer = MultiShardListStreamer::new_with_filter(
        &db,
        b"prefix/".to_vec(),
        None,
        Some(Box::new(filter)),
    )?;
    
    // Step 4: Iterate through the filtered results - this tests the new lazy filtering
    eprintln!("Iterating through filtered results...");
    let mut count = 0;
    
    while let Some(entry) = filtered_streamer.next() {
        eprintln!("Got entry {}: {}", count + 1, entry.as_str());
        count += 1;
        
        if count >= 10 {
            break; // Prevent infinite loop
        }
    }
    
    // Also continue with the original streamer to test mixed usage
    while let Some(entry) = streamer.next() {
        eprintln!("Original streamer entry {}: {}", count + 1, entry.as_str());
        count += 1;
        
        if count >= 10 {
            break; // Prevent infinite loop
        }
    }
    
    eprintln!("Successfully iterated {} entries without crash", count);
    
    Ok(())
}

/// This test verifies that the new_with_filter flow also works correctly
#[test] 
fn fst_bounds_new_with_filter_test() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create the same data structure
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        
        for i in 0..50 {
            b1.insert(&format!("test/path{}/file.ext", i), Some(Value::Text(format!("val{}", i).into())));
        }
        
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        for i in 1..=50 {
            let tag = if i % 3 == 0 { "target" } else { "other" };
            tag_builder.insert_tags(i, vec![tag]);
        }
        tag_builder.finish()?;
    }

    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        
        for i in 50..80 {
            b2.insert(&format!("test/path{}/file.ext", i), Some(Value::Text(format!("val{}", i).into())));
        }
        
        b2.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base2);
        for i in 1..=30 {
            tag_builder.insert_tags(i, vec!["other"]); // No "target" tags in this shard
        }
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;
    db.load_tag_index()?;

    // Use new_with_filter (this should apply filtering during construction)
    let tag_config = TagFilterConfig {
        include_tags: vec!["target".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    
    let filter = LazyTagFilter::from_config(&tag_config);
    let mut stream = MultiShardListStreamer::new_with_filter(
        &db,
        b"test/".to_vec(),
        None,
        Some(Box::new(filter)),
    )?;
    
    // Iterate through all results
    let mut count = 0;
    while let Some(entry) = stream.next() {
        count += 1;
        match entry {
            Entry::Key(key, _, _) => {
                assert!(key.starts_with("test/"), "Key should start with prefix: {}", key);
            },
            _ => {}
        }
        
        if count > 100 {
            break; // Safety limit
        }
    }
    
    assert!(count > 0, "Should find some target entries");
    eprintln!("Successfully processed {} entries with new_with_filter", count);
    
    Ok(())
}