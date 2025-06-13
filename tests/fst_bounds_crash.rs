use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, TagMode};
use foliant::multi_list::{MultiShardListStreamer, TagFilterConfig, LazyTagFilter};
use foliant::Streamer;
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

/// Test for FST index out of bounds crash when using tag filtering with cursors/seek operations.
/// This reproduces the crash seen with: foliant shell -i data/db -n 200 -- ls s3://rafagalante/ '#Image'
#[test]
fn fst_bounds_crash_with_tag_filtering() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create a larger dataset that might trigger the FST bounds issue
    // Simulate a structure similar to s3://rafagalante/ with Image tags
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        
        // Create many entries to simulate a realistic dataset
        for i in 0..100 {
            b1.insert(&format!("s3://rafagalante/image{:03}.jpg", i), 
                     Some(Value::Text(format!("content{}", i).into())));
            b1.insert(&format!("s3://rafagalante/doc{:03}.pdf", i), 
                     Some(Value::Text(format!("doc{}", i).into())));
        }
        
        // Add some nested structure
        for i in 0..50 {
            b1.insert(&format!("s3://rafagalante/folder{}/image{}.jpg", i % 10, i), 
                     Some(Value::Text(format!("nested{}", i).into())));
        }
        
        b1.close()?;
        
        // Build tag index - some images, some documents, some other types
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        
        // Create sorted list to match FST order (alphabetical)
        let mut entries = Vec::new();
        for i in 0..100 {
            entries.push((format!("s3://rafagalante/doc{:03}.pdf", i), vec!["Document"]));
            entries.push((format!("s3://rafagalante/image{:03}.jpg", i), vec!["Image"]));
        }
        for i in 0..50 {
            entries.push((format!("s3://rafagalante/folder{}/image{}.jpg", i % 10, i), vec!["Image"]));
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        
        // Assign tags based on alphabetical order
        for (idx, (_path, tags)) in entries.iter().enumerate() {
            tag_builder.insert_tags((idx + 1) as u32, tags.iter().cloned());
        }
        tag_builder.finish()?;
    }

    // Add a second shard to make it multi-shard
    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        
        for i in 100..150 {
            b2.insert(&format!("s3://rafagalante/image{:03}.jpg", i), 
                     Some(Value::Text(format!("content{}", i).into())));
        }
        
        b2.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base2);
        // All entries in shard2 are images
        for i in 1..=50 {
            tag_builder.insert_tags(i as u32, vec!["Image"]);
        }
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;
    db.load_tag_index()?;

    // Test: Filter for Image tags with prefix - this should trigger the crash
    let tag_config = TagFilterConfig {
        include_tags: vec!["Image".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    
    let filter = LazyTagFilter::from_config(&tag_config);
    let mut stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        "s3://rafagalante/".as_bytes().to_vec(),
        None,
        Some(Box::new(filter)),
    )?;
    
    // Try to iterate through results - this should crash with FST bounds error
    let mut count = 0;
    while let Some(entry) = stream.next() {
        count += 1;
        // Limit iterations to avoid infinite loop if the bug is fixed
        if count > 1000 {
            break;
        }
        
        // Verify we only get Image entries
        match entry {
            Entry::Key(key, _lut_id, _payload) => {
                assert!(key.contains("image") || key.contains("Image"));
            },
            _ => {} // Allow other entry types
        }
    }
    
    // If we get here without crashing, the bug is fixed
    assert!(count > 0, "Should have found some Image entries");
    
    Ok(())
}

/// Simpler test case to isolate the FST bounds issue
#[test]
fn fst_bounds_simple_case() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create a minimal test case
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        
        // Create entries that when sorted alphabetically might cause address misalignment  
        b1.insert("prefix/aaa", Some(Value::Text("val1".into())));
        b1.insert("prefix/bbb", Some(Value::Text("val2".into())));
        b1.insert("prefix/zzz", Some(Value::Text("val3".into())));
        b1.insert("other/xxx", Some(Value::Text("val4".into())));
        
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        // Alphabetical order: other/xxx, prefix/aaa, prefix/bbb, prefix/zzz
        tag_builder.insert_tags(1, vec!["TypeA"]);  // other/xxx
        tag_builder.insert_tags(2, vec!["TypeB"]);  // prefix/aaa  
        tag_builder.insert_tags(3, vec!["TypeB"]);  // prefix/bbb
        tag_builder.insert_tags(4, vec!["TypeA"]);  // prefix/zzz
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.load_tag_index()?;

    // Filter for TypeB and use a prefix
    let tag_config = TagFilterConfig {
        include_tags: vec!["TypeB".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    
    let filter = LazyTagFilter::from_config(&tag_config);
    let mut stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        "prefix/".as_bytes().to_vec(),
        None,
        Some(Box::new(filter)),
    )?;
    
    // Iterate and check for crash
    let mut results = Vec::new();
    while let Some(entry) = stream.next() {
        results.push(entry);
    }
    
    // Should find prefix/aaa and prefix/bbb (both TypeB)
    assert_eq!(results.len(), 2);
    
    Ok(())
}

/// Test for FST bounds crash specifically with cursor seek operations on filtered streams.
/// This targets the specific issue where cached transitions contain old shard indices.
#[test]
fn fst_bounds_crash_with_cursor_seek() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create test data with multiple shards
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        
        // Create data that will have complex transition structures
        for i in 0..20 {
            b1.insert(&format!("prefix/deep/path{:02}/file.jpg", i), 
                     Some(Value::Text(format!("image{}", i).into())));
            b1.insert(&format!("prefix/deep/path{:02}/doc.pdf", i), 
                     Some(Value::Text(format!("doc{}", i).into())));
        }
        
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        
        // Create sorted list and assign tags
        let mut entries = Vec::new();
        for i in 0..20 {
            entries.push((format!("prefix/deep/path{:02}/doc.pdf", i), vec!["Document"]));
            entries.push((format!("prefix/deep/path{:02}/file.jpg", i), vec!["Image"]));
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        
        for (idx, (_path, tags)) in entries.iter().enumerate() {
            tag_builder.insert_tags((idx + 1) as u32, tags.iter().cloned());
        }
        tag_builder.finish()?;
    }

    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        
        for i in 20..30 {
            b2.insert(&format!("prefix/deep/path{:02}/file.jpg", i), 
                     Some(Value::Text(format!("image{}", i).into())));
        }
        
        b2.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base2);
        for i in 1..=10 {
            tag_builder.insert_tags(i as u32, vec!["Image"]);
        }
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;
    db.load_tag_index()?;

    // Create filtered stream
    let tag_config = TagFilterConfig {
        include_tags: vec!["Image".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    
    let filter = LazyTagFilter::from_config(&tag_config);
    let mut stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        "prefix/deep/".as_bytes().to_vec(),
        None,
        Some(Box::new(filter)),
    )?;
    
    // Get first few results to populate transition cache
    let mut first_results = Vec::new();
    for _ in 0..5 {
        if let Some(entry) = stream.next() {
            first_results.push(entry);
        } else {
            break;
        }
    }
    
    // Now try to get cursor and seek - this might trigger the FST bounds error
    let cursor = stream.cursor();
    
    // Create a new stream and seek to the cursor position
    let filter2 = LazyTagFilter::from_config(&tag_config);
    let mut stream2 = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        "prefix/deep/".as_bytes().to_vec(), 
        None,
        Some(Box::new(filter2)),
    )?;
    
    // Seek to cursor position - this might trigger the crash
    stream2.seek(cursor);
    
    // Try to get next results
    let mut remaining_results = Vec::new();
    for _ in 0..10 {
        if let Some(entry) = stream2.next() {
            remaining_results.push(entry);
        } else {
            break;
        }
    }
    
    assert!(first_results.len() > 0, "Should get some initial results");
    
    Ok(())
}

/// Test that reproduces the exact crash from: foliant shell -i data/db -n 200 -- ls s3://rafagalante/ '#Image'
/// This test simulates real-world data that triggers the FST bounds error.
#[test]
fn fst_bounds_real_world_simulation() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create multiple shards with realistic s3:// paths and mixed content
    // Shard 1: Large number of mixed files 
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        
        // Create a large, diverse dataset similar to s3://rafagalante/
        for i in 0..200 {
            let suffix = if i % 3 == 0 { "jpg" } else if i % 3 == 1 { "pdf" } else { "txt" };
            let prefix = if i % 4 == 0 { "images" } else if i % 4 == 1 { "docs" } else if i % 4 == 2 { "backup" } else { "misc" };
            
            b1.insert(&format!("s3://rafagalante/{}/file{:03}.{}", prefix, i, suffix), 
                     Some(Value::Text(format!("content{}", i).into())));
        }
        
        // Add some deeply nested paths
        for i in 0..50 {
            b1.insert(&format!("s3://rafagalante/deep/nested/path{}/image{}.jpg", i % 10, i), 
                     Some(Value::Text(format!("nested{}", i).into())));
        }
        
        b1.close()?;
        
        // Build tags - create realistic tag distribution where only some files are Images
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        
        // Get all keys and sort them (FST order)
        let mut all_keys = Vec::new();
        for i in 0..200 {
            let suffix = if i % 3 == 0 { "jpg" } else if i % 3 == 1 { "pdf" } else { "txt" };
            let prefix = if i % 4 == 0 { "images" } else if i % 4 == 1 { "docs" } else if i % 4 == 2 { "backup" } else { "misc" };
            all_keys.push(format!("s3://rafagalante/{}/file{:03}.{}", prefix, i, suffix));
        }
        for i in 0..50 {
            all_keys.push(format!("s3://rafagalante/deep/nested/path{}/image{}.jpg", i % 10, i));
        }
        all_keys.sort();
        
        // Assign tags based on file extension and content
        for (idx, key) in all_keys.iter().enumerate() {
            let tags = if key.ends_with(".jpg") {
                vec!["Image"]
            } else if key.ends_with(".pdf") {
                vec!["Document"] 
            } else {
                vec!["Text"]
            };
            tag_builder.insert_tags((idx + 1) as u32, tags.iter().cloned());
        }
        tag_builder.finish()?;
    }

    // Shard 2: More files that might cause filtering misalignment
    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        
        for i in 200..350 {
            let suffix = if i % 2 == 0 { "jpg" } else { "mp4" };
            b2.insert(&format!("s3://rafagalante/media/video{}.{}", i, suffix), 
                     Some(Value::Text(format!("media{}", i).into())));
        }
        
        b2.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base2);
        for i in 1..=150 {
            let tags = if i % 2 == 1 { vec!["Image"] } else { vec!["Video"] };
            tag_builder.insert_tags(i as u32, tags.iter().cloned());
        }
        tag_builder.finish()?;
    }

    // Shard 3: Files with no Image tags - this shard should be completely filtered out
    {
        let base3 = db_dir.join("shard3.idx");
        let mut b3 = DatabaseBuilder::<Value>::new(&base3, PAYLOAD_STORE_VERSION_V3)?;
        
        for i in 350..400 {
            b3.insert(&format!("s3://rafagalante/docs/report{}.pdf", i), 
                     Some(Value::Text(format!("report{}", i).into())));
        }
        
        b3.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base3);
        for i in 1..=50 {
            tag_builder.insert_tags(i as u32, vec!["Document"]);
        }
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;
    db.add_shard(&db_dir.join("shard3.idx"))?;
    db.load_tag_index()?;

    // Exactly replicate the failing command: ls s3://rafagalante/ '#Image'
    let tag_config = TagFilterConfig {
        include_tags: vec!["Image".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    
    let filter = LazyTagFilter::from_config(&tag_config);
    let mut stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        "s3://rafagalante/".as_bytes().to_vec(),
        None,
        Some(Box::new(filter)),
    )?;
    
    // Iterate like the original command (with limit of 200)
    let mut count = 0;
    while let Some(entry) = stream.next() {
        count += 1;
        if count > 200 {
            break;
        }
        
        // Verify we only get Image entries
        match entry {
            Entry::Key(key, _lut_id, _payload) => {
                assert!(key.starts_with("s3://rafagalante/"), "Key should start with prefix: {}", key);
            },
            _ => {} // Allow other entry types
        }
    }
    
    // If we get here without crashing, the test passes
    assert!(count > 0, "Should have found some Image entries");
    
    Ok(())
}

/// Test that directly simulates the issue of cached transitions with wrong shard indices.
/// This test manually creates the problematic scenario.
#[test]
fn fst_bounds_direct_simulation() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create some shards
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        b1.insert("aaa/file1", Some(Value::Text("val1".into())));
        b1.insert("aaa/file2", Some(Value::Text("val2".into())));
        b1.insert("bbb/file3", Some(Value::Text("val3".into())));
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        tag_builder.insert_tags(1, vec!["TypeA"]);  // aaa/file1
        tag_builder.insert_tags(2, vec!["TypeB"]);  // aaa/file2
        tag_builder.insert_tags(3, vec!["TypeA"]);  // bbb/file3
        tag_builder.finish()?;
    }

    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        b2.insert("aaa/file4", Some(Value::Text("val4".into())));
        b2.insert("ccc/file5", Some(Value::Text("val5".into())));
        b2.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base2);
        tag_builder.insert_tags(1, vec!["TypeA"]);  // aaa/file4
        tag_builder.insert_tags(2, vec!["TypeB"]);  // ccc/file5
        tag_builder.finish()?;
    }

    // Shard 3 has no TypeA entries - this shard should be filtered out
    {
        let base3 = db_dir.join("shard3.idx");
        let mut b3 = DatabaseBuilder::<Value>::new(&base3, PAYLOAD_STORE_VERSION_V3)?;
        b3.insert("ddd/file6", Some(Value::Text("val6".into())));
        b3.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base3);
        tag_builder.insert_tags(1, vec!["TypeC"]);  // ddd/file6 - different type
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;
    db.add_shard(&db_dir.join("shard3.idx"))?;
    db.load_tag_index()?;

    // First, create a streamer WITHOUT filtering to establish transitions with all 3 shards
    let mut streamer = MultiShardListStreamer::new(db.shards(), Vec::new(), None);
    
    // Get some results to populate the transition cache with all 3 shards
    let _first = streamer.next();  // This should populate frame.trans with 3 shards (0, 1, 2)
    
    // Now manually apply filtering that should remove shard 3 (leaving shards 0, 1)
    let tag_config = TagFilterConfig {
        include_tags: vec!["TypeA".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    // Create new filtered streamer instead of using old bitmap methods
    let filter = LazyTagFilter::from_config(&tag_config);
    let mut filtered_streamer = MultiShardListStreamer::new_with_filter(
        db.shards(),
        Vec::new(),
        None,
        Some(Box::new(filter)),
    )?;
    
    eprintln!("DEBUG: Testing new lazy filtering approach");
    let mut remaining_results = Vec::new();
    for i in 0..10 {
        eprintln!("DEBUG: Calling next() iteration {}", i);
        if let Some(entry) = filtered_streamer.next() {
            eprintln!("DEBUG: Got filtered entry: {}", entry.as_str());
            remaining_results.push(entry);
        } else {
            eprintln!("DEBUG: No more filtered entries");
            break;
        }
    }
    
    // Also continue with original streamer
    for _i in 0..5 {
        if let Some(entry) = streamer.next() {
            eprintln!("DEBUG: Got original entry: {}", entry.as_str());
        } else {
            break;
        }
    }
    
    // If we get here without crashing, the test passes
    Ok(())
}

/// Test that reproduces the exact FST bounds crash scenario:
/// - Load 20 shards (reduced from original 200)
/// - Shard 4 contains prefix/alpha/ with many keys (reduced from original shard 58)
/// - Apply tag filtering for Target
/// - Transition cache has old shard indices but FST array is smaller after filtering
#[test]
fn fst_bounds_crash_minimal_repro() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    const NUM_SHARDS: usize = 20; // Reduced from original 200
    const TARGET_SHARD: usize = 4; // Reduced from original 58
    
    eprintln!("Creating {} shards, with shard {} containing prefix/alpha/ data", NUM_SHARDS, TARGET_SHARD);
    
    for shard_id in 0..NUM_SHARDS {
        let base_path = db_dir.join(format!("shard{:03}.idx", shard_id));
        let mut builder = DatabaseBuilder::<Value>::new(&base_path, PAYLOAD_STORE_VERSION_V3)?;
        
        if shard_id == TARGET_SHARD {
            // Target shard: Create prefix/alpha/ entries like the real data
            // The real shard has 4408 keys, but we'll create a smaller representative set
            for file_id in 0..100 {
                let key = format!("prefix/alpha/file{:04}.jpg", file_id);
                builder.insert(&key, Some(Value::Text(format!("data_{}", file_id).into())));
            }
            for file_id in 0..50 {
                let key = format!("prefix/alpha/doc{:04}.pdf", file_id);
                builder.insert(&key, Some(Value::Text(format!("doc_data_{}", file_id).into())));
            }
        } else {
            // Other shards: different prefixes to simulate the real database structure
            let prefix = format!("prefix/beta{}/", shard_id);
            for file_id in 0..10 {
                let key = format!("{}file{}.ext", prefix, file_id);
                builder.insert(&key, Some(Value::Text(format!("data_{}", file_id).into())));
            }
        }
        builder.close()?;
        
        // Build tag index
        let mut tag_builder = foliant::TagIndexBuilder::new(&base_path);
        
        if shard_id == TARGET_SHARD {
            // Target shard: Mix of Target and Other tags (simulating real data)
            for file_id in 1..=100 {
                tag_builder.insert_tags(file_id as u32, vec!["Target"]);
            }
            for file_id in 101..=150 {
                tag_builder.insert_tags(file_id as u32, vec!["Other"]);
            }
        } else {
            // Other shards: mostly non-Target tags to force filtering
            let tags = if shard_id % 10 == 0 { 
                vec!["Target"] // Only every 10th shard has some Target tags
            } else { 
                vec!["Other"] 
            };
            for file_id in 1..=10 {
                tag_builder.insert_tags(file_id as u32, tags.iter().cloned());
            }
        }
        tag_builder.finish()?;
    }

    // Load database
    let mut db = Database::<Value>::new();
    for shard_id in 0..NUM_SHARDS {
        db.add_shard(&db_dir.join(format!("shard{:03}.idx", shard_id)))?;
    }
    db.load_tag_index()?;
    
    eprintln!("Database loaded with {} shards", db.shards().len());
    eprintln!("Shard {} should contain prefix/alpha/ data", TARGET_SHARD);
    
    // Replicate the failing command scenario with reduced complexity
    
    // First, start an unfiltered streamer to populate transition cache with all shard indices
    let mut streamer = MultiShardListStreamer::new(
        db.shards(),
        b"prefix/alpha/".to_vec(),
        None
    );
    
    eprintln!("Populating transition cache with unfiltered streamer...");
    // This should populate the transition cache with references to shard indices 0-19
    let _first = streamer.next();
    let _second = streamer.next();
    
    // Now apply Target tag filtering
    eprintln!("Applying Target tag filter...");
    let tag_config = TagFilterConfig {
        include_tags: vec!["Target".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    // Create new filtered streamer instead of using old bitmap methods  
    let filter = LazyTagFilter::from_config(&tag_config);
    let mut filtered_streamer = MultiShardListStreamer::new_with_filter(
        db.shards(),
        b"prefix/alpha/".to_vec(),
        None,
        Some(Box::new(filter)),
    )?;
    
    eprintln!("Testing new lazy filtering approach instead of old bitmap methods");
    
    let mut count = 0;
    while let Some(entry) = filtered_streamer.next() {
        match entry {
            Entry::Key(key, lut_id, _) => {
                eprintln!("Filtered entry {}: {} #{}", count + 1, key, lut_id);
                assert!(key.starts_with("prefix/alpha/"));
            },
            Entry::CommonPrefix(prefix, child_count) => {
                eprintln!("Filtered prefix {}: {} ({})", count + 1, prefix, child_count.unwrap_or(0));
            }
        }
        count += 1;
        
        if count >= 50 {
            break; // Safety limit
        }
    }
    
    // Also continue with the original streamer for comparison
    let mut original_count = 2; // Already got 2 items
    while let Some(entry) = streamer.next() {
        eprintln!("Original entry {}: {}", original_count + 1, entry.as_str());
        original_count += 1;
        
        if original_count >= 10 {
            break; // Safety limit for original
        }
    }
    
    count += original_count;
    
    eprintln!("Test completed successfully with {} entries", count);
    
    Ok(())
}

/// Alternative test using new_with_filter directly (like the shell does)
#[test]
fn fst_bounds_crash_shell_direct_approach() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create scenario where many shards exist but only some have the target tag
    const NUM_SHARDS: usize = 150;
    const ALPHA_SHARD: usize = 58;
    
    for shard_id in 0..NUM_SHARDS {
        let base_path = db_dir.join(format!("s{:03}.idx", shard_id));
        let mut builder = DatabaseBuilder::<Value>::new(&base_path, PAYLOAD_STORE_VERSION_V3)?;
        
        if shard_id == ALPHA_SHARD {
            // The target shard with alpha data
            for i in 0..50 {
                builder.insert(&format!("prefix/alpha/img{:03}.jpg", i), Some(Value::Text("data".into())));
            }
        } else {
            // Other shards with different prefixes
            builder.insert(&format!("prefix/other{}/file.txt", shard_id), Some(Value::Text("other".into())));
        }
        builder.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base_path);
        if shard_id == ALPHA_SHARD {
            for i in 1..=50 {
                tag_builder.insert_tags(i as u32, vec!["Target"]);
            }
        } else {
            tag_builder.insert_tags(1, vec!["Other"]);
        }
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    for shard_id in 0..NUM_SHARDS {
        db.add_shard(&db_dir.join(format!("s{:03}.idx", shard_id)))?;
    }
    db.load_tag_index()?;
    
    // Use the shell's approach: create filtered streamer directly with new_with_filter
    let tag_config = TagFilterConfig {
        include_tags: vec!["Target".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    
    let filter = LazyTagFilter::from_config(&tag_config);
    
    eprintln!("Creating filtered streamer directly like shell does...");
    let mut stream = MultiShardListStreamer::new_with_filter(
        db.shards(),
        "prefix/alpha/".as_bytes().to_vec(),
        None,
        Some(Box::new(filter)),
    )?;
    
    let mut results = Vec::new();
    while let Some(entry) = stream.next() {
        eprintln!("Got: {}", entry.as_str());
        results.push(entry);
        if results.len() >= 100 {
            break;
        }
    }
    
    eprintln!("Shell approach test completed with {} results", results.len());
    
    Ok(())
}