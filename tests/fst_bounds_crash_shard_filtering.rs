use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, TagMode};
use foliant::multi_list::{MultiShardListStreamer, TagFilterConfig, TagFilterBitmap, ShardBitmapFilter};
use foliant::Streamer;
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

/// Test that reproduces the FST bounds crash when:
/// 1. We have many shards
/// 2. Some early shards get filtered out during tag filtering
/// 3. Cached transitions still reference the original shard indices
/// 4. After filtering, self.fsts array is smaller but transitions cache has old indices
#[test]
fn fst_bounds_crash_early_shard_filtering() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create scenario where early shards get filtered out
    const NUM_SHARDS: usize = 20;
    
    for shard_id in 0..NUM_SHARDS {
        let base_path = db_dir.join(format!("shard{:02}.idx", shard_id));
        let mut builder = DatabaseBuilder::<Value>::new(&base_path, PAYLOAD_STORE_VERSION_V3)?;
        
        // All shards have similar structure to create complex transitions
        for file_id in 0..5 {
            let key = format!("s3://bucket/path{}/file{}.ext", shard_id, file_id);
            builder.insert(&key, Some(Value::Text(format!("data{}", file_id).into())));
        }
        builder.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base_path);
        // Critical: early shards (0-4) don't have target tag, later shards (10+) do
        // This means when filtering for "target", we'll remove shards 0-9 but keep 10-19
        // The issue: transition cache populated with references to shards 0-19
        // After filtering: only shards 10-19 remain, so fsts array only has indices 0-9
        // But cached transitions still reference original indices 10-19!
        let tags = if shard_id >= 10 {
            vec!["target"]  // Only shards 10+ have the target tag
        } else {
            vec!["other"]   // Shards 0-9 have different tag
        };
        
        for file_id in 1..=5 {
            tag_builder.insert_tags(file_id as u32, tags.iter().cloned());
        }
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    for shard_id in 0..NUM_SHARDS {
        db.add_shard(&db_dir.join(format!("shard{:02}.idx", shard_id)))?;
    }
    db.load_tag_index()?;
    
    eprintln!("Created {} shards. Shards 0-9 have 'other' tag, shards 10-19 have 'target' tag", NUM_SHARDS);
    
    // Start with unfiltered streamer to populate transition cache
    let mut streamer = MultiShardListStreamer::new(
        db.shards(),
        b"s3://bucket/".to_vec(),
        None
    );
    
    // Iterate a bit to populate transition cache with all shard indices 0-19
    eprintln!("Populating transition cache with all shard indices...");
    let _first = streamer.next();
    let _second = streamer.next();
    
    // Now apply filtering that removes shards 0-9, keeping only 10-19
    // This should remap shard indices: [0,1,2,...,19] -> [0,1,2,...,9] (for shards 10-19)
    // But transition cache still has references to original indices 10-19!
    eprintln!("Applying filter that removes early shards 0-9...");
    
    let tag_config = TagFilterConfig {
        include_tags: vec!["target".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    let filter = TagFilterBitmap::new(db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    let bitmaps = filter.build_shard_bitmaps(db.shards())?;
    
    // Count remaining shards
    let remaining = bitmaps.iter().filter(|bm| bm.is_some()).count();
    eprintln!("Filtering keeps {} out of {} shards", remaining, NUM_SHARDS);
    
    // Apply filtering - this should trigger the shard index remapping bug
    streamer.apply_shard_bitmaps(db.shards(), bitmaps);
    
    // Continue iteration - cached transitions may have indices 10-19 
    // but self.fsts now only has length 10 (indices 0-9)
    // This should cause: "index out of bounds: the len is 10 but the index is 15"
    eprintln!("Continuing iteration - this may crash with FST bounds error...");
    
    let mut count = 2; // Already got 2 items
    while let Some(entry) = streamer.next() {
        eprintln!("Entry {}: {}", count + 1, entry.as_str());
        count += 1;
        
        if count >= 50 {
            break; // Safety limit
        }
    }
    
    eprintln!("Success! Processed {} entries without FST bounds crash", count);
    
    Ok(())
}

/// Simpler test focusing on the core shard index remapping issue
#[test]
fn fst_bounds_crash_index_remapping() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create shards where some get filtered out
    const TOTAL_SHARDS: usize = 8;
    
    for shard_id in 0..TOTAL_SHARDS {
        let base_path = db_dir.join(format!("s{}.idx", shard_id));
        let mut builder = DatabaseBuilder::<Value>::new(&base_path, PAYLOAD_STORE_VERSION_V3)?;
        
        builder.insert(&format!("key{}", shard_id), Some(Value::Text("data".into())));
        builder.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base_path);
        // Shards 0,1,2 have "remove" tag -> will be filtered out
        // Shards 3,4,5,6,7 have "keep" tag -> will remain  
        // After filtering: original indices [3,4,5,6,7] -> new indices [0,1,2,3,4]
        let tag = if shard_id < 3 { "remove" } else { "keep" };
        tag_builder.insert_tags(1, vec![tag]);
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    for shard_id in 0..TOTAL_SHARDS {
        db.add_shard(&db_dir.join(format!("s{}.idx", shard_id)))?;
    }
    db.load_tag_index()?;
    
    // Start unfiltered to cache transitions with all shard indices 0-7
    let mut streamer = MultiShardListStreamer::new(db.shards(), Vec::new(), None);
    let _first = streamer.next(); // Populate transition cache
    
    eprintln!("Transition cache populated with shard indices 0-7");
    
    // Filter to remove shards 0,1,2 - keeping 3,4,5,6,7
    // self.fsts will have length 5, but cached transitions may reference indices 3-7
    let tag_config = TagFilterConfig {
        include_tags: vec!["keep".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    let filter = TagFilterBitmap::new(db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    let bitmaps = filter.build_shard_bitmaps(db.shards())?;
    
    eprintln!("Applying filter: removes shards 0,1,2; keeps 3,4,5,6,7 -> remapped to 0,1,2,3,4");
    streamer.apply_shard_bitmaps(db.shards(), bitmaps);
    
    // Continue iteration - if cache has reference to old shard index 5,
    // but new fsts array only has indices 0-4, we get bounds error
    let mut results = Vec::new();
    while let Some(entry) = streamer.next() {
        eprintln!("Got: {}", entry.as_str());
        results.push(entry);
        if results.len() > 10 {
            break;
        }
    }
    
    eprintln!("Index remapping test passed with {} results", results.len());
    
    Ok(())
}