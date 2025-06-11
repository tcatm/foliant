use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, TagMode};
use foliant::multi_list::{MultiShardListStreamer, TagFilterConfig, TagFilterBitmap, ShardBitmapFilter};
use foliant::Streamer;
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

/// Test that reproduces the exact FST bounds crash from the real failing command:
/// `target/debug/foliant shell -i data/db -n 200 -- ls s3://rafagalante/ '#Image'`
/// 
/// This test replicates the actual sequence:
/// 1. new_with_filter() calls new() which does walk_prefix() with ALL shards
/// 2. walk_prefix() creates frame states with Node references from original FST array 
/// 3. apply_shard_bitmaps() replaces FST array with filtered subset
/// 4. Frame states now have Node references from old FSTs but shard indices for new FST array
/// 5. When next() is called, Node.transition() accesses invalid FST data -> crash
#[test]
fn fst_bounds_crash_realistic_new_with_filter() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create scenario that mirrors real data: many shards, specific prefix exists in one shard
    const NUM_SHARDS: usize = 100;  // Closer to real "-n 200" 
    const TARGET_SHARD: usize = 58; // Real shard number from error message
    
    eprintln!("Creating {} shards to simulate real database", NUM_SHARDS);
    
    for shard_id in 0..NUM_SHARDS {
        let base_path = db_dir.join(format!("shard{:03}.idx", shard_id));
        let mut builder = DatabaseBuilder::<Value>::new(&base_path, PAYLOAD_STORE_VERSION_V3)?;
        
        if shard_id == TARGET_SHARD {
            // Target shard: Contains the "s3://rafagalante/" prefix like real data
            eprintln!("Shard {}: Creating s3://rafagalante/ data (target shard)", shard_id);
            for file_id in 0..50 {
                let key = format!("s3://rafagalante/image{:04}.jpg", file_id);
                builder.insert(&key, Some(Value::Text(format!("image_data_{}", file_id).into())));
            }
            for file_id in 0..30 {
                let key = format!("s3://rafagalante/document{:04}.pdf", file_id);
                builder.insert(&key, Some(Value::Text(format!("doc_data_{}", file_id).into())));
            }
        } else {
            // Other shards: Different prefixes to simulate real multi-shard database
            let prefix = if shard_id < 20 {
                format!("s3://bucket{}/", shard_id)
            } else if shard_id < 50 {
                format!("s3://data{}/", shard_id)
            } else {
                format!("s3://archive{}/", shard_id)
            };
            
            for file_id in 0..5 {
                let key = format!("{}file{}.txt", prefix, file_id);
                builder.insert(&key, Some(Value::Text(format!("content_{}", file_id).into())));
            }
        }
        builder.close()?;
        
        // Build tag index to match real scenario
        let mut tag_builder = foliant::TagIndexBuilder::new(&base_path);
        
        if shard_id == TARGET_SHARD {
            // Target shard: Mix of Image and Document tags (like real data)
            for file_id in 1..=50 {
                tag_builder.insert_tags(file_id as u32, vec!["Image"]);
            }
            for file_id in 51..=80 {
                tag_builder.insert_tags(file_id as u32, vec!["Document"]);
            }
        } else {
            // Other shards: Mostly non-Image tags to force filtering
            let tags = if shard_id % 15 == 0 { 
                vec!["Image"] // Only a few shards have Image tags
            } else { 
                vec!["Other", "Data"] 
            };
            for file_id in 1..=5 {
                tag_builder.insert_tags(file_id as u32, tags.iter().cloned());
            }
        }
        tag_builder.finish()?;
    }

    // Load database exactly like the real command
    let mut db = Database::<Value>::new();
    for shard_id in 0..NUM_SHARDS {
        db.add_shard(&db_dir.join(format!("shard{:03}.idx", shard_id)))?;
    }
    db.load_tag_index()?;
    
    eprintln!("Database loaded with {} shards", db.shards().len());
    eprintln!("Shard {} contains s3://rafagalante/ data", TARGET_SHARD);
    
    // Exactly replicate the failing shell command sequence:
    // `foliant shell -i data/db -n 200 -- ls s3://rafagalante/ '#Image'`
    
    let tag_config = TagFilterConfig {
        include_tags: vec!["Image".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    
    let filter = TagFilterBitmap::new(db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    
    eprintln!("Creating filtered streamer with new_with_filter (like shell command)...");
    
    // This is the exact call path that fails:
    // 1. new_with_filter() -> new() -> walk_prefix() [creates frame states with ALL shards]
    // 2. new_with_filter() -> apply_shard_bitmaps() [replaces FST array, frame states now invalid]
    let mut stream = MultiShardListStreamer::new_with_filter(
        db.shards(),
        "s3://rafagalante/".as_bytes().to_vec(),
        None, // No delimiter like shell default
        Some(filter),
    )?;
    
    eprintln!("Starting iteration (this should crash with FST bounds error)...");
    
    // Iterate like the shell does - this is where the crash happens
    let mut count = 0;
    while let Some(entry) = stream.next() {
        count += 1;
        eprintln!("Entry {}: {}", count, entry.as_str());
        
        // Safety limit to avoid infinite loop if bug is fixed
        if count >= 50 {
            break;
        }
    }
    
    eprintln!("Test completed successfully with {} entries (bug is fixed!)", count);
    
    Ok(())
}

/// Simpler test case focusing on the exact issue: walk_prefix followed by apply_shard_bitmaps
#[test]
fn fst_bounds_crash_walk_prefix_then_filter() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create minimal scenario that triggers the issue
    const NUM_SHARDS: usize = 10;
    
    for shard_id in 0..NUM_SHARDS {
        let base_path = db_dir.join(format!("s{}.idx", shard_id));
        let mut builder = DatabaseBuilder::<Value>::new(&base_path, PAYLOAD_STORE_VERSION_V3)?;
        
        if shard_id == 5 {
            // Only one shard has the target prefix
            builder.insert("target/file1.jpg", Some(Value::Text("data1".into())));
            builder.insert("target/file2.jpg", Some(Value::Text("data2".into())));
        } else {
            builder.insert(&format!("other{}/file.txt", shard_id), Some(Value::Text("other".into())));
        }
        builder.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base_path);
        if shard_id == 5 {
            tag_builder.insert_tags(1, vec!["Keep"]);
            tag_builder.insert_tags(2, vec!["Keep"]);
        } else {
            tag_builder.insert_tags(1, vec!["Remove"]);
        }
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    for shard_id in 0..NUM_SHARDS {
        db.add_shard(&db_dir.join(format!("s{}.idx", shard_id)))?;
    }
    db.load_tag_index()?;
    
    // Demonstrate the exact issue:
    eprintln!("Step 1: Create unfiltered streamer (calls walk_prefix with all {} shards)", NUM_SHARDS);
    let mut streamer = MultiShardListStreamer::new(db.shards(), b"target/".to_vec(), None);
    
    eprintln!("Step 2: Apply filtering (changes FST array, but frame states still reference old FSTs)");
    let tag_config = TagFilterConfig {
        include_tags: vec!["Keep".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    let filter = TagFilterBitmap::new(db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    let bitmaps = filter.build_shard_bitmaps(db.shards())?;
    
    let remaining_shards = bitmaps.iter().filter(|bm| bm.is_some()).count();
    eprintln!("Filtering keeps {} out of {} shards", remaining_shards, NUM_SHARDS);
    
    streamer.apply_shard_bitmaps(db.shards(), bitmaps);
    
    eprintln!("Step 3: Start iteration (frame states have Node refs from old FSTs, shard indices for new FST array)");
    
    let mut results = Vec::new();
    while let Some(entry) = streamer.next() {
        eprintln!("Got: {}", entry.as_str());
        results.push(entry);
        if results.len() > 10 {
            break;
        }
    }
    
    eprintln!("Success! Got {} results without crash", results.len());
    
    Ok(())
}