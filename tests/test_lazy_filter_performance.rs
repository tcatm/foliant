use foliant::{Database, TagMode, Streamer};
use foliant::multi_list::{MultiShardListStreamer, LazyTagFilter};
use std::time::Instant;

#[test]
#[ignore] // Run with: cargo test --test test_lazy_filter_performance -- --ignored --nocapture
fn test_frame_rebuild_optimization() {
    // This test verifies that the frame rebuild optimization works
    // by checking that filtered frame states are reused instead of rebuilt
    
    // Load a test database with multiple shards
    let mut db = Database::<serde_json::Value>::new();
    
    // Add shards if they exist in test data
    let test_shards = vec![
        "tests/data/shard1.idx",
        "tests/data/shard2.idx", 
        "tests/data/shard3.idx",
    ];
    
    for shard_path in test_shards {
        if std::path::Path::new(shard_path).exists() {
            db.add_shard(shard_path).ok();
        }
    }
    
    if db.shards().is_empty() {
        eprintln!("No test shards found, skipping performance test");
        return;
    }
    
    // Load tag indices
    db.load_tag_index().ok();
    
    // Test with a common prefix
    let prefix = "";
    let include_tags = vec!["test".to_string()];
    
    // Time the creation with lazy filter (which previously rebuilt frames)
    let start = Instant::now();
    let filter = Box::new(LazyTagFilter::new(
        include_tags.clone(),
        vec![],
        TagMode::Or,
    ));
    
    let mut streamer = MultiShardListStreamer::new_with_filter(
        &db,
        prefix.as_bytes().to_vec(),
        Some(b'/'),
        Some(filter),
    ).expect("Failed to create streamer");
    
    let setup_time = start.elapsed();
    
    // Consume some entries to ensure it works
    let mut count = 0;
    while let Some(_) = streamer.next() {
        count += 1;
        if count >= 10 {
            break;
        }
    }
    
    println!("Setup time with lazy filter: {:?}", setup_time);
    println!("Found {} entries", count);
    
    // Compare with no filter
    let start = Instant::now();
    let _streamer_no_filter = MultiShardListStreamer::new(
        &db,
        prefix.as_bytes().to_vec(),
        Some(b'/'),
    );
    
    let setup_time_no_filter = start.elapsed();
    
    println!("Setup time without filter: {:?}", setup_time_no_filter);
    
    // The setup time with filter should not be significantly longer
    // than without filter (allowing for bitmap computation overhead)
    assert!(setup_time.as_micros() < setup_time_no_filter.as_micros() * 10,
            "Setup with filter took too long, possible frame rebuild issue");
}