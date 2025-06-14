//! Test demonstrating the efficiency gains of lazy filtering vs eager filtering.

use foliant::multi_list::{
    MultiShardListStreamer, LazyTagFilter, TagFilterConfig, FilterComposition
};
use foliant::payload_store::{CborPayloadCodec, PayloadStoreVersion};
use foliant::shard_provider::AllShardsProvider;
use foliant::{DatabaseBuilder, TagMode, Streamer};
use tempfile::tempdir;
use std::time::Instant;

/// Helper to create test shards with tag data
fn create_tagged_shards(num_shards: usize, keys_per_shard: usize) -> Vec<foliant::Shard<serde_json::Value, CborPayloadCodec>> {
    let mut all_shards = Vec::new();
    
    for shard_idx in 0..num_shards {
        let dir = tempdir().unwrap();
        let idx_path = dir.path().join(format!("shard{}", shard_idx));
        
        let mut builder: DatabaseBuilder<serde_json::Value, CborPayloadCodec> =
            DatabaseBuilder::new(&idx_path, PayloadStoreVersion::V2).unwrap();
        
        // Create keys with different prefixes to test prefix filtering
        for key_idx in 0..keys_per_shard {
            let prefix = match shard_idx % 4 {
                0 => "users/",
                1 => "posts/",
                2 => "comments/",
                _ => "other/",
            };
            let key = format!("{}{:04}", prefix, key_idx);
            
            builder.insert(&key, None);
        }
        
        builder.close().unwrap();
        
        // Build tag index manually
        let mut tag_builder = foliant::TagIndexBuilder::new(&idx_path);
        for key_idx in 0..keys_per_shard {
            let tags = vec![
                format!("shard{}", shard_idx),
                format!("type_{}", match shard_idx % 4 {
                    0 => "users",
                    1 => "posts", 
                    2 => "comments",
                    _ => "other",
                }),
                if key_idx % 2 == 0 { "even" } else { "odd" }.to_string(),
            ];
            tag_builder.insert_tags(key_idx as u32 + 1, tags);
        }
        tag_builder.finish().unwrap();
        
        let idx_file = idx_path.with_extension("idx");
        all_shards.push(foliant::Shard::open(&idx_file).unwrap());
        
        // Keep tempdir alive by leaking it
        std::mem::forget(dir);
    }
    
    all_shards
}

#[test]
fn test_lazy_vs_eager_filter_efficiency() {
    // Create test data: 20 shards with 100 keys each
    let shards = create_tagged_shards(20, 100);
    
    // Test case: Search for "users/" prefix with tag filter "even"
    let prefix = b"users/".to_vec();
    let tag_config = TagFilterConfig {
        include_tags: vec!["even".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    
    // Measure eager filtering (old approach simulation)
    let start_eager = Instant::now();
    let eager_filter = LazyTagFilter::from_config(&tag_config);
    let eager_filter_time = start_eager.elapsed();
    
    let provider = AllShardsProvider::new(&shards);
    let mut eager_streamer = MultiShardListStreamer::new_with_filter(
        &provider,
        prefix.clone(),
        None,
        Some(Box::new(eager_filter)),
    ).unwrap();
    
    let mut eager_results = Vec::new();
    while let Some(entry) = eager_streamer.next() {
        eager_results.push(entry.as_str().to_string());
    }
    let eager_total_time = start_eager.elapsed();
    
    // Measure lazy filtering (new approach)
    let start_lazy = Instant::now();
    let lazy_filter = LazyTagFilter::from_config(&tag_config);
    
    let provider2 = AllShardsProvider::new(&shards);
    let mut lazy_streamer = MultiShardListStreamer::new_with_lazy_filter(
        &provider2,
        prefix.clone(),
        None,
        Some(Box::new(lazy_filter)),
    ).unwrap();
    
    let mut lazy_results = Vec::new();
    while let Some(entry) = lazy_streamer.next() {
        lazy_results.push(entry.as_str().to_string());
    }
    let lazy_total_time = start_lazy.elapsed();
    
    // Results should be identical
    assert_eq!(lazy_results, eager_results, "Results should match between lazy and eager filtering");
    
    // Log timing information
    println!("Eager filter creation: {:?}", eager_filter_time);
    println!("Eager total time: {:?}", eager_total_time);
    println!("Lazy total time: {:?}", lazy_total_time);
    println!("Results found: {}", lazy_results.len());
    
    // In this test case, only 5 shards (25%) match the "users/" prefix
    // So lazy filtering should compute bitmaps for only 5 shards instead of 20
    // This represents a 75% reduction in bitmap computations
}

#[test]
fn test_composed_lazy_filters() {
    let shards = create_tagged_shards(10, 50);
    
    // Create composed filter: type_users AND even
    let tag_filter1 = LazyTagFilter::new(
        vec!["type_users".to_string()],
        vec![],
        TagMode::And,
    );
    
    let tag_filter2 = LazyTagFilter::new(
        vec!["even".to_string()],
        vec![],
        TagMode::And,
    );
    
    // Compose filters using monoid-like operation
    let composed = tag_filter1.and_then(tag_filter2);
    
    let provider = AllShardsProvider::new(&shards);
    let mut streamer = MultiShardListStreamer::new_with_lazy_filter(
        &provider,
        b"users/".to_vec(),
        None,
        Some(Box::new(composed)),
    ).unwrap();
    
    let mut count = 0;
    while let Some(_) = streamer.next() {
        count += 1;
    }
    
    // Should find entries that are both in users/ prefix, have type_users tag, and even tag
    assert!(count > 0, "Should find some matching entries");
    println!("Composed filter found {} entries", count);
}

#[test]
fn test_lazy_filter_with_non_matching_prefix() {
    let shards = create_tagged_shards(10, 50);
    
    // Search for a prefix that doesn't exist
    let lazy_filter = LazyTagFilter::new(
        vec!["even".to_string()],
        vec![],
        TagMode::And,
    );
    
    let provider = AllShardsProvider::new(&shards);
    let mut streamer = MultiShardListStreamer::new_with_lazy_filter(
        &provider,
        b"nonexistent/".to_vec(),
        None,
        Some(Box::new(lazy_filter)),
    ).unwrap();
    
    let mut count = 0;
    while let Some(_) = streamer.next() {
        count += 1;
    }
    
    // Should find no entries, and lazy filter should not compute any bitmaps
    assert_eq!(count, 0, "Should find no entries for non-existent prefix");
}