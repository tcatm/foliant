use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, TagIndexBuilder, Streamer};
use serde_json::{json, Value};
use std::fs;
use tempfile::tempdir;

#[test]
fn build_tag_index_and_list_tags() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("apple", Some(json!({"tags":["fruit","red"]})));
    builder.insert("banana", Some(json!({"tags":["fruit","yellow"]})));
    builder.insert("cherry", Some(json!({"tags":["fruit","red"]})));
    builder.insert("date", Some(json!({"tags":["fruit","brown"]})));
    builder.insert("eggplant", Some(json!({"tags":["vegetable","purple"]})));
    builder.close()?;
    
    let mut db_builder = Database::<Value>::open(&base)?;
    TagIndexBuilder::build_index(&mut db_builder, "tags", None)?;

    let db: Database<Value> = Database::open(&base)?;

    // Test list_tags with counts
    let mut tags_with_counts: Vec<(String, usize)> = db.list_tags(None)?.collect();
    tags_with_counts.sort_by(|a, b| a.0.cmp(&b.0));
    
    let expected = vec![
        ("brown".to_string(), 1),
        ("fruit".to_string(), 4),
        ("purple".to_string(), 1),
        ("red".to_string(), 2),
        ("vegetable".to_string(), 1),
        ("yellow".to_string(), 1),
    ];
    assert_eq!(tags_with_counts, expected);

    // Test that tags are returned in the expected order
    let mut tag_stream = db.list_tags(None)?;
    let mut tag_names: Vec<String> = Vec::new();
    while let Some((tag, _count)) = tag_stream.next() {
        tag_names.push(tag);
    }
    
    // Verify all expected tags are present (order might vary)
    let mut sorted_names = tag_names.clone();
    sorted_names.sort();
    let expected_names = vec![
        "brown".to_string(),
        "fruit".to_string(),
        "purple".to_string(),
        "red".to_string(),
        "vegetable".to_string(),
        "yellow".to_string(),
    ];
    assert_eq!(sorted_names, expected_names);

    Ok(())
}

#[test]
fn list_tags_with_prefix() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_prefix.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("fruits/apple", Some(json!({"tags":["fruit","red"]})));
    builder.insert("fruits/banana", Some(json!({"tags":["fruit","yellow"]})));
    builder.insert("vegetables/carrot", Some(json!({"tags":["vegetable","orange"]})));
    builder.insert("vegetables/lettuce", Some(json!({"tags":["vegetable","green"]})));
    builder.close()?;
    
    let mut db_builder = Database::<Value>::open(&base)?;
    TagIndexBuilder::build_index(&mut db_builder, "tags", None)?;

    let db: Database<Value> = Database::open(&base)?;

    // Test tags under fruits/ prefix (should only count tags from entries under fruits/)
    let mut fruit_tags: Vec<(String, usize)> = db.list_tags(Some("fruits/"))?.collect();
    fruit_tags.sort_by(|a, b| a.0.cmp(&b.0));
    
    let expected_fruit = vec![
        ("fruit".to_string(), 2),
        ("red".to_string(), 1),
        ("yellow".to_string(), 1),
    ];
    assert_eq!(fruit_tags, expected_fruit);

    // Test tags under vegetables/ prefix (should only count tags from entries under vegetables/)
    let mut veggie_tags: Vec<(String, usize)> = db.list_tags(Some("vegetables/"))?.collect();
    veggie_tags.sort_by(|a, b| a.0.cmp(&b.0));
    
    let expected_veggie = vec![
        ("green".to_string(), 1),
        ("orange".to_string(), 1),
        ("vegetable".to_string(), 2),
    ];
    assert_eq!(veggie_tags, expected_veggie);

    Ok(())
}

#[test]
fn build_tag_index_empty_tags() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_empty.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("a", Some(json!({"tags":[]})));
    builder.insert("b", Some(json!({"no_tags_field": "value"})));
    builder.close()?;
    
    let mut db_builder = Database::<Value>::open(&base)?;
    TagIndexBuilder::build_index(&mut db_builder, "tags", None)?;

    let db: Database<Value> = Database::open(&base)?;
    let tags: Vec<(String, usize)> = db.list_tags(None)?.collect();
    assert!(tags.is_empty());
    
    
    Ok(())
}

#[test]
fn build_tag_index_multi_shard() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base_dir = dir.path().join("db_multi");
    fs::create_dir(&base_dir)?;
    
    // Create first shard
    {
        let mut b = DatabaseBuilder::<Value>::new(&base_dir.join("s1.idx"), PAYLOAD_STORE_VERSION_V3)?;
        b.insert("foo", Some(json!({"tags":["tag1","common"]})));
        b.insert("bar", Some(json!({"tags":["tag1","tag2"]})));
        b.close()?;
    }
    
    // Create second shard
    {
        let mut b = DatabaseBuilder::<Value>::new(&base_dir.join("s2.idx"), PAYLOAD_STORE_VERSION_V3)?;
        b.insert("baz", Some(json!({"tags":["tag2","common"]})));
        b.insert("qux", Some(json!({"tags":["tag3"]})));
        b.close()?;
    }
    
    // Build tag indexes
    let mut db_builder = Database::<Value>::new();
    db_builder.add_shard(&base_dir.join("s1.idx"))?;
    db_builder.add_shard(&base_dir.join("s2.idx"))?;
    TagIndexBuilder::build_index(&mut db_builder, "tags", None)?;
    
    // Test tag counts across shards
    let mut db = Database::<Value>::new();
    db.add_shard(&base_dir.join("s1.idx"))?;
    db.add_shard(&base_dir.join("s2.idx"))?;
    
    let mut tags_with_counts: Vec<(String, usize)> = db.list_tags(None)?.collect();
    tags_with_counts.sort_by(|a, b| a.0.cmp(&b.0));
    
    let expected = vec![
        ("common".to_string(), 2),  // appears in both shards
        ("tag1".to_string(), 2),    // appears twice in shard1
        ("tag2".to_string(), 2),    // appears once in each shard
        ("tag3".to_string(), 1),    // appears once in shard2
    ];
    assert_eq!(tags_with_counts, expected);

    Ok(())
}

#[test]
fn tag_index_performance_large_dataset() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_large.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    
    const TOTAL: usize = 1000;
    for i in 0..TOTAL {
        let key = format!("item{:04}", i);
        let tag = if i % 3 == 0 { "category_a" } else if i % 3 == 1 { "category_b" } else { "category_c" };
        builder.insert(&key, Some(json!({"tags":[tag, "common"]})));
    }
    builder.close()?;
    
    let mut db_builder = Database::<Value>::open(&base)?;
    TagIndexBuilder::build_index(&mut db_builder, "tags", None)?;

    let db: Database<Value> = Database::open(&base)?;
    
    let mut tags_with_counts: Vec<(String, usize)> = db.list_tags(None)?.collect();
    tags_with_counts.sort_by(|a, b| a.0.cmp(&b.0));
    
    let expected = vec![
        ("category_a".to_string(), 334),  // items 0, 3, 6, ... (334 items)
        ("category_b".to_string(), 333),  // items 1, 4, 7, ... (333 items)
        ("category_c".to_string(), 333),  // items 2, 5, 8, ... (333 items)
        ("common".to_string(), 1000),     // all items
    ];
    assert_eq!(tags_with_counts, expected);

    Ok(())
}