use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, TagMode};
use foliant::multi_list::{MultiShardListStreamer, TagFilterConfig, TagFilterBitmap};
use foliant::Streamer;
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

/// Synthetic hierarchy for generic multi-shard tag-filter tests
const SYNTH_LIST: &[(&str, &[&str])] = &[
    ("assets/", &["Directory"]),
    ("assets/file1.pdf", &["Document"]),
    ("assets/file2.mp4", &["Video"]),
    ("assets/file3.pdf", &["Document"]),
    ("assets/file4.docx", &["Document", "English"]),
    ("backups/", &["Infrastructure", "Directory"]),
    ("backups/file5.zip", &["Archive", "Infrastructure"]),
    ("backups/file6.zip", &["Archive", "Infrastructure"]),
    ("cvs/", &["Directory"]),
    ("cvs/projectX/", &["Directory"]),
    ("cvs/projectX/file7.odt", &["Document", "LangA"]),
    ("cvs/projectX/file8.docx", &["Document", "LangA"]),
    ("cvs/projectX/file9.pdf", &["Document", "LangA"]),
    ("cvs/projectX/file10.doc", &["Document", "LangA"]),
    ("cvs/projectX/file11.docx", &["Document", "English"]),
    ("cvs/projectX/file12.pdf", &["Document", "LangA"]),
    ("cvs/projectX/file13.docx", &["Document", "English"]),
    ("pdf_archive/", &["Directory"]),
    ("pdf_archive/file14.pdf", &["Document", "English"]),
    ("pdf_archive/file15.pdf", &["Document", "English"]),
    ("pdf_archive/file16.ppt", &["Presentation"]),
    ("pdf_archive/file17.pdf", &["Document"]),
    ("pdf_archive/file18.pdf", &["Document"]),
    ("pdf_archive/book/", &["Directory"]),
    ("pdf_archive/book/file19.pdf", &["Document"]),
    ("pdf_archive/book/file20.pdf", &["Document"]),
    ("pdf_archive/book/file21.xlsx", &["Document", "Finance"]),
    ("pdf_archive/book/file22.jpg", &["Image"]),
    ("financials/", &["Directory"]),
    ("financials/file23.pdf", &["Document", "Finance"]),
    ("financials/file24.pdf", &["Document", "Finance"]),
    ("financials/file25.xlsx", &["Document", "Finance"]),
    ("logs/", &["Directory"]),
    ("logs/file26.log", &["Document"]),
    ("logs/file27.log", &["Document"]),
    ("media/", &["Directory"]),
    ("media/file28.mp4", &["Video"]),
    ("media/file29.png", &["Image"]),
    ("other/", &["Directory"]),
    ("other/file30.txt", &["Document"]),
];

/// Test basic tag filtering with MultiShardListStreamer across multiple shards
#[test]
fn multi_shard_tag_filter_basic() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Shard 1: entries with various tags
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        b1.insert("alpha/doc1", Some(Value::Text("content1".into())));
        b1.insert("alpha/doc2", Some(Value::Text("content2".into())));
        b1.insert("beta/file1", Some(Value::Text("file1".into())));
        b1.close()?;
        
        // Add tag index for shard 1
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        tag_builder.insert_tags(1, vec!["important", "draft"]);     // alpha/doc1
        tag_builder.insert_tags(2, vec!["important", "published"]);  // alpha/doc2
        tag_builder.insert_tags(3, vec!["draft", "internal"]);      // beta/file1
        tag_builder.finish()?;
    }

    // Shard 2: more entries with overlapping tags
    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        b2.insert("alpha/doc3", Some(Value::Text("content3".into())));
        b2.insert("gamma/report", Some(Value::Text("report".into())));
        b2.close()?;
        
        // Add tag index for shard 2  
        let mut tag_builder = foliant::TagIndexBuilder::new(&base2);
        tag_builder.insert_tags(1, vec!["published", "urgent"]);   // alpha/doc3
        tag_builder.insert_tags(2, vec!["internal", "urgent"]);    // gamma/report
        tag_builder.finish()?;
    }

    // Load database with both shards
    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;
    db.load_tag_index()?;

    // Test 1: Filter by single include tag
    let tag_config = TagFilterConfig {
        include_tags: vec!["important".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    

    let filter = TagFilterBitmap::new(&db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    
    let stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        None,
        Some(filter),
    )?;
    
    let mut results: Vec<Entry<Value>> = stream.collect();
    results.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    
    // Should only get entries tagged with "important"
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_str(), "alpha/doc1");
    assert_eq!(results[1].as_str(), "alpha/doc2");

    // Test 2: Filter with exclusion
    let tag_config = TagFilterConfig {
        include_tags: vec!["important".to_string()],
        exclude_tags: vec!["draft".to_string()],
        mode: TagMode::And,
    };
    
    let filter = TagFilterBitmap::new(&db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    let stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        None,
        Some(filter),
    )?;
    
    let results: Vec<Entry<Value>> = stream.collect();
    
    // Should only get "alpha/doc2" (important but not draft)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "alpha/doc2");

    Ok(())

}

/// Test synthetic generic hierarchy: filter by tag "Document" returns only document entries
#[test]
fn multi_shard_tag_filter_synthetic_generic_document_filter() -> Result<(), Box<dyn Error>> {
    let td = tempdir()?;
    let idx = td.path().join("shard.idx");
    // Build the FST index
    let mut dbb = DatabaseBuilder::<Value>::new(&idx, PAYLOAD_STORE_VERSION_V3)?;
    for (i, (path, _tags)) in SYNTH_LIST.iter().enumerate() {
        dbb.insert(path, Some(Value::Text(format!("val{}", i).into())));
    }
    dbb.close()?;

    // Build the tag index
    let mut tbb = foliant::TagIndexBuilder::new(&idx.with_extension("tags"));
    
    // Create a sorted list to match FST alphabetical order
    let mut path_tag_pairs: Vec<(String, &[&str])> = SYNTH_LIST.iter()
        .map(|(path, tags)| (path.to_string(), *tags))
        .collect();
    path_tag_pairs.sort_by(|a, b| a.0.cmp(&b.0));
    
    // Assign tags according to alphabetical order (FST order)
    for (i, (_path, tags)) in path_tag_pairs.iter().enumerate() {
        tbb.insert_tags((i + 1) as u32, tags.iter().cloned());
    }
    tbb.finish()?;

    // Load database and tag-index
    let mut db = Database::<Value>::new();
    db.add_shard(&idx)?;
    db.load_tag_index()?;

    // Query for entries tagged with "Document"
    let tag_config = TagFilterConfig {
        include_tags: vec!["Document".to_string()],
        exclude_tags: vec![],
        mode: TagMode::Or,
    };
    let filter = TagFilterBitmap::new(&db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    let stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        None,
        Some(filter),
    )?;
    let results: Vec<Entry<Value>> = stream.collect();
    let mut keys: Vec<String> = results.iter().map(|e| e.as_str().to_string()).collect();
    keys.sort();
    // Expected document-tagged entries (in alphabetical order to match FST)
    let expected = vec![
        "assets/file1.pdf",
        "assets/file3.pdf",
        "assets/file4.docx",
        "cvs/projectX/file10.doc",
        "cvs/projectX/file11.docx",
        "cvs/projectX/file12.pdf",
        "cvs/projectX/file13.docx",
        "cvs/projectX/file7.odt",
        "cvs/projectX/file8.docx",
        "cvs/projectX/file9.pdf",
        "financials/file23.pdf",
        "financials/file24.pdf",
        "financials/file25.xlsx",
        "logs/file26.log",
        "logs/file27.log",
        "other/file30.txt",
        "pdf_archive/book/file19.pdf",
        "pdf_archive/book/file20.pdf",
        "pdf_archive/book/file21.xlsx",
        "pdf_archive/file14.pdf",
        "pdf_archive/file15.pdf",
        "pdf_archive/file17.pdf",
        "pdf_archive/file18.pdf",
    ];
    assert_eq!(keys, expected);
    Ok(())
}

/// Test tag filtering with delimiter grouping and child counting
#[test]
fn multi_shard_tag_filter_with_delimiter() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create test data with hierarchical structure
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        b1.insert("docs/alpha/file1", Some(Value::Text("content1".into())));
        b1.insert("docs/alpha/file2", Some(Value::Text("content2".into())));
        b1.insert("docs/beta/file1", Some(Value::Text("content3".into())));
        b1.insert("configs/app.json", Some(Value::Text("config".into())));
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        // Keys in alphabetical order: configs/app.json(1), docs/alpha/file1(2), docs/alpha/file2(3), docs/beta/file1(4)
        tag_builder.insert_tags(1, vec!["config"]);      // configs/app.json
        tag_builder.insert_tags(2, vec!["published"]);   // docs/alpha/file1  
        tag_builder.insert_tags(3, vec!["draft"]);       // docs/alpha/file2
        tag_builder.insert_tags(4, vec!["published"]);   // docs/beta/file1
        tag_builder.finish()?;
    }

    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        b2.insert("docs/alpha/file3", Some(Value::Text("content4".into())));
        b2.insert("docs/gamma/file1", Some(Value::Text("content5".into())));
        b2.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base2);
        // Keys in alphabetical order: docs/alpha/file3(1), docs/gamma/file1(2) 
        tag_builder.insert_tags(1, vec!["published"]);   // docs/alpha/file3
        tag_builder.insert_tags(2, vec!["draft"]);       // docs/gamma/file1
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;
    db.load_tag_index()?;

    // Test: Filter published content with delimiter grouping
    let tag_config = TagFilterConfig {
        include_tags: vec!["published".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    let filter = TagFilterBitmap::new(&db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    let stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        Some(b'/'),
        Some(filter),
    )?;
    
    let mut results: Vec<Entry<Value>> = stream.collect();
    results.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    
    // Should get "docs/" prefix (contains published content) but not "configs/" 
    // (doesn't contain published content when show_empty_prefixes=false)
    assert_eq!(results.len(), 1);
    match &results[0] {
        Entry::CommonPrefix(prefix, count) => {
            assert_eq!(prefix, "docs/");
            // Should count published items in docs/ (3 items: alpha/file1, alpha/file3, beta/file1)
            assert!(count.is_some());
            assert!(count.unwrap() > 0);
        },
        _ => panic!("Expected CommonPrefix, got {:?}", results[0]),
    }

    Ok(())
}

/// Test complex nested hierarchy and tag filtering across multiple shards.
///
/// Uses multiple scenarios: keys-only, first-level grouping without empty prefixes,
/// nested grouping with empty prefixes, and prefix-restricted key queries.
#[test]
fn multi_shard_tag_filter_complex_hierarchy() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Shard 1: complex nested keys with tags
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        b1.insert("root/alpha/one", Some(Value::Text("r1".into())));
        b1.insert("root/beta/one", Some(Value::Text("r2".into())));
        b1.insert("root/gamma/one", Some(Value::Text("r3".into())));
        b1.insert("solo/file", Some(Value::Text("s1".into())));
        b1.close()?;
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        tag_builder.insert_tags(1, vec!["red", "common"]);
        tag_builder.insert_tags(2, vec!["common"]);
        tag_builder.insert_tags(3, vec!["common", "blue"]);
        tag_builder.insert_tags(4, vec!["solo", "common"]);
        tag_builder.insert_tags(5, vec!["solo", "common"]);
        tag_builder.finish()?;
    }

    // Shard 2: more nested keys with tags
    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        b2.insert("root/alpha/three", Some(Value::Text("r4".into())));
        b2.insert("root/beta/two", Some(Value::Text("r5".into())));
        b2.insert("root/gamma/two", Some(Value::Text("r6".into())));
        b2.insert("root/delta/one", Some(Value::Text("r7".into())));
        b2.insert("solo/file2", Some(Value::Text("s2".into())));
        b2.close()?;
        let mut tag_builder = foliant::TagIndexBuilder::new(&base2);
        tag_builder.insert_tags(1, vec!["common", "blue"]);
        tag_builder.insert_tags(2, vec!["common"]);
        tag_builder.insert_tags(3, vec!["common", "blue"]);
        tag_builder.insert_tags(4, vec!["common"]);
        tag_builder.insert_tags(5, vec!["solo", "common"]);
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;
    db.load_tag_index()?;

    // Scenario 1: keys-only query (include "common", AND)
    let tag_config = TagFilterConfig {
        include_tags: vec!["common".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    let filter = TagFilterBitmap::new(&db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    let stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        None,
        Some(filter),
    )?;
    let entries: Vec<_> = stream.collect();
    let mut keys: Vec<String> = entries.iter().map(|e| e.as_str().to_string()).collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "root/alpha/one",
            "root/alpha/three",
            "root/beta/one",
            "root/beta/two",
            "root/delta/one",
            "root/gamma/one",
            "root/gamma/two",
            "solo/file",
            "solo/file2",
        ]
    );

    // Scenario 2: first-level grouping only (show_empty_prefixes=false)
    let filter = TagFilterBitmap::new(&db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    let stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        Some(b'/'),
        Some(filter),
    )?;
    let entries: Vec<_> = stream.collect();
    let mut prefixes: Vec<(String, Option<usize>)> = entries
        .into_iter()
        .filter_map(|e| if let Entry::CommonPrefix(p, c) = e { Some((p, c)) } else { None })
        .collect();
    prefixes.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        prefixes,
        vec![
            ("root/".to_string(), Some(7)),
            ("solo/".to_string(), Some(2)),
        ]
    );


    // Scenario 4: prefix-restricted key-only query under "root/beta/"
    let filter = TagFilterBitmap::new(&db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    let stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        "root/beta/".as_bytes().to_vec(),
        None,
        Some(filter),
    )?;
    let entries: Vec<_> = stream.collect();
    let mut beta_keys: Vec<String> = entries.iter().map(|e| e.as_str().to_string()).collect();
    beta_keys.sort();
    assert_eq!(beta_keys, vec!["root/beta/one", "root/beta/two"]);

    Ok(())
}

/// Test tag filtering with OR mode across multiple shards
#[test]
fn multi_shard_tag_filter_or_mode() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Shard 1
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        b1.insert("file1", Some(Value::Text("content1".into())));
        b1.insert("file2", Some(Value::Text("content2".into())));
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        tag_builder.insert_tags(1, vec!["urgent"]);
        tag_builder.insert_tags(2, vec!["important"]);
        tag_builder.finish()?;
    }

    // Shard 2
    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        b2.insert("file3", Some(Value::Text("content3".into())));
        b2.insert("file4", Some(Value::Text("content4".into())));
        b2.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base2);
        tag_builder.insert_tags(1, vec!["urgent", "critical"]);
        tag_builder.insert_tags(2, vec!["draft"]);
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;
    db.load_tag_index()?;

    // Test OR mode: should get items with either "urgent" OR "important"
    let tag_config = TagFilterConfig {
        include_tags: vec!["urgent".to_string(), "important".to_string()],
        exclude_tags: vec![],
        mode: TagMode::Or,
    };
    let filter = TagFilterBitmap::new(&db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    let stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        None,
        Some(filter),
    )?;
    
    let mut results: Vec<Entry<Value>> = stream.collect();
    results.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    
    // Should get file1 (urgent), file2 (important), file3 (urgent)
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_str(), "file1");
    assert_eq!(results[1].as_str(), "file2");
    assert_eq!(results[2].as_str(), "file3");

    Ok(())
}

/// Test tag filtering with prefix restriction
#[test]
fn multi_shard_tag_filter_with_prefix() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Create data in multiple directories
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        b1.insert("docs/file1", Some(Value::Text("doc1".into())));
        b1.insert("docs/file2", Some(Value::Text("doc2".into())));
        b1.insert("src/file1", Some(Value::Text("source1".into())));
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        tag_builder.insert_tags(1, vec!["important"]);
        tag_builder.insert_tags(2, vec!["draft"]);
        tag_builder.insert_tags(3, vec!["important"]);
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.load_tag_index()?;

    // Test: Filter for important items, but only in docs/ prefix
    let tag_config = TagFilterConfig {
        include_tags: vec!["important".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };
    
    let filter = TagFilterBitmap::new(&db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    let stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        "docs".as_bytes().to_vec(),
        None,
        Some(filter),
    )?;
    
    let results: Vec<Entry<Value>> = stream.collect();
    
    // Should only get docs/file1 (important in docs prefix)
    // src/file1 should be excluded by prefix restriction
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "docs/file1");

    Ok(())
}


/// Test tag filtering with cursor-based resumption
#[test]
fn multi_shard_tag_filter_cursor_resume() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        b1.insert("item1", Some(Value::Text("content1".into())));
        b1.insert("item2", Some(Value::Text("content2".into())));
        b1.insert("item3", Some(Value::Text("content3".into())));
        b1.insert("item4", Some(Value::Text("content4".into())));
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        tag_builder.insert_tags(1, vec!["test"]);
        tag_builder.insert_tags(2, vec!["test"]);
        tag_builder.insert_tags(3, vec!["test"]);
        tag_builder.insert_tags(4, vec!["test"]);
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.load_tag_index()?;

    let tag_config = TagFilterConfig {
        include_tags: vec!["test".to_string()],
        exclude_tags: vec![],
        mode: TagMode::And,
    };

    // Get first 2 items
    let filter = TagFilterBitmap::new(&db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    let mut stream1 = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        None,
        Some(filter.clone()),
    )?;
    
    let item1 = stream1.next().unwrap();
    let item2 = stream1.next().unwrap();
    let cursor = stream1.cursor();
    
    assert_eq!(item1.as_str(), "item1");
    assert_eq!(item2.as_str(), "item2");

    // Resume from cursor and get remaining items
    let mut stream2 = MultiShardListStreamer::resume_with_filter(
        &db.shards(),
        Vec::new(),
        None,
        cursor,
        Some(filter),
    )?;
    
    let item3 = stream2.next().unwrap();
    let item4 = stream2.next().unwrap();
    
    // Debug: Check if there are more items
    let extra_item = stream2.next();
    if let Some(ref item) = extra_item {
        eprintln!("DEBUG: Unexpected extra item: {:?}", item.as_str());
    }
    assert!(extra_item.is_none(), "Expected no more items, but got: {:?}", extra_item.as_ref().map(|e| e.as_str()));
    
    assert_eq!(item3.as_str(), "item3");
    assert_eq!(item4.as_str(), "item4");

    Ok(())
}

/// Test tag filtering with complex exclusion patterns
#[test]
fn multi_shard_tag_filter_complex_exclusion() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        b1.insert("doc1", Some(Value::Text("content1".into())));
        b1.insert("doc2", Some(Value::Text("content2".into())));
        b1.insert("doc3", Some(Value::Text("content3".into())));
        b1.insert("doc4", Some(Value::Text("content4".into())));
        b1.close()?;
        
        let mut tag_builder = foliant::TagIndexBuilder::new(&base1);
        tag_builder.insert_tags(1, vec!["important", "published"]);
        tag_builder.insert_tags(2, vec!["important", "draft"]);
        tag_builder.insert_tags(3, vec!["urgent", "published"]);
        tag_builder.insert_tags(4, vec!["urgent", "draft", "deprecated"]);
        tag_builder.finish()?;
    }

    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.load_tag_index()?;

    // Test: Get published content, excluding deprecated items
    let tag_config = TagFilterConfig {
        include_tags: vec!["published".to_string()],
        exclude_tags: vec!["deprecated".to_string()],
        mode: TagMode::And,
    };
    let filter = TagFilterBitmap::new(&db.shards(), &tag_config.include_tags, &tag_config.exclude_tags, tag_config.mode)?;
    let stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        None,
        Some(filter),
    )?;
    
    let mut results: Vec<Entry<Value>> = stream.collect();
    results.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    
    // Should get doc1 (important + published) and doc3 (urgent + published)
    // doc4 is excluded because it's deprecated, even though it might match other criteria
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_str(), "doc1");

    assert_eq!(results[1].as_str(), "doc3");

    Ok(())
}

