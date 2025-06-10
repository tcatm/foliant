use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, Streamer, TagIndexBuilder, TagMode};
use serde_json::{json, Value};
use std::fs;
use tempfile::tempdir;

#[test]
fn list_by_tags_basic() -> Result<(), Box<dyn std::error::Error>> {
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

    let or_list = db_list_tags(&db, &["red", "yellow"], TagMode::Or, None);
    let expected_or = vec![
        "apple".to_string(),
        "banana".to_string(),
        "cherry".to_string(),
    ];
    assert_eq!(or_list, expected_or);

    let and_list = db_list_tags(&db, &["fruit", "red"], TagMode::And, None);
    let expected_and = vec!["apple".to_string(), "cherry".to_string()];
    assert_eq!(and_list, expected_and);

    let prefix_list = db_list_tags(&db, &["fruit"], TagMode::And, Some("c"));
    let expected_prefix = vec!["cherry".to_string()];
    assert_eq!(prefix_list, expected_prefix);

    Ok(())
}

#[test]
fn list_by_tags_empty_tags() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_empty.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("a", Some(json!({"tags":["tag"]})));
    builder.close()?;
    let mut db_builder = Database::<Value>::open(&base)?;
    TagIndexBuilder::build_index(&mut db_builder, "tags", None)?;
    let db: Database<Value> = Database::open(&base)?;
    assert!(db_list_tags(&db, &[], TagMode::Or, None).is_empty());
    assert!(db_list_tags(&db, &[], TagMode::And, None).is_empty());
    Ok(())
}

#[test]
fn list_by_tags_unknown_tags() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_unknown.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("a", Some(json!({"tags":["t1"]})));
    builder.insert("b", Some(json!({"tags":["t2"]})));
    builder.close()?;
    let mut db_builder = Database::<Value>::open(&base)?;
    TagIndexBuilder::build_index(&mut db_builder, "tags", None)?;
    let db: Database<Value> = Database::open(&base)?;
    assert!(db_list_tags(&db, &["nope"], TagMode::Or, None).is_empty());
    assert!(db_list_tags(&db, &["nope"], TagMode::And, None).is_empty());
    Ok(())
}

#[test]
fn list_by_tags_no_overlap_and() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_no_overlap.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("a", Some(json!({"tags":["x"]})));
    builder.insert("b", Some(json!({"tags":["y"]})));
    builder.close()?;
    let mut db_builder = Database::<Value>::open(&base)?;
    TagIndexBuilder::build_index(&mut db_builder, "tags", None)?;
    let db: Database<Value> = Database::open(&base)?;
    assert!(db_list_tags(&db, &["x", "y"], TagMode::And, None).is_empty());
    Ok(())
}

#[test]
fn list_by_tags_with_values() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_values.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("x", Some(json!({"tags":["a"],"value":1})));
    builder.insert("y", Some(json!({"tags":["b"],"value":2})));
    builder.insert("z", Some(json!({"tags":["a","b"],"value":3})));
    builder.close()?;
    let mut db_builder = Database::<Value>::open(&base)?;
    TagIndexBuilder::build_index(&mut db_builder, "tags", None)?;
    let db: Database<Value> = Database::open(&base)?;
    let entries: Vec<Entry<Value>> = db.list_by_tags(&["a"], &[], TagMode::Or, None)?.collect();
    let mut kvs: Vec<(String, Option<Value>)> = entries
        .into_iter()
        .filter_map(|entry| match entry {
            Entry::Key(s, _, v) => Some((s, v)),
            _ => None,
        })
        .collect();
    kvs.sort_by(|a, b| a.0.cmp(&b.0));
    let expected = vec![
        ("x".to_string(), Some(json!({"tags":["a"],"value":1}))),
        ("z".to_string(), Some(json!({"tags":["a","b"],"value":3}))),
    ];
    assert_eq!(kvs, expected);
    Ok(())
}

#[test]
fn list_by_tags_multi_shard() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base_dir = dir.path().join("db_multi");
    fs::create_dir(&base_dir)?;
    {
        let mut b =
            DatabaseBuilder::<Value>::new(&base_dir.join("s1.idx"), PAYLOAD_STORE_VERSION_V3)?;
        b.insert("foo", Some(json!({"tags":["t1"]})));
        b.insert("bar", Some(json!({"tags":["t1","t2"]})));
        b.close()?;
    }
    {
        let mut b =
            DatabaseBuilder::<Value>::new(&base_dir.join("s2.idx"), PAYLOAD_STORE_VERSION_V3)?;
        b.insert("baz", Some(json!({"tags":["t2"]})));
        b.insert("qux", Some(json!({"tags":["t3"]})));
        b.close()?;
    }
    let mut db_builder = Database::<Value>::new();
    db_builder.add_shard(&base_dir.join("s1.idx"))?;
    db_builder.add_shard(&base_dir.join("s2.idx"))?;
    TagIndexBuilder::build_index(&mut db_builder, "tags", None)?;
    let mut db = Database::<Value>::new();
    db.add_shard(&base_dir.join("s1.idx"))?;
    db.add_shard(&base_dir.join("s2.idx"))?;
    let or_list = db_list_tags(&db, &["t1", "t2"], TagMode::Or, None);
    assert_eq!(
        or_list,
        vec!["bar".to_string(), "baz".to_string(), "foo".to_string()]
    );
    let and = db_list_tags(&db, &["t1", "t2"], TagMode::And, None);
    assert_eq!(and, vec!["bar".to_string()]);
    let or_pref = db_list_tags(&db, &["t1", "t2"], TagMode::Or, Some("b"));
    assert_eq!(or_pref, vec!["bar".to_string(), "baz".to_string()]);
    Ok(())
}

#[test]
fn list_by_tags_merge_reorder_tags() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_tags_merge.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    const TOTAL: usize = 10_001;
    for i in 0..TOTAL {
        let key = format!("k{:05}", TOTAL - i);
        let tag = if i % 2 == 0 { "even" } else { "odd" };
        builder.insert(&key, Some(json!({"tags":[tag]})));
    }
    builder.flush_fst()?;
    builder.close()?;
    let mut db_builder = Database::<Value>::open(&base)?;
    TagIndexBuilder::build_index(&mut db_builder, "tags", None)?;

    let db: Database<Value> = Database::open(&base)?;
    let even_keys = db_list_tags(&db, &["even"], TagMode::Or, None);
    let mut expected_even = Vec::new();
    for n in (1..=TOTAL).step_by(2) {
        expected_even.push(format!("k{:05}", n));
    }
    assert_eq!(even_keys, expected_even);

    let odd_keys = db_list_tags(&db, &["odd"], TagMode::Or, None);
    let mut expected_odd = Vec::new();
    for n in (2..=TOTAL).step_by(2) {
        expected_odd.push(format!("k{:05}", n));
    }
    assert_eq!(odd_keys, expected_odd);

    Ok(())
}

#[test]
fn list_by_tags_merge_reorder_image_text() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_tags_merge_image_text.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    // 1. Insert a large number of Image+English-tagged entries, then flush to create segment #1.
    const IMAGE_COUNT: usize = 10_000;
    for i in 1..=IMAGE_COUNT {
        if i % 2 == 0 {
            builder.insert(
                &format!("img{:05}", i),
                Some(json!({"tags":["Image","English"]})),
            );
        } else {
            builder.insert(
                &format!("img{:05}", i),
                Some(json!({"tags":["English","Image"]})),
            );
        }
    }
    builder.flush_fst()?;

    // 2. Insert a small number of Structured Data+Text-tagged entries, then close (segment #2).
    const TEXT_COUNT: usize = 3;
    for i in 1..=TEXT_COUNT {
        if i % 2 == 0 {
            builder.insert(
                &format!("txt{:02}", i),
                Some(json!({"tags":["Text","Structured Data"]})),
            );
        } else {
            builder.insert(
                &format!("txt{:02}", i),
                Some(json!({"tags":["Structured Data","Text"]})),
            );
        }
    }
    builder.close()?;
    let mut db_builder = Database::<Value>::open(&base)?;
    TagIndexBuilder::build_index(&mut db_builder, "tags", None)?;

    let db: Database<Value> = Database::open(&base)?;
    // Single-tag queries should return exactly the respective keys.
    let image_keys = db_list_tags(&db, &["Image"], TagMode::Or, None);
    let expected_image: Vec<String> = (1..=IMAGE_COUNT).map(|i| format!("img{:05}", i)).collect();
    assert_eq!(image_keys, expected_image);

    let text_keys = db_list_tags(&db, &["Text"], TagMode::Or, None);
    let expected_text: Vec<String> = (1..=TEXT_COUNT).map(|i| format!("txt{:02}", i)).collect();
    assert_eq!(text_keys, expected_text);

    // AND queries for multi-tag entries must also return exactly those same keys.
    let image_eng_keys = db_list_tags(&db, &["Image", "English"], TagMode::And, None);
    assert_eq!(image_eng_keys, expected_image);

    let text_struct_keys = db_list_tags(&db, &["Text", "Structured Data"], TagMode::And, None);
    assert_eq!(text_struct_keys, expected_text);

    // OR query should return the union of both sets.
    let mut expected_both = expected_image.clone();
    expected_both.extend(expected_text.clone());
    expected_both.sort();
    let both_keys = db_list_tags(&db, &["Image", "Text"], TagMode::Or, None);
    assert_eq!(both_keys, expected_both);

    // AND query for disjoint tags should be empty.
    let and_keys = db_list_tags(&db, &["Image", "Text"], TagMode::And, None);
    assert!(and_keys.is_empty());

    Ok(())
}
#[test]
fn build_tag_index_single_entry_no_tags() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_single_no_tags.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("a", Some(json!({"tags":[]})));
    builder.close()?;
    let mut db_builder = Database::<Value>::open(&base)?;
    TagIndexBuilder::build_index(&mut db_builder, "tags", None)?;
    let db: Database<Value> = Database::open(&base)?;
    let tags: Vec<(String, usize)> = db.list_tags(None)?.collect();
    assert!(tags.is_empty());
    Ok(())
}

fn db_list_tags(
    db: &Database<Value>,
    tags: &[&str],
    mode: TagMode,
    prefix: Option<&str>,
) -> Vec<String> {
    let entries: Vec<Entry<Value>> = db.list_by_tags(tags, &[], mode, prefix).unwrap().collect();
    let mut res: Vec<String> = entries
        .into_iter()
        .filter_map(|entry| match entry {
            Entry::Key(s, _, _) => Some(s),
            _ => None,
        })
        .collect();
    res.sort();
    res
}
