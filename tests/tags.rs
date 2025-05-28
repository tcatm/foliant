use foliant::{Database, DatabaseBuilder, Entry, Streamer, TagMode};
use serde_cbor::Value;
use tempfile::tempdir;
use std::fs;

#[test]
fn list_by_tags_basic() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert_ext("apple", None, vec!["fruit", "red"]);
    builder.insert_ext("banana", None, vec!["fruit", "yellow"]);
    builder.insert_ext("cherry", None, vec!["fruit", "red"]);
    builder.insert_ext("date", None, vec!["fruit", "brown"]);
    builder.insert_ext("eggplant", None, vec!["vegetable", "purple"]);
    builder.close()?;

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
    let base = dir.path().join("db_empty");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert_ext("a", None, vec!["tag"]);
    builder.close()?;
    let db: Database<Value> = Database::open(&base)?;
    assert!(db_list_tags(&db, &[], TagMode::Or, None).is_empty());
    assert!(db_list_tags(&db, &[], TagMode::And, None).is_empty());
    Ok(())
}

#[test]
fn list_by_tags_unknown_tags() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_unknown");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert_ext("a", None, vec!["t1"]);
    builder.insert_ext("b", None, vec!["t2"]);
    builder.close()?;
    let db: Database<Value> = Database::open(&base)?;
    assert!(db_list_tags(&db, &["nope"], TagMode::Or, None).is_empty());
    assert!(db_list_tags(&db, &["nope"], TagMode::And, None).is_empty());
    Ok(())
}

#[test]
fn list_by_tags_no_overlap_and() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_no_overlap");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert_ext("a", None, vec!["x"]);
    builder.insert_ext("b", None, vec!["y"]);
    builder.close()?;
    let db: Database<Value> = Database::open(&base)?;
    assert!(db_list_tags(&db, &["x", "y"], TagMode::And, None).is_empty());
    Ok(())
}

#[test]
fn list_by_tags_with_values() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_values");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert_ext("x", Some(Value::Integer(1)), vec!["a"]);
    builder.insert_ext("y", Some(Value::Integer(2)), vec!["b"]);
    builder.insert_ext("z", Some(Value::Integer(3)), vec!["a", "b"]);
    builder.close()?;
    let db: Database<Value> = Database::open(&base)?;
    let entries: Vec<Entry<Value>> = db.list_by_tags(&["a"], TagMode::Or, None)?.collect();
    let mut kvs: Vec<(String, Option<Value>)> = entries
        .into_iter()
        .filter_map(|entry| match entry {
            Entry::Key(s, _, v) => Some((s, v)),
            _ => None,
        })
        .collect();
    kvs.sort_by(|a, b| a.0.cmp(&b.0));
    let expected = vec![
        ("x".to_string(), Some(Value::Integer(1))),
        ("z".to_string(), Some(Value::Integer(3))),
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
        let mut b = DatabaseBuilder::<Value>::new(&base_dir.join("s1"))?;
        b.insert_ext("foo", None, vec!["t1"]);
        b.insert_ext("bar", None, vec!["t1", "t2"]);
        b.close()?;
    }
    {
        let mut b = DatabaseBuilder::<Value>::new(&base_dir.join("s2"))?;
        b.insert_ext("baz", None, vec!["t2"]);
        b.insert_ext("qux", None, vec!["t3"]);
        b.close()?;
    }
    let db: Database<Value> = Database::open(&base_dir)?;
    let or_list = db_list_tags(&db, &["t1", "t2"], TagMode::Or, None);
    assert_eq!(or_list, vec!["bar".to_string(), "baz".to_string(), "foo".to_string()]);
    let and = db_list_tags(&db, &["t1", "t2"], TagMode::And, None);
    assert_eq!(and, vec!["bar".to_string()]);
    let or_pref = db_list_tags(&db, &["t1", "t2"], TagMode::Or, Some("b"));
    assert_eq!(or_pref, vec!["bar".to_string(), "baz".to_string()]);
    Ok(())
}

fn db_list_tags(
    db: &Database<Value>,
    tags: &[&str],
    mode: TagMode,
    prefix: Option<&str>,
) -> Vec<String> {
    let entries: Vec<Entry<Value>> = db.list_by_tags(tags, mode, prefix).unwrap().collect();
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
