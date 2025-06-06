use foliant::{Database, DatabaseBuilder, Entry, Streamer};
use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn multi_shard_listing_complex() -> Result<(), Box<dyn Error>> {
    // Create a temp directory with two shards
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Shard 1
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        b1.insert("alpha/a", None);
        b1.insert("alpha/b", None);
        b1.insert("beta/x", None);
        b1.close()?;
    }
    // Shard 2
    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        // Keys must be inserted in lex order
        b2.insert("alpha/b", None);
        b2.insert("delta", None);
        b2.insert("gamma/z", None);
        b2.close()?;
    }

    // Load shards into a new database
    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;

    // 1) No delimiter: merge and list all keys with first-shard precedence
    let mut all: Vec<Entry> = db.list("", None)?.collect();
    all.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let mut expected = vec![
        Entry::Key("alpha/a".to_string(), 0, None),
        Entry::Key("alpha/b".to_string(), 0, None),
        Entry::Key("beta/x".to_string(), 0, None),
        Entry::Key("delta".to_string(), 0, None),
        Entry::Key("gamma/z".to_string(), 0, None),
    ];
    expected.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert_eq!(all, expected);

    // 2) Top-level delimiter grouping on '/'
    let mut grp: Vec<Entry> = db.list("", Some('/'))?.collect();
    grp.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let mut exp_grp = vec![
        Entry::CommonPrefix("alpha/".to_string()),
        Entry::CommonPrefix("beta/".to_string()),
        Entry::CommonPrefix("gamma/".to_string()),
        // 'delta' has no slash, so appears as a key
        Entry::Key("delta".to_string(), 0, None),
    ];
    exp_grp.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert_eq!(grp, exp_grp);
    Ok(())
}

/// Test full multi-shard listing including values and delimiter grouping.
#[test]
fn multi_shard_listing_complex_with_values() -> Result<(), Box<dyn Error>> {
    // Create a temp directory with two shards
    let dir = tempdir()?;
    let db_dir = dir.path().join("db");
    std::fs::create_dir(&db_dir)?;

    // Shard 1
    {
        let base1 = db_dir.join("shard1.idx");
        let mut b1 = DatabaseBuilder::<Value>::new(&base1, PAYLOAD_STORE_VERSION_V3)?;
        b1.insert("alpha/a", Some(Value::Integer(1)));
        b1.insert("alpha/b", Some(Value::Integer(2)));
        b1.insert("beta/x", Some(Value::Text("x".into())));
        b1.close()?;
    }
    // Shard 2
    {
        let base2 = db_dir.join("shard2.idx");
        let mut b2 = DatabaseBuilder::<Value>::new(&base2, PAYLOAD_STORE_VERSION_V3)?;
        // Keys must be inserted in lex order
        b2.insert("alpha/b", Some(Value::Integer(20)));
        b2.insert(
            "delta",
            Some(Value::Array(vec![Value::Integer(3), Value::Integer(4)])),
        );
        b2.insert("gamma/z", Some(Value::Bool(true)));
        b2.close()?;
    }

    // Load shards into a new database
    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("shard1.idx"))?;
    db.add_shard(&db_dir.join("shard2.idx"))?;

    // 1) No delimiter: merge and list all keys with first-shard precedence
    let mut all: Vec<Entry> = db.list("", None)?.collect();
    all.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let mut expected = vec![
        Entry::Key("alpha/a".to_string(), 0, Some(Value::Integer(1))),
        Entry::Key("alpha/b".to_string(), 0, Some(Value::Integer(2))),
        Entry::Key("beta/x".to_string(), 0, Some(Value::Text("x".into()))),
        Entry::Key(
            "delta".to_string(),
            0,
            Some(Value::Array(vec![Value::Integer(3), Value::Integer(4)])),
        ),
        Entry::Key("gamma/z".to_string(), 0, Some(Value::Bool(true))),
    ];
    expected.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert_eq!(all, expected);

    // 2) Top-level delimiter grouping on '/'
    let mut grp: Vec<Entry> = db.list("", Some('/'))?.collect();
    grp.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let mut exp_grp = vec![
        Entry::CommonPrefix("alpha/".to_string()),
        Entry::CommonPrefix("beta/".to_string()),
        Entry::CommonPrefix("gamma/".to_string()),
        // 'delta' has no slash, so appears as a key
        Entry::Key(
            "delta".to_string(),
            0,
            Some(Value::Array(vec![Value::Integer(3), Value::Integer(4)])),
        ),
    ];
    exp_grp.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert_eq!(grp, exp_grp);
    Ok(())
}

/// Test multi-shard listing with deeper prefixes and delimiter grouping
#[test]
fn multi_shard_listing_deep_prefix() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db5");
    std::fs::create_dir(&db_dir)?;

    // Shard 1: nested under foo/a and foo/b
    {
        let base = db_dir.join("s1.idx");
        let mut b = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
        b.insert("foo/a/1", None);
        b.insert("foo/a/2", None);
        b.insert("foo/b/1", None);
        b.close()?;
    }
    // Shard 2: some overlap and deeper path
    {
        let base = db_dir.join("s2.idx");
        let mut b = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
        b.insert("foo/a/3/x", None);
        b.insert("foo/c/1", None);
        b.close()?;
    }

    // Load shards into a new database
    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("s1.idx"))?;
    db.add_shard(&db_dir.join("s2.idx"))?;
    // List under "foo/" (one-level prefix) with grouping at '/'
    let mut lvl1: Vec<Entry> = db.list("foo/", Some('/'))?.collect();
    lvl1.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let exp1 = vec![
        Entry::CommonPrefix("foo/a/".to_string()),
        Entry::CommonPrefix("foo/b/".to_string()),
        Entry::CommonPrefix("foo/c/".to_string()),
    ];
    assert_eq!(lvl1, exp1);

    // List under "foo/a/" (deeper prefix) with grouping
    let mut lvl2: Vec<Entry> = db.list("foo/a/", Some('/'))?.collect();
    lvl2.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    // Leaves for 1 & 2, then grouping for deeper subtree at 'foo/a/3/'
    let exp2 = vec![
        Entry::Key("foo/a/1".to_string(), 0, None),
        Entry::Key("foo/a/2".to_string(), 0, None),
        Entry::CommonPrefix("foo/a/3/".to_string()),
    ];
    assert_eq!(lvl2, exp2);
    Ok(())
}

/// Test multi-shard listing when all values are None (no payload)
#[test]
fn multi_shard_listing_none_values() -> Result<(), Box<dyn Error>> {
    // Create a temp directory with two shards
    let dir = tempdir()?;
    let db_dir = dir.path().join("db2");
    std::fs::create_dir(&db_dir)?;

    // Shard A
    {
        let base = db_dir.join("A.idx");
        let mut b = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
        b.insert("x/1", None);
        b.insert("x/2", None);
        b.insert("y/a", None);
        b.close()?;
    }
    // Shard B
    {
        let base = db_dir.join("B.idx");
        let mut b = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
        // keys in sorted order
        b.insert("x/2", None);
        b.insert("y/b", None);
        b.insert("z", None);
        b.close()?;
    }

    // Load shards into a new database
    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("A.idx"))?;
    db.add_shard(&db_dir.join("B.idx"))?;

    // No delimiter: list all keys, first-shard wins for duplicates
    let mut all: Vec<Entry> = db.list("", None)?.collect();
    all.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let expected = vec![
        Entry::Key("x/1".to_string(), 0, None),
        Entry::Key("x/2".to_string(), 0, None),
        Entry::Key("y/a".to_string(), 0, None),
        Entry::Key("y/b".to_string(), 0, None),
        Entry::Key("z".to_string(), 0, None),
    ];
    assert_eq!(all, expected);

    // With delimiter: group at '/', expect prefixes and standalone z
    let mut grp: Vec<Entry> = db.list("", Some('/'))?.collect();
    grp.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let expected_grp = vec![
        Entry::CommonPrefix("x/".to_string()),
        Entry::CommonPrefix("y/".to_string()),
        Entry::Key("z".to_string(), 0, None),
    ];
    assert_eq!(grp, expected_grp);
    Ok(())
}

/// Test multi-shard listing with a non-empty prefix and no delimiter
#[test]
fn multi_shard_listing_with_prefix() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_dir = dir.path().join("db3");
    std::fs::create_dir(&db_dir)?;

    // Shard 1 (keys in lex order)
    {
        let base = db_dir.join("sh1.idx");
        let mut b = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
        b.insert("bar/1", Some(Value::Integer(1)));
        b.insert("foo/a", Some(Value::Integer(10)));
        b.close()?;
    }
    // Shard 2 (keys in lex order)
    {
        let base = db_dir.join("sh2.idx");
        let mut b = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
        b.insert("baz/x", Some(Value::Integer(2)));
        b.insert("foo/b", Some(Value::Integer(20)));
        b.close()?;
    }

    // Load shards into a new database
    let mut db = Database::<Value>::new();
    db.add_shard(&db_dir.join("sh1.idx"))?;
    db.add_shard(&db_dir.join("sh2.idx"))?;
    // List only keys under "foo"
    let mut list: Vec<Entry> = db.list("foo", None)?.collect();
    list.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let expected = vec![
        Entry::Key("foo/a".to_string(), 0, Some(Value::Integer(10))),
        Entry::Key("foo/b".to_string(), 0, Some(Value::Integer(20))),
    ];
    assert_eq!(list, expected);
    Ok(())
}
