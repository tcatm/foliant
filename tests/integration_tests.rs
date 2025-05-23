use foliant::{DatabaseBuilder, Database, Entry};
use foliant::Streamer;
use tempfile::tempdir;
use std::error::Error;
use serde_cbor::Value as Value;

// SimpleList: insert distinct keys and verify list("", None)
#[test]
fn simple_list() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    let keys = ["alpha", "beta", "gamma"];
    for &k in &keys {
        builder.insert(k, None);
    }
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("", None).unwrap().collect();
    list.sort();
    let mut expected: Vec<Entry> = keys.iter().map(|&s| Entry::Key(s.to_string(), None)).collect();
    expected.sort();
    assert_eq!(list, expected);
    Ok(())
}

// DelimiterGrouping: group on '/'
#[test]
fn delimiter_grouping() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    let paths = ["foo/bar", "foo/baz", "foobar"];
    for &p in &paths {
        builder.insert(p, None);
    }
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("foo", Some('/')).unwrap().collect();
    list.sort();
    let expected = vec![
        Entry::CommonPrefix("foo/".to_string()),
        Entry::Key("foobar".to_string(), None),
    ];
    assert_eq!(list, expected);
    Ok(())
}

// EmptyKey: inserting empty string
#[test]
fn empty_key() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("", None);
    let db = builder.into_database()?;
    let list: Vec<Entry> = db.list("", None).unwrap().collect();
    assert_eq!(list, vec![Entry::Key("".to_string(), None)]);
    Ok(())
}

// PrefixSplit: keys "test" and "team"
#[test]
fn prefix_split() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("team", None);
    builder.insert("test", None);
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("te", None).unwrap().collect();
    list.sort();
    let mut expected = vec![
        Entry::Key("test".to_string(), None),
        Entry::Key("team".to_string(), None),
    ];
    expected.sort();
    assert_eq!(list, expected);
    Ok(())
}

// MidEdgePrefix: prefix falls mid-edge
#[test]
fn mid_edge_prefix() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("abcde", None);
    builder.insert("abcdx", None);
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("abc", None).unwrap().collect();
    list.sort();
    let mut expected = vec![
        Entry::Key("abcde".to_string(), None),
        Entry::Key("abcdx".to_string(), None),
    ];
    expected.sort();
    assert_eq!(list, expected);
    Ok(())
}

// UnicodeKeys: multi-byte prefixes
#[test]
fn unicode_keys() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    let words = ["こん", "こんにちは", "こんばんは"];
    for &w in &words {
        builder.insert(w, None);
    }
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("こん", None).unwrap().collect();
    list.sort();
    let mut expected: Vec<Entry> = words.iter().map(|&s| Entry::Key(s.to_string(), None)).collect();
    expected.sort();
    assert_eq!(list, expected);
    Ok(())
}

// InMemoryPayloadRoundtrip: builder into_database returns mmap DB
#[test]
fn in_memory_payload_roundtrip() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("alpha", Some(Value::Integer(42)));
    builder.insert("beta", Some(Value::Text("hello".into())));
    builder.insert("gamma", None);
    let db = builder.into_database()?;
    assert_eq!(db.get_value("alpha")?, Some(Value::Integer(42)));
    assert_eq!(db.get_value("beta")?, Some(Value::Text("hello".into())));
    assert_eq!(db.get_value("gamma")?, None);
    assert_eq!(db.get_value("delta")?, None);
    Ok(())
}

// MmapPayloadAccess: various CBOR types
#[test]
fn mmap_payload_access() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("x", Some(Value::Bool(true)));
    builder.insert("y", Some(Value::Array(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ])));
    let db = builder.into_database()?;
    assert_eq!(db.get_value("x")?, Some(Value::Bool(true)));
    assert_eq!(db.get_value("y")?, Some(Value::Array(vec![
        Value::Integer(1), Value::Integer(2), Value::Integer(3)
    ])));
    assert_eq!(db.get_value("z")?, None);
    Ok(())
}

// SerializeVsMmapListing: list direct vs reopened
#[test]
fn serialize_vs_mmap_listing() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    let keys = ["alpha", "beta", "gamma/delta", "gamma/epsilon"];
    for &k in &keys {
        builder.insert(k, None);
    }
    let db1 = builder.into_database()?;
    let db2 = Database::<Value>::open(&base)?;
    let mut l1: Vec<Entry> = db1.list("", None).unwrap().collect();
    let mut l2: Vec<Entry> = db2.list("", None).unwrap().collect();
    l1.sort(); l2.sort();
    assert_eq!(l1, l2);
    Ok(())
}

// RoundtripPrefixes: check multiple prefixes
#[test]
fn roundtrip_prefixes() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    let keys = ["", "a", "ab", "abc", "companion", "compression"];
    for &k in &keys {
        builder.insert(k, None);
    }
    let db1 = builder.into_database()?;
    let db2 = Database::<Value>::open(&base)?;
    for &pref in &["", "a", "ab", "comp"] {
        let mut r1: Vec<Entry> = db1.list(pref, Some('/')).unwrap().collect();
        let mut r2: Vec<Entry> = db2.list(pref, Some('/')).unwrap().collect();
        r1.sort(); r2.sort();
        assert_eq!(r1, r2);
    }
    Ok(())
}

// UTF8DelimiterListing: test multi-listing a key with multi-byte UTF-8 works without panic
#[test]
fn multi_list_utf8_truncate() {
    let dir = tempdir().unwrap();
    let base = dir.path().join("testdb");
    let mut builder = DatabaseBuilder::<Value>::new(&base).unwrap();
    builder.insert("é", None);
    builder.close().unwrap();
    let db: Database<Value> = Database::open(&base).unwrap();
    let mut stream = db.list("", Some('/')).unwrap();
    let first = stream.next().unwrap();
    assert_eq!(first.as_str(), "é");
    assert!(stream.next().is_none());
}