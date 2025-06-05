use foliant::Streamer;
use foliant::{Database, DatabaseBuilder, Entry};
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

// SimpleList: insert distinct keys and verify list("", None)
#[test]
fn simple_list() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    let keys = ["alpha", "beta", "gamma"];
    for &k in &keys {
        builder.insert(k, None);
    }
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("", None).unwrap().collect();
    list.sort();
    let mut expected: Vec<Entry> = keys
        .iter()
        .map(|&s| Entry::Key(s.to_string(), 0, None))
        .collect();
    expected.sort();
    assert_eq!(list, expected);
    Ok(())
}

// IgnoreDuplicates: inserting duplicate keys with ignore_duplicates skips extra entries
#[test]
fn ignore_duplicates_integration() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_ignore.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.ignore_duplicates();
    builder.insert("key1", None);
    builder.insert("key1", None);
    builder.insert("key2", None);
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("", None).unwrap().collect();
    list.sort();
    let expected = vec![
        Entry::Key("key1".to_string(), 0, None),
        Entry::Key("key2".to_string(), 0, None),
    ];
    assert_eq!(list, expected);
    Ok(())
}

// DelimiterGrouping: group on '/'
#[test]
fn delimiter_grouping() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
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
        Entry::Key("foobar".to_string(), 0, None),
    ];
    assert_eq!(list, expected);
    Ok(())
}

// EmptyKey: inserting empty string
#[test]
fn empty_key() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("", None);
    let db = builder.into_database()?;
    let list: Vec<Entry> = db.list("", None).unwrap().collect();
    assert_eq!(list, vec![Entry::Key("".to_string(), 0, None)]);
    Ok(())
}

// PrefixSplit: keys "test" and "team"
#[test]
fn prefix_split() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("team", None);
    builder.insert("test", None);
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("te", None).unwrap().collect();
    list.sort();
    let mut expected = vec![
        Entry::Key("test".to_string(), 0, None),
        Entry::Key("team".to_string(), 0, None),
    ];
    expected.sort();
    assert_eq!(list, expected);
    Ok(())
}

// MidEdgePrefix: prefix falls mid-edge
#[test]
fn mid_edge_prefix() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("abcde", None);
    builder.insert("abcdx", None);
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("abc", None).unwrap().collect();
    list.sort();
    let mut expected = vec![
        Entry::Key("abcde".to_string(), 0, None),
        Entry::Key("abcdx".to_string(), 0, None),
    ];
    expected.sort();
    assert_eq!(list, expected);
    Ok(())
}

// UnicodeKeys: multi-byte prefixes
#[test]
fn unicode_keys() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    let words = ["こん", "こんにちは", "こんばんは"];
    for &w in &words {
        builder.insert(w, None);
    }
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("こん", None).unwrap().collect();
    list.sort();
    let mut expected: Vec<Entry> = words
        .iter()
        .map(|&s| Entry::Key(s.to_string(), 0, None))
        .collect();
    expected.sort();
    assert_eq!(list, expected);
    Ok(())
}

// InMemoryPayloadRoundtrip: builder into_database returns mmap DB
#[test]
fn in_memory_payload_roundtrip() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
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
    let base = dir.path().join("db.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("x", Some(Value::Bool(true)));
    builder.insert(
        "y",
        Some(Value::Array(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ])),
    );
    let db = builder.into_database()?;
    assert_eq!(db.get_value("x")?, Some(Value::Bool(true)));
    assert_eq!(
        db.get_value("y")?,
        Some(Value::Array(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3)
        ]))
    );
    assert_eq!(db.get_value("z")?, None);
    Ok(())
}

// SerializeVsMmapListing: list direct vs reopened
#[test]
fn serialize_vs_mmap_listing() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    let keys = ["alpha", "beta", "gamma/delta", "gamma/epsilon"];
    for &k in &keys {
        builder.insert(k, None);
    }
    let db1 = builder.into_database()?;
    let db2 = Database::<Value>::open(&base)?;
    let mut l1: Vec<Entry> = db1.list("", None).unwrap().collect();
    let mut l2: Vec<Entry> = db2.list("", None).unwrap().collect();
    l1.sort();
    l2.sort();
    assert_eq!(l1, l2);
    Ok(())
}

// RoundtripPrefixes: check multiple prefixes
#[test]
fn roundtrip_prefixes() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
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
        r1.sort();
        r2.sort();
        assert_eq!(r1, r2);
    }
    Ok(())
}

// UTF8DelimiterListing: test multi-listing a key with multi-byte UTF-8 works without panic
#[test]
fn multi_list_utf8_truncate() {
    let dir = tempdir().unwrap();
    let base = dir.path().join("testdb.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base).unwrap();
    builder.insert("é", None);
    builder.close().unwrap();
    let db: Database<Value> = Database::open(&base).unwrap();
    let mut stream = db.list("", Some('/')).unwrap();
    let first = stream.next().unwrap();
    assert_eq!(first.as_str(), "é");
    assert!(stream.next().is_none());
}

#[test]
fn batching_and_sorted() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("d", None);
    builder.insert("a", None);
    builder.insert("c", None);
    builder.insert("b", None);
    let db = builder.into_database()?;
    let list: Vec<Entry> = db.list("", None).unwrap().collect();
    let keys: Vec<String> = list.into_iter().map(|e| e.as_str().to_string()).collect();
    assert_eq!(keys, vec!["a", "b", "c", "d"]);
    Ok(())
}

#[test]
fn batching_and_sorted_with_values() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_values.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("d", Some(Value::Integer(1)));
    builder.insert("a", Some(Value::Text("alpha".into())));
    builder.insert("c", Some(Value::Bool(true)));
    builder.insert(
        "b",
        Some(Value::Array(vec![Value::Integer(2), Value::Integer(3)])),
    );
    let db = builder.into_database()?;
    let list: Vec<Entry> = db.list("", None).unwrap().collect();
    let keys: Vec<String> = list.into_iter().map(|e| e.as_str().to_string()).collect();
    assert_eq!(keys, vec!["a", "b", "c", "d"]);
    assert_eq!(db.get_value("a")?, Some(Value::Text("alpha".into())));
    assert_eq!(
        db.get_value("b")?,
        Some(Value::Array(vec![Value::Integer(2), Value::Integer(3)]))
    );
    assert_eq!(db.get_value("c")?, Some(Value::Bool(true)));
    assert_eq!(db.get_value("d")?, Some(Value::Integer(1)));
    Ok(())
}

#[test]
fn get_key_basic() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_get_key_basic.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("foo", Some(Value::Integer(1)));
    builder.insert("bar", Some(Value::Text("two".into())));
    builder.insert("baz", Some(Value::Bool(true)));
    let db = builder.into_database()?;
    let entries: Vec<Entry> = db.list("", None).unwrap().collect();
    for entry in &entries {
        let ptr = entry.ptr().expect("entry should have a ptr");
        eprintln!("Checking entry: {:?} with ptr {}", entry, ptr);
        // Check that get_key maps the pointer back to the correct key
        let got = db.get_key(ptr)?;
        assert_eq!(
            got,
            Some(entry.clone()),
            "get_key returned {:?} for entry '{}' ptr {}",
            got,
            entry.as_str(),
            ptr
        );
    }
    Ok(())
}

#[test]
fn get_key_with_new_fst_segment() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_get_key_new_fst.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    // First segment: foo and bar
    builder.insert("foo", Some(Value::Integer(1)));
    builder.insert("bar", Some(Value::Text("two".into())));
    builder.flush_fst()?;
    // Second segment: baz and qux
    builder.insert("baz", Some(Value::Bool(true)));
    builder.insert("qux", Some(Value::Array(vec![Value::Integer(3)])));
    let db = builder.into_database()?;
    let entries: Vec<Entry> = db.list("", None).unwrap().collect();
    for entry in &entries {
        let ptr = entry.ptr().expect("entry should have a ptr");
        // Check that get_key maps the pointer back to the correct key, even across segments
        let got = db.get_key(ptr)?;
        assert_eq!(
            got,
            Some(entry.clone()),
            "get_key returned {:?} for entry '{}' ptr {}",
            got,
            entry.as_str(),
            ptr
        );
    }
    Ok(())
}

#[test]
fn get_key_multiple_fst_segments() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_get_key_multiple_fst.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    // Partition many keys into three batches, flushing fst after the first two batches
    // to create multiple fst segments and stress fst's memory caching behavior on insert.
    let num_keys = 1_200;
    // Generate fixed-width numeric keys in descending order to create out-of-order segments.
    let keys: Vec<String> = (0..num_keys).rev().map(|i| format!("{:05}", i)).collect();
    let third = num_keys / 3;
    // First segment
    for key in keys[0..third].iter().rev() {
        builder.insert(key, Some(Value::Integer(1)));
    }
    builder.flush_fst()?;
    // Second segment
    for key in keys[third..2 * third].iter().rev() {
        builder.insert(key, Some(Value::Integer(2)));
    }
    builder.flush_fst()?;
    // Third segment (implicit flush at close)
    for key in keys[2 * third..].iter().rev() {
        builder.insert(key, Some(Value::Integer(3)));
    }
    let db = builder.into_database()?;
    let entries: Vec<Entry> = db.list("", None).unwrap().collect();
    for entry in &entries {
        let ptr = entry.ptr().expect("entry should have a ptr");
        let got = db.get_key(ptr)?;
        assert_eq!(
            got,
            Some(entry.clone()),
            "get_key returned {:?} for entry '{}' ptr {}",
            got,
            entry.as_str(),
            ptr
        );
    }
    Ok(())
}
