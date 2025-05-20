use foliant::{DatabaseBuilder, Database, Entry, Value};
use foliant::Streamer;
use tempfile::tempdir;
use std::error::Error;

// 1) SimpleList: insert distinct keys and verify list("", None)
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
    let mut list: Vec<Entry> = db.list("", None).collect();
    list.sort();
    let mut expected: Vec<Entry> = keys.iter().map(|&s| Entry::Key(s.to_string())).collect();
    expected.sort();
    assert_eq!(list, expected);
    Ok(())
}

// 2) DelimiterGrouping: group on '/'
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
    let mut list: Vec<Entry> = db.list("foo", Some('/')).collect();
    list.sort();
    let expected = vec![
        Entry::CommonPrefix("foo/".to_string()),
        Entry::Key("foobar".to_string()),
    ];
    assert_eq!(list, expected);
    Ok(())
}

// 3) EmptyKey: inserting empty string
#[test]
fn empty_key() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("", None);
    let db = builder.into_database()?;
    let list: Vec<Entry> = db.list("", None).collect();
    assert_eq!(list, vec![Entry::Key("".to_string())]);
    Ok(())
}

// 4) DuplicateInsert: same key twice
#[test]
fn duplicate_insert() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("repeat", None);
    builder.insert("repeat", None);
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("", None).collect();
    list.sort();
    assert_eq!(list, vec![Entry::Key("repeat".to_string())]);
    Ok(())
}

// 5) PrefixSplit: keys "test" and "team"
#[test]
fn prefix_split() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("test", None);
    builder.insert("team", None);
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("te", None).collect();
    list.sort();
    let mut expected = vec![
        Entry::Key("test".to_string()),
        Entry::Key("team".to_string()),
    ];
    expected.sort();
    assert_eq!(list, expected);
    Ok(())
}

// 6) MidEdgePrefix: prefix falls mid-edge
#[test]
fn mid_edge_prefix() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("abcdx", None);
    builder.insert("abcde", None);
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("abc", None).collect();
    list.sort();
    let mut expected = vec![
        Entry::Key("abcde".to_string()),
        Entry::Key("abcdx".to_string()),
    ];
    expected.sort();
    assert_eq!(list, expected);
    Ok(())
}

// 7) Ordering: lex order enforced
#[test]
fn ordering() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    for &k in &["b", "a", "c"] {
        builder.insert(k, None);
    }
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("", None).collect();
    list.sort();
    let expected = vec![
        Entry::Key("a".to_string()),
        Entry::Key("b".to_string()),
        Entry::Key("c".to_string()),
    ];
    assert_eq!(list, expected);
    Ok(())
}

// 8) UnicodeKeys: multi-byte prefixes
#[test]
fn unicode_keys() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    let words = ["こんにちは", "こんばんは", "こん"];
    for &w in &words {
        builder.insert(w, None);
    }
    let db = builder.into_database()?;
    let mut list: Vec<Entry> = db.list("こん", None).collect();
    list.sort();
    let mut expected: Vec<Entry> = words.iter().map(|&s| Entry::Key(s.to_string())).collect();
    expected.sort();
    assert_eq!(list, expected);
    Ok(())
}

// 9) InMemoryPayloadRoundtrip: builder into_database returns mmap DB
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

// 10) MmapPayloadAccess: various CBOR types
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

// 11) SerializeVsMmapListing: list direct vs reopened
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
    let mut l1: Vec<Entry> = db1.list("", None).collect();
    let mut l2: Vec<Entry> = db2.list("", None).collect();
    l1.sort(); l2.sort();
    assert_eq!(l1, l2);
    Ok(())
}

// 12) RoundtripPrefixes: check multiple prefixes
#[test]
fn roundtrip_prefixes() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    let keys = ["", "a", "ab", "abc", "compression", "companion"];
    for &k in &keys {
        builder.insert(k, None);
    }
    let db1 = builder.into_database()?;
    let db2 = Database::<Value>::open(&base)?;
    for &pref in &["", "a", "ab", "comp"] {
        let mut r1: Vec<Entry> = db1.list(pref, Some('/')).collect();
        let mut r2: Vec<Entry> = db2.list(pref, Some('/')).collect();
        r1.sort(); r2.sort();
        assert_eq!(r1, r2);
    }
    Ok(())
}