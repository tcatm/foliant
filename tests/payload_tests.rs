use foliant::{Database, DatabaseBuilder, Value};
use serde_cbor;
use tempfile::tempdir;

/// Test inserting and retrieving typed CBOR values in-memory, and round-trip via serialization.
#[test]
fn in_memory_value_roundtrip() {
    let mut db = Database::new();
    // Insert integer and string values directly
    db.insert("alpha", Some(Value::Integer(42)));
    db.insert("beta", Some(Value::Text("hello".to_string())));
    db.insert("gamma", None::<Value>);
    // Retrieve decoded values
    assert_eq!(db.get_value("alpha").unwrap(), Some(Value::Integer(42)));
    assert_eq!(db.get_value("beta").unwrap(), Some(Value::Text("hello".to_string())));
    assert_eq!(db.get_value("gamma").unwrap(), None::<Value>);
    assert_eq!(db.get_value("delta").unwrap(), None::<Value>);
    // Serialize to disk and read back via mmap
    let dir = tempdir().expect("temp dir");
    let base = dir.path().join("db");
    db.save(&base).unwrap();
    let db2 = Database::open(&base).unwrap();
    assert_eq!(db2.get_value("alpha").unwrap(), Some(Value::Integer(42)));
    assert_eq!(db2.get_value("beta").unwrap(), Some(Value::Text("hello".to_string())));
    assert_eq!(db2.get_value("gamma").unwrap(), None::<Value>);
}

/// Test retrieving CBOR values via memory-mapped trie.
#[test]
fn mmap_value_access() {
    // Setup temporary base path
    let dir = tempdir().expect("temp dir");
    let base = dir.path().join("db");
    // Initialize database builder
    let mut builder = DatabaseBuilder::new(&base).unwrap();
    // Boolean and array types inserted directly
    builder.insert("x", Some(Value::Bool(true)));
    builder.insert(
        "y",
        Some(Value::Array(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ])),
    );
    // Serialize to disk and load via mmap
    builder.close().unwrap();
    let mdb = Database::open(&base).unwrap();
    // Access decoded values
    assert_eq!(mdb.get_value("x").unwrap(), Some(Value::Bool(true)));
    assert_eq!(mdb.get_value("y").unwrap(), Some(Value::Array(vec![
        Value::Integer(1), Value::Integer(2), Value::Integer(3)
    ])));
    assert_eq!(mdb.get_value("z").unwrap(), None::<Value>);
}