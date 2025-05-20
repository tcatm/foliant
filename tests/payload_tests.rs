use foliant::{Database, Value};
use serde_cbor;
use tempfile::tempdir;

/// Test inserting and retrieving typed CBOR values in-memory, and round-trip via serialization.
#[test]
fn in_memory_value_roundtrip() {
    let mut db = Database::new();
    // Insert an integer and a string as CBOR values
    let v_int = serde_cbor::to_vec(&42u8).unwrap();
    let v_str = serde_cbor::to_vec(&"hello".to_string()).unwrap();
    db.insert("alpha", Some(v_int));
    db.insert("beta", Some(v_str));
    db.insert("gamma", None);
    // Retrieve decoded values
    assert_eq!(db.get_value("alpha").unwrap(), Some(Value::Integer(42)));
    assert_eq!(db.get_value("beta").unwrap(), Some(Value::Text("hello".to_string())));
    assert_eq!(db.get_value("gamma").unwrap(), None);
    assert_eq!(db.get_value("delta").unwrap(), None);
    // Serialize to disk and read back via mmap
    let dir = tempdir().expect("temp dir");
    let base = dir.path().join("db");
    db.save(&base).unwrap();
    let db2 = Database::open(&base).unwrap();
    assert_eq!(db2.get_value("alpha").unwrap(), Some(Value::Integer(42)));
    assert_eq!(db2.get_value("beta").unwrap(), Some(Value::Text("hello".to_string())));
    assert_eq!(db2.get_value("gamma").unwrap(), None);
}

/// Test retrieving CBOR values via memory-mapped trie.
#[test]
fn mmap_value_access() {
    let mut db = Database::new();
    // Boolean and array types
    let v_bool = serde_cbor::to_vec(&true).unwrap();
    let v_arr = serde_cbor::to_vec(&vec![1u8,2,3]).unwrap();
    db.insert("x", Some(v_bool));
    db.insert("y", Some(v_arr));
    // Serialize to disk and load via mmap
    let dir = tempdir().expect("temp dir");
    let base = dir.path().join("db");
    db.save(&base).unwrap();
    let mdb = Database::open(&base).unwrap();
    // Access decoded values
    assert_eq!(mdb.get_value("x").unwrap(), Some(Value::Bool(true)));
    assert_eq!(mdb.get_value("y").unwrap(), Some(Value::Array(vec![
        Value::Integer(1), Value::Integer(2), Value::Integer(3)
    ])));
    assert_eq!(mdb.get_value("z").unwrap(), None);
}