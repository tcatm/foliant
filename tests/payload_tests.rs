use foliant::{Index, Value};
use serde_cbor;
use tempfile::NamedTempFile;
use std::io::Write;

/// Test inserting and retrieving typed CBOR values in-memory, and round-trip via serialization.
#[test]
fn in_memory_value_roundtrip() {
    let mut trie = Index::new();
    // Insert an integer and a string as CBOR values
    let v_int = serde_cbor::to_vec(&42u8).unwrap();
    let v_str = serde_cbor::to_vec(&"hello".to_string()).unwrap();
    trie.insert("alpha", Some(v_int));
    trie.insert("beta", Some(v_str));
    trie.insert("gamma", None);
    // Retrieve decoded values
    assert_eq!(trie.get_value("alpha").unwrap(), Some(Value::Integer(42)));
    assert_eq!(trie.get_value("beta").unwrap(), Some(Value::Text("hello".to_string())));
    assert_eq!(trie.get_value("gamma").unwrap(), None);
    assert_eq!(trie.get_value("delta").unwrap(), None);
    // Serialize to buffer and read back
    // Serialize to buffer, write to a temp file, and load via mmap
    let mut buf = Vec::new();
    trie.write_index(&mut buf).unwrap();
    let mut tmp2 = NamedTempFile::new().expect("temp file");
    tmp2.write_all(&buf).expect("write temp");
    let trie2 = Index::load(tmp2.path()).unwrap();
    assert_eq!(trie2.get_value("alpha").unwrap(), Some(Value::Integer(42)));
    assert_eq!(trie2.get_value("beta").unwrap(), Some(Value::Text("hello".to_string())));
    assert_eq!(trie2.get_value("gamma").unwrap(), None);
}

/// Test retrieving CBOR values via memory-mapped trie.
#[test]
fn mmap_value_access() {
    let mut trie = Index::new();
    // Boolean and array types
    let v_bool = serde_cbor::to_vec(&true).unwrap();
    let v_arr = serde_cbor::to_vec(&vec![1u8,2,3]).unwrap();
    trie.insert("x", Some(v_bool));
    trie.insert("y", Some(v_arr));
    // Write to temporary file
    let mut tmp = NamedTempFile::new().expect("temp file");
    trie.write_index(&mut tmp).unwrap();
    // Load via mmap
    let mtrie = Index::load(tmp.path()).unwrap();
    // Access decoded values
    assert_eq!(mtrie.get_value("x").unwrap(), Some(Value::Bool(true)));
    assert_eq!(mtrie.get_value("y").unwrap(), Some(Value::Array(vec![
        Value::Integer(1), Value::Integer(2), Value::Integer(3)
    ])));
    assert_eq!(mtrie.get_value("z").unwrap(), None);
}