use foliant::{Index, Entry};
use std::io::Write;
use tempfile::NamedTempFile;

/// This test builds a small trie of file-like paths, serializes it to disk,
/// then memory-maps the file and verifies that both the in-memory and
/// mmap-backed tries produce the same listings (with and without grouping).
#[test]
fn serialize_and_mmap_list() {
    // Build a sample trie
    let mut trie = Index::new();
    let keys = [
        "alpha",
        "beta",
        "gamma/delta",
        "gamma/epsilon",
    ];
    for &k in &keys {
        trie.insert(k, None);
    }

    // Serialize into an in-memory buffer
    let mut buf = Vec::new();
    trie.write_index(&mut buf).unwrap();

    // Write to a NamedTempFile
    let mut tmp = NamedTempFile::new().expect("temp file");
    tmp.as_file_mut().write_all(&buf).unwrap();

    // Load via memory-mapped trie
    let mtrie = Index::open(tmp.path()).unwrap();

    // Compare listings without delimiter
    let mut list_mem = trie.list("", None);
    let mut list_mmap = mtrie.list("", None);
    list_mem.sort();
    list_mmap.sort();
    assert_eq!(list_mem, list_mmap);

    // Compare listings grouped by '/'
    let mut grp_mem = trie.list("", Some('/'));
    let mut grp_mmap = mtrie.list("", Some('/'));
    grp_mem.sort();
    grp_mmap.sort();
    assert_eq!(grp_mem, grp_mmap);

}

#[test]
fn serialize_and_mmap_list_prefix_a() {
    // Build a trie with some keys, including ones not matching the prefix
    let mut trie = Index::new();
    let keys = ["a1", "a2", "a3", "b1"];
    for &k in &keys {
        trie.insert(k, None);
    }

    // Serialize into an in-memory buffer
    let mut buf = Vec::new();
    trie.write_index(&mut buf).unwrap();

    // Write to a NamedTempFile
    let mut tmp = NamedTempFile::new().expect("temp file");
    tmp.as_file_mut().write_all(&buf).unwrap();

    // Load via memory-mapped trie
    let mtrie = Index::open(tmp.path()).unwrap();

    // List entries with prefix "a"
    let mut list_mem = trie.list("a", None);
    let mut list_mmap = mtrie.list("a", None);
    list_mem.sort();
    list_mmap.sort();

    // Both tries should return only the "a"-prefixed entries
    assert_eq!(list_mem, list_mmap);
    assert_eq!(
        list_mem,
        vec![
            Entry::Key("a1".to_string()),
            Entry::Key("a2".to_string()),
            Entry::Key("a3".to_string()),
        ]
    );

}
