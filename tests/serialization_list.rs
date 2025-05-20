use foliant::{Database, Entry};
use foliant::Streamer;
use tempfile::tempdir;

/// This test builds a small trie of file-like paths, serializes it to disk,
/// then memory-maps the file and verifies that both the in-memory and
/// mmap-backed tries produce the same listings (with and without grouping).
#[test]
fn serialize_and_mmap_list() {
    // Build a sample trie
    let mut db = Database::new();
    let keys = [
        "alpha",
        "beta",
        "gamma/delta",
        "gamma/epsilon",
    ];
    for &k in &keys {
        db.insert(k, None);
    }

    // Serialize to disk and load via mmap
    let dir = tempdir().expect("temp dir");
    let base = dir.path().join("db");
    db.save(&base).unwrap();
    let mdb = Database::open(&base).unwrap();

    // Compare listings without delimiter
    let mut list_mem: Vec<Entry> = db.list("", None).collect();
    let mut list_mmap: Vec<Entry> = mdb.list("", None).collect();
    list_mem.sort();
    list_mmap.sort();
    assert_eq!(list_mem, list_mmap);

    // Compare listings grouped by '/'
    let mut grp_mem: Vec<Entry> = db.list("", Some('/')).collect();
    let mut grp_mmap: Vec<Entry> = mdb.list("", Some('/')).collect();
    grp_mem.sort();
    grp_mmap.sort();
    assert_eq!(grp_mem, grp_mmap);

}

#[test]
fn serialize_and_mmap_list_prefix_a() {
    // Build a database in-memory with some keys, including ones not matching the prefix
    let mut db = Database::new();
    let keys = ["a1", "a2", "a3", "b1"];
    for &k in &keys {
    db.insert(k, None);
    }

    // Serialize to disk and load via mmap
    let dir = tempdir().expect("temp dir");
    let base = dir.path().join("db");
    db.save(&base).unwrap();
    let mdb = Database::open(&base).unwrap();

    // List entries with prefix "a"
    let mut list_mem: Vec<Entry> = db.list("a", None).collect();
    let mut list_mmap: Vec<Entry> = mdb.list("a", None).collect();
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
