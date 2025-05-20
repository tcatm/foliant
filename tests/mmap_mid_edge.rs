use foliant::{Database, Entry};
use foliant::Streamer;
use tempfile::tempdir;

/// Test that listing with a prefix that falls in the middle of an edge
/// correctly finds the subtree via the indexed mmap reader.
#[test]
fn mid_edge_prefix_mmap() {
    // Build a small trie with a compressed edge "abcd"
    let mut db = Database::new();
    db.insert("abcdx", None::<Vec<u8>>);
    db.insert("abcde", None::<Vec<u8>>);

    // Serialize to disk and load via mmap
    let dir = tempdir().expect("temp dir");
    let base = dir.path().join("db");
    db.save(&base).expect("save failed");
    let mtrie = Database::open(&base).expect("mmap load failed");


    // Expected results for prefix "abc"
    let expected = vec![
        Entry::Key("abcde".to_string()),
        Entry::Key("abcdx".to_string()),
    ];

    // In-memory listing
    let mut mem: Vec<Entry> = db.list("abc", None::<char>).collect();
    mem.sort();
    assert_eq!(mem, expected, "in-memory trie mismatch");

    // Mmap listing
    let mut mm: Vec<Entry> = mtrie.list("abc", None::<char>).collect();
    mm.sort();
    assert_eq!(mm, expected, "mmap trie mismatch");

}