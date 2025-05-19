use foliant::{Trie, MmapTrie, Entry};
use std::io::Write;
use tempfile::NamedTempFile;

/// Test that listing with a prefix that falls in the middle of an edge
/// correctly finds the subtree via the indexed mmap reader.
#[test]
fn mid_edge_prefix_mmap() {
    // Build a small trie with a compressed edge "abcd"
    let mut trie = Trie::new();
    trie.insert("abcdx");
    trie.insert("abcde");

    // Serialize to a buffer
    let mut buf = Vec::new();
    trie.write_radix(&mut buf).unwrap();

    // Write to a NamedTempFile for mmap loading
    let mut tmp = NamedTempFile::new().expect("temp file");
    tmp.as_file_mut().write_all(&buf).unwrap();

    // Load via mmap-based trie
    let mtrie = MmapTrie::load(tmp.path()).expect("mmap load failed");

    // Expected results for prefix "abc"
    let expected = vec![
        Entry::Key("abcde".to_string()),
        Entry::Key("abcdx".to_string()),
    ];

    // In-memory listing
    let mut mem = trie.list("abc", None);
    mem.sort();
    assert_eq!(mem, expected, "in-memory trie mismatch");

    // Mmap listing
    let mut mm = mtrie.list("abc", None);
    mm.sort();
    assert_eq!(mm, expected, "mmap trie mismatch");

}