use foliant::{Index, Entry};
use tempfile::NamedTempFile;
use std::io::Write;
mod common;
use common::collect_sorted;

// Tests for expected behavior of a radix (compressed) trie interface.
// These should pass on a standard Trie implementation and remain valid after compression.

#[test]
fn long_common_prefix_keys() {
    let mut trie = Index::new();
    let words = ["compression", "complete", "companion"];
    for &w in &words {
        trie.insert(w, None);
    }
    let result = collect_sorted(trie.list_iter("com", None));
    let mut expected: Vec<Entry> = words
        .iter()
        .map(|&s| Entry::Key(s.to_string()))
        .collect();
    expected.sort();
    assert_eq!(result, expected);
}

#[test]
fn serialize_deserialize_roundtrip() {
    let mut trie = Index::new();
    let keys = ["", "a", "ab", "abc", "compression", "companion"];
    for &k in &keys {
        trie.insert(k, None);
    }
    // Serialize to buffer, write to temp file, and reload via mmap
    let mut buf = Vec::new();
    trie.write_index(&mut buf).unwrap();
    let mut tmp = NamedTempFile::new().expect("temp file");
    tmp.write_all(&buf).expect("write temp");
    let trie2 = Index::open(tmp.path()).unwrap();
    // Compare listings
    let orig = collect_sorted(trie.list_iter("", None));
    let de = collect_sorted(trie2.list("", None));
    assert_eq!(orig, de);
    // Also test some prefixes
    for &pref in &["", "a", "ab", "comp"] {
        let o = collect_sorted(trie.list_iter(pref, Some('/')));
        let d = collect_sorted(trie2.list_iter(pref, Some('/')));
        assert_eq!(o, d, "mismatch on prefix {}", pref);
    }
}

#[test]
fn listing_mid_edge_prefix() {
    let mut trie = Index::new();
    let words = ["compression", "complete"];
    for &w in &words {
        trie.insert(w, None);
    }
    // Prefix that ends in the middle of an edge label in a compressed trie
    let result = collect_sorted(trie.list_iter("comp", None));
    let mut expected: Vec<Entry> = words
        .iter()
        .map(|&s| Entry::Key(s.to_string()))
        .collect();
    expected.sort();
    assert_eq!(result, expected);

    // Longer prefix matching only one key
    let res2 = collect_sorted(trie.list_iter("compre", None));
    let expected2: Vec<Entry> = vec!["compression"]
        .into_iter()
        .map(|s| Entry::Key(s.to_string()))
        .collect();
    assert_eq!(res2, expected2);
}

#[test]
fn splitting_on_partial_overlap() {
    let mut trie = Index::new();
    trie.insert("test", None);
    trie.insert("team", None);
    let result = collect_sorted(trie.list_iter("te", None));
    let mut expected: Vec<Entry> = vec!["test", "team"]
        .into_iter()
        .map(|s| Entry::Key(s.to_string()))
        .collect();
    expected.sort();
    assert_eq!(result, expected);
}

#[test]
fn delimiter_grouping_after_compression() {
    let mut trie = Index::new();
    let paths = [
        "dir1/file1",
        "dir1/subdir/file2",
        "dir2/file3",
    ];
    for &p in &paths {
        trie.insert(p, None);
    }
    // Top-level grouping on '/'
    let top = collect_sorted(trie.list_iter("", Some('/')));
    let expected_top: Vec<Entry> = vec![
        Entry::CommonPrefix("dir1/".to_string()),
        Entry::CommonPrefix("dir2/".to_string()),
    ];
    assert_eq!(top, expected_top);

    // Grouping within dir1
    let dir1 = collect_sorted(trie.list_iter("dir1/", Some('/')));
    let expected_dir1: Vec<Entry> = vec![
        Entry::Key("dir1/file1".to_string()),
        Entry::CommonPrefix("dir1/subdir/".to_string()),
    ];
    assert_eq!(dir1, expected_dir1);
}
// Additional edge-case tests for radix trie behavior

#[test]
fn empty_key() {
    let mut trie = Index::new();
    trie.insert("", None);
    let result = collect_sorted(trie.list_iter("", None));
    assert_eq!(result, vec![Entry::Key("".to_string())]);
}

#[test]
fn duplicate_insert() {
    let mut trie = Index::new();
    trie.insert("repeat", None);
    trie.insert("repeat", None);
    let result = collect_sorted(trie.list_iter("", None));
    assert_eq!(result, vec![Entry::Key("repeat".to_string())]);
}

#[test]
fn prefix_key_cases() {
    let mut trie = Index::new();
    let keys = ["a", "ab", "abc"];
    for &k in &keys { trie.insert(k, None); }
    let r1 = collect_sorted(trie.list_iter("a", None));
    let expected1: Vec<Entry> = keys.iter().map(|&s| Entry::Key(s.to_string())).collect();
    assert_eq!(r1, expected1);
    let r2 = collect_sorted(trie.list_iter("ab", None));
    let expected2: Vec<Entry> = ["ab", "abc"].iter().map(|&s| Entry::Key(s.to_string())).collect();
    assert_eq!(r2, expected2);
    let r3 = collect_sorted(trie.list_iter("abc", None));
    let expected3 = vec![Entry::Key("abc".to_string())];
    assert_eq!(r3, expected3);
    let r4 = collect_sorted(trie.list_iter("abcd", None));
    assert!(r4.is_empty());
}

#[test]
fn ordering_of_keys() {
    let mut trie = Index::new();
    for &k in &["b", "a", "c"] { trie.insert(k, None); }
    let result = collect_sorted(trie.list_iter("", None));
    let expected = vec![
        Entry::Key("a".to_string()),
        Entry::Key("b".to_string()),
        Entry::Key("c".to_string()),
    ];
    assert_eq!(result, expected);
}

#[test]
fn nonexistent_prefix_radix() {
    let mut trie = Index::new();
    trie.insert("hello", None);
    let result = collect_sorted(trie.list_iter("helz", None));
    assert!(result.is_empty());
}

#[test]
fn unicode_keys() {
    let mut trie = Index::new();
    let words = ["こんにちは", "こんばんは", "こん"];
    for &w in &words { trie.insert(w, None); }
    let result = collect_sorted(trie.list_iter("こん", None));
    let mut expected: Vec<Entry> = words.iter().map(|&s| Entry::Key(s.to_string())).collect();
    expected.sort();
    assert_eq!(result, expected);
}

#[test]
fn unicode_common_prefix() {
    let mut trie = Index::new();
    let words = ["🤖robot", "🤖romantic"];
    for &w in &words { trie.insert(w, None); }
    let r1 = collect_sorted(trie.list_iter("🤖", None));
    let mut exp1: Vec<Entry> = words.iter().map(|&s| Entry::Key(s.to_string())).collect();
    exp1.sort();
    assert_eq!(r1, exp1);
    let r2 = collect_sorted(trie.list_iter("🤖rob", None));
    assert_eq!(r2, vec![Entry::Key("🤖robot".to_string())]);
}

#[test]
fn multi_delimiter_grouping() {
    let mut trie = Index::new();
    let paths = ["foo/bar/baz", "foo/bar/qux"];
    for &p in &paths { trie.insert(p, None); }
    let r1 = collect_sorted(trie.list_iter("foo/", Some('/')));
    let exp1 = vec![Entry::CommonPrefix("foo/bar/".to_string())];
    assert_eq!(r1, exp1);
    let r2 = collect_sorted(trie.list_iter("foo/bar/", Some('/')));
    let exp2: Vec<Entry> = paths.iter().map(|&s| Entry::Key(s.to_string())).collect();
    assert_eq!(r2, exp2);
}

#[test]
fn delimiter_edge_cases() {
    let mut trie = Index::new();
    let paths = ["/a", "b/"];
    for &p in &paths { trie.insert(p, None); }
    let r1 = collect_sorted(trie.list_iter("", Some('/')));
    let exp1 = vec![
        Entry::CommonPrefix("/".to_string()),
        Entry::CommonPrefix("b/".to_string()),
    ];
    assert_eq!(r1, exp1);
    let r2 = collect_sorted(trie.list_iter("/", Some('/')));
    assert_eq!(r2, vec![Entry::Key("/a".to_string())]);
}

#[test]
fn only_delimiter_key() {
    let mut trie = Index::new();
    trie.insert("/", None);
    // Plain listing
    let plain = collect_sorted(trie.list_iter("", None));
    assert_eq!(plain, vec![Entry::Key("/".to_string())]);
    // Grouping at root
    let grp = collect_sorted(trie.list_iter("", Some('/')));
    assert_eq!(grp, vec![Entry::CommonPrefix("/".to_string())]);
    // Listing after prefix '/'
    let after_grp = collect_sorted(trie.list_iter("/", Some('/')));
    assert_eq!(after_grp, vec![Entry::CommonPrefix("/".to_string())]);
    let after_plain = collect_sorted(trie.list_iter("/", None));
    assert_eq!(after_plain, vec![Entry::Key("/".to_string())]);
}

#[test]
fn trailing_delimiter_key() {
    let mut trie = Index::new();
    trie.insert("a/", None);
    // Grouping at root should yield the full 'a/' as a common prefix
    let grp = collect_sorted(trie.list_iter("", Some('/')));
    assert_eq!(grp, vec![Entry::CommonPrefix("a/".to_string())]);
    // Listing after prefix 'a/'
    let after = collect_sorted(trie.list_iter("a/", Some('/')));
    assert_eq!(after, vec![Entry::CommonPrefix("a/".to_string())]);
    // Plain listing still returns the key
    let plain = collect_sorted(trie.list_iter("", None));
    assert_eq!(plain, vec![Entry::Key("a/".to_string())]);
}

#[test]
fn delimiter_at_start_and_end() {
    let mut trie = Index::new();
    let key = "/start/";
    trie.insert(key, None);
    // Grouping at root picks up first '/'
    let root_grp = collect_sorted(trie.list_iter("", Some('/')));
    assert_eq!(root_grp, vec![Entry::CommonPrefix("/".to_string())]);
    // After consuming first '/', grouping yields the full '/start/'
    let mid_grp = collect_sorted(trie.list_iter("/", Some('/')));
    assert_eq!(mid_grp, vec![Entry::CommonPrefix(key.to_string())]);
    // After full prefix, listing yields the key as a common-prefix (ends with '/')
    let end_grp = collect_sorted(trie.list_iter(key, Some('/')));
    assert_eq!(end_grp, vec![Entry::CommonPrefix(key.to_string())]);
}

#[test]
fn multiple_leading_delimiters() {
    let mut trie = Index::new();
    trie.insert("//foo", None);
    // At root, first slash groups '/'
    let r1 = collect_sorted(trie.list_iter("", Some('/')));
    assert_eq!(r1, vec![Entry::CommonPrefix("/".to_string())]);
    // After one '/', grouping yields '//' as common prefix
    let r2 = collect_sorted(trie.list_iter("/", Some('/')));
    assert_eq!(r2, vec![Entry::CommonPrefix("//".to_string())]);
    // After '//' prefix, the remaining key is 'foo'
    let r3 = collect_sorted(trie.list_iter("//", Some('/')));
    assert_eq!(r3, vec![Entry::Key("//foo".to_string())]);
}