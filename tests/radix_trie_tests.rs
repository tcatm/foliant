use foliant::{Index, Entry};
use foliant::Streamer;
use tempfile::NamedTempFile;
use std::io::Write;

// Tests for expected behavior of a radix (compressed) trie interface.
// These should pass on a standard Trie implementation and remain valid after compression.

#[test]
fn long_common_prefix_keys() {
    let mut trie = Index::new();
    let words = ["compression", "complete", "companion"];
    for &w in &words {
        trie.insert(w, None);
    }
    let mut result: Vec<Entry> = trie.list("com", None).collect();
    result.sort();
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
    let mut orig: Vec<Entry> = trie.list("", None).collect();
    orig.sort();
    let mut de: Vec<Entry> = trie2.list("", None).collect();
    de.sort();
    assert_eq!(orig, de);
    // Also test some prefixes
    for &pref in &["", "a", "ab", "comp"] {
        let mut o: Vec<Entry> = trie.list(pref, Some('/')).collect();
        o.sort();
        let mut d: Vec<Entry> = trie2.list(pref, Some('/')).collect();
        d.sort();
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
    let mut result: Vec<Entry> = trie.list("comp", None).collect();
    result.sort();
    let mut expected: Vec<Entry> = words
        .iter()
        .map(|&s| Entry::Key(s.to_string()))
        .collect();
    expected.sort();
    assert_eq!(result, expected);

    // Longer prefix matching only one key
    let mut res2: Vec<Entry> = trie.list("compre", None).collect();
    res2.sort();
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
    let mut result: Vec<Entry> = trie.list("te", None).collect();
    result.sort();
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
    let mut top: Vec<Entry> = trie.list("", Some('/')).collect();
    top.sort();
    let expected_top: Vec<Entry> = vec![
        Entry::CommonPrefix("dir1/".to_string()),
        Entry::CommonPrefix("dir2/".to_string()),
    ];
    assert_eq!(top, expected_top);

    // Grouping within dir1
    let mut dir1: Vec<Entry> = trie.list("dir1/", Some('/')).collect();
    dir1.sort();
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
    let mut result: Vec<Entry> = trie.list("", None).collect();
    result.sort();
    assert_eq!(result, vec![Entry::Key("".to_string())]);
}

#[test]
fn duplicate_insert() {
    let mut trie = Index::new();
    trie.insert("repeat", None);
    trie.insert("repeat", None);
    let mut result: Vec<Entry> = trie.list("", None).collect();
    result.sort();
    assert_eq!(result, vec![Entry::Key("repeat".to_string())]);
}

#[test]
fn prefix_key_cases() {
    let mut trie = Index::new();
    let keys = ["a", "ab", "abc"];
    for &k in &keys { trie.insert(k, None); }
    let mut r1: Vec<Entry> = trie.list("a", None).collect();
    r1.sort();
    let expected1: Vec<Entry> = keys.iter().map(|&s| Entry::Key(s.to_string())).collect();
    assert_eq!(r1, expected1);
    let mut r2: Vec<Entry> = trie.list("ab", None).collect();
    r2.sort();
    let expected2: Vec<Entry> = ["ab", "abc"].iter().map(|&s| Entry::Key(s.to_string())).collect();
    assert_eq!(r2, expected2);
    let mut r3: Vec<Entry> = trie.list("abc", None).collect();
    r3.sort();
    let expected3 = vec![Entry::Key("abc".to_string())];
    assert_eq!(r3, expected3);
    let mut r4: Vec<Entry> = trie.list("abcd", None).collect();
    r4.sort();
    assert!(r4.is_empty());
}

#[test]
fn ordering_of_keys() {
    let mut trie = Index::new();
    for &k in &["b", "a", "c"] { trie.insert(k, None); }
    let mut result: Vec<Entry> = trie.list("", None).collect();
    result.sort();
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
    let mut result: Vec<Entry> = trie.list("helz", None).collect();
    result.sort();
    assert!(result.is_empty());
}

#[test]
fn unicode_keys() {
    let mut trie = Index::new();
    let words = ["こんにちは", "こんばんは", "こん"];
    for &w in &words { trie.insert(w, None); }
    let mut result: Vec<Entry> = trie.list("こん", None).collect();
    result.sort();
    let mut expected: Vec<Entry> = words.iter().map(|&s| Entry::Key(s.to_string())).collect();
    expected.sort();
    assert_eq!(result, expected);
}

#[test]
fn unicode_common_prefix() {
    let mut trie = Index::new();
    let words = ["🤖robot", "🤖romantic"];
    for &w in &words { trie.insert(w, None); }
    let mut r1: Vec<Entry> = trie.list("🤖", None).collect();
    r1.sort();
    let mut exp1: Vec<Entry> = words.iter().map(|&s| Entry::Key(s.to_string())).collect();
    exp1.sort();
    assert_eq!(r1, exp1);
    let mut r2: Vec<Entry> = trie.list("🤖rob", None).collect();
    r2.sort();
    assert_eq!(r2, vec![Entry::Key("🤖robot".to_string())]);
}

#[test]
fn multi_delimiter_grouping() {
    let mut trie = Index::new();
    let paths = ["foo/bar/baz", "foo/bar/qux"];
    for &p in &paths { trie.insert(p, None); }
    let mut r1: Vec<Entry> = trie.list("foo/", Some('/')).collect();
    r1.sort();
    let exp1 = vec![Entry::CommonPrefix("foo/bar/".to_string())];
    assert_eq!(r1, exp1);
    let mut r2: Vec<Entry> = trie.list("foo/bar/", Some('/')).collect();
    r2.sort();
    let exp2: Vec<Entry> = paths.iter().map(|&s| Entry::Key(s.to_string())).collect();
    assert_eq!(r2, exp2);
}

#[test]
fn delimiter_edge_cases() {
    let mut trie = Index::new();
    let paths = ["/a", "b/"];
    for &p in &paths { trie.insert(p, None); }
    let mut r1: Vec<Entry> = trie.list("", Some('/')).collect();
    r1.sort();
    let exp1 = vec![
        Entry::CommonPrefix("/".to_string()),
        Entry::CommonPrefix("b/".to_string()),
    ];
    assert_eq!(r1, exp1);
    let mut r2: Vec<Entry> = trie.list("/", Some('/')).collect();
    r2.sort();
    assert_eq!(r2, vec![Entry::Key("/a".to_string())]);
}

#[test]
fn only_delimiter_key() {
    let mut trie = Index::new();
    trie.insert("/", None);
    // Plain listing
    let mut plain: Vec<Entry> = trie.list("", None).collect();
    plain.sort();
    assert_eq!(plain, vec![Entry::Key("/".to_string())]);
    // Grouping at root
    let mut grp: Vec<Entry> = trie.list("", Some('/')).collect();
    grp.sort();
    assert_eq!(grp, vec![Entry::CommonPrefix("/".to_string())]);
    // Listing after prefix '/'
    let mut after_grp: Vec<Entry> = trie.list("/", Some('/')).collect();
    after_grp.sort();
    assert_eq!(after_grp, vec![Entry::CommonPrefix("/".to_string())]);
    let mut after_plain: Vec<Entry> = trie.list("/", None).collect();
    after_plain.sort();
    assert_eq!(after_plain, vec![Entry::Key("/".to_string())]);
}

#[test]
fn trailing_delimiter_key() {
    let mut trie = Index::new();
    trie.insert("a/", None);
    // Grouping at root should yield the full 'a/' as a common prefix
    let mut grp: Vec<Entry> = trie.list("", Some('/')).collect();
    grp.sort();
    assert_eq!(grp, vec![Entry::CommonPrefix("a/".to_string())]);
    // Listing after prefix 'a/'
    let mut after: Vec<Entry> = trie.list("a/", Some('/')).collect();
    after.sort();
    assert_eq!(after, vec![Entry::CommonPrefix("a/".to_string())]);
    // Plain listing still returns the key
    let mut plain: Vec<Entry> = trie.list("", None).collect();
    plain.sort();
    assert_eq!(plain, vec![Entry::Key("a/".to_string())]);
}

#[test]
fn delimiter_at_start_and_end() {
    let mut trie = Index::new();
    let key = "/start/";
    trie.insert(key, None);
    // Grouping at root picks up first '/'
    let mut root_grp: Vec<Entry> = trie.list("", Some('/')).collect();
    root_grp.sort();
    assert_eq!(root_grp, vec![Entry::CommonPrefix("/".to_string())]);
    // After consuming first '/', grouping yields the full '/start/'
    let mut mid_grp: Vec<Entry> = trie.list("/", Some('/')).collect();
    mid_grp.sort();
    assert_eq!(mid_grp, vec![Entry::CommonPrefix(key.to_string())]);
    // After full prefix, listing yields the key as a common-prefix (ends with '/')
    let mut end_grp: Vec<Entry> = trie.list(key, Some('/')).collect();
    end_grp.sort();
    assert_eq!(end_grp, vec![Entry::CommonPrefix(key.to_string())]);
}

#[test]
fn multiple_leading_delimiters() {
    let mut trie = Index::new();
    trie.insert("//foo", None);
    // At root, first slash groups '/'
    let mut r1: Vec<Entry> = trie.list("", Some('/')).collect();
    r1.sort();
    assert_eq!(r1, vec![Entry::CommonPrefix("/".to_string())]);
    // After one '/', grouping yields '//' as common prefix
    let mut r2: Vec<Entry> = trie.list("/", Some('/')).collect();
    r2.sort();
    assert_eq!(r2, vec![Entry::CommonPrefix("//".to_string())]);
    // After '//' prefix, the remaining key is 'foo'
    let mut r3: Vec<Entry> = trie.list("//", Some('/')).collect();
    r3.sort();
    assert_eq!(r3, vec![Entry::Key("//foo".to_string())]);
}