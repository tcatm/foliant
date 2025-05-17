use index::{Trie, Entry};

// Tests for expected behavior of a radix (compressed) trie interface.
// These should pass on a standard Trie implementation and remain valid after compression.

#[test]
fn long_common_prefix_keys() {
    let mut trie = Trie::new();
    let words = ["compression", "complete", "companion"];
    for &w in &words {
        trie.insert(w);
    }
    let mut result = trie.list("com", None);
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
    let mut trie = Trie::new();
    let keys = ["", "a", "ab", "abc", "compression", "companion"];
    for &k in &keys {
        trie.insert(k);
    }
    // Serialize to buffer
    let mut buf = Vec::new();
    trie.write_radix(&mut buf).unwrap();
    // Deserialize back
    let trie2 = Trie::read_radix(&mut &buf[..]).unwrap();
    // Compare listings
    let mut orig = trie.list("", None);
    let mut de = trie2.list("", None);
    orig.sort(); de.sort();
    assert_eq!(orig, de);
    // Also test some prefixes
    for &pref in &["", "a", "ab", "comp"] {
        let mut o = trie.list(pref, Some('/')); o.sort();
        let mut d = trie2.list(pref, Some('/')); d.sort();
        assert_eq!(o, d, "mismatch on prefix {}", pref);
    }
}

#[test]
fn listing_mid_edge_prefix() {
    let mut trie = Trie::new();
    let words = ["compression", "complete"];
    for &w in &words {
        trie.insert(w);
    }
    // Prefix that ends in the middle of an edge label in a compressed trie
    let mut result = trie.list("comp", None);
    result.sort();
    let mut expected: Vec<Entry> = words
        .iter()
        .map(|&s| Entry::Key(s.to_string()))
        .collect();
    expected.sort();
    assert_eq!(result, expected);

    // Longer prefix matching only one key
    let mut res2 = trie.list("compre", None);
    res2.sort();
    let expected2: Vec<Entry> = vec!["compression"]
        .into_iter()
        .map(|s| Entry::Key(s.to_string()))
        .collect();
    assert_eq!(res2, expected2);
}

#[test]
fn splitting_on_partial_overlap() {
    let mut trie = Trie::new();
    trie.insert("test");
    trie.insert("team");
    let mut result = trie.list("te", None);
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
    let mut trie = Trie::new();
    let paths = [
        "dir1/file1",
        "dir1/subdir/file2",
        "dir2/file3",
    ];
    for &p in &paths {
        trie.insert(p);
    }
    // Top-level grouping on '/'
    let mut top = trie.list("", Some('/'));
    top.sort();
    let expected_top: Vec<Entry> = vec![
        Entry::CommonPrefix("dir1/".to_string()),
        Entry::CommonPrefix("dir2/".to_string()),
    ];
    assert_eq!(top, expected_top);

    // Grouping within dir1
    let mut dir1 = trie.list("dir1/", Some('/'));
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
    let mut trie = Trie::new();
    trie.insert("");
    let result = trie.list("", None);
    assert_eq!(result, vec![Entry::Key("".to_string())]);
}

#[test]
fn duplicate_insert() {
    let mut trie = Trie::new();
    trie.insert("repeat");
    trie.insert("repeat");
    let result = trie.list("", None);
    assert_eq!(result, vec![Entry::Key("repeat".to_string())]);
}

#[test]
fn prefix_key_cases() {
    let mut trie = Trie::new();
    let keys = ["a", "ab", "abc"];
    for &k in &keys { trie.insert(k); }
    let mut r1 = trie.list("a", None);
    r1.sort();
    let expected1: Vec<Entry> = keys.iter().map(|&s| Entry::Key(s.to_string())).collect();
    assert_eq!(r1, expected1);
    let mut r2 = trie.list("ab", None);
    r2.sort();
    let expected2: Vec<Entry> = ["ab", "abc"].iter().map(|&s| Entry::Key(s.to_string())).collect();
    assert_eq!(r2, expected2);
    let mut r3 = trie.list("abc", None);
    r3.sort();
    let expected3 = vec![Entry::Key("abc".to_string())];
    assert_eq!(r3, expected3);
    let r4 = trie.list("abcd", None);
    assert!(r4.is_empty());
}

#[test]
fn ordering_of_keys() {
    let mut trie = Trie::new();
    for &k in &["b", "a", "c"] { trie.insert(k); }
    let result = trie.list("", None);
    let expected = vec![
        Entry::Key("a".to_string()),
        Entry::Key("b".to_string()),
        Entry::Key("c".to_string()),
    ];
    assert_eq!(result, expected);
}

#[test]
fn nonexistent_prefix_radix() {
    let mut trie = Trie::new();
    trie.insert("hello");
    let result = trie.list("helz", None);
    assert!(result.is_empty());
}

#[test]
fn unicode_keys() {
    let mut trie = Trie::new();
    let words = ["こんにちは", "こんばんは", "こん"];
    for &w in &words { trie.insert(w); }
    let mut result = trie.list("こん", None);
    result.sort();
    let mut expected: Vec<Entry> = words.iter().map(|&s| Entry::Key(s.to_string())).collect();
    expected.sort();
    assert_eq!(result, expected);
}

#[test]
fn unicode_common_prefix() {
    let mut trie = Trie::new();
    let words = ["🤖robot", "🤖romantic"];
    for &w in &words { trie.insert(w); }
    let mut r1 = trie.list("🤖", None);
    r1.sort();
    let mut exp1: Vec<Entry> = words.iter().map(|&s| Entry::Key(s.to_string())).collect();
    exp1.sort();
    assert_eq!(r1, exp1);
    let mut r2 = trie.list("🤖rob", None);
    r2.sort();
    assert_eq!(r2, vec![Entry::Key("🤖robot".to_string())]);
}

#[test]
fn multi_delimiter_grouping() {
    let mut trie = Trie::new();
    let paths = ["foo/bar/baz", "foo/bar/qux"];
    for &p in &paths { trie.insert(p); }
    let mut r1 = trie.list("foo/", Some('/'));
    r1.sort();
    let exp1 = vec![Entry::CommonPrefix("foo/bar/".to_string())];
    assert_eq!(r1, exp1);
    let mut r2 = trie.list("foo/bar/", Some('/'));
    r2.sort();
    let exp2: Vec<Entry> = paths.iter().map(|&s| Entry::Key(s.to_string())).collect();
    assert_eq!(r2, exp2);
}

#[test]
fn delimiter_edge_cases() {
    let mut trie = Trie::new();
    let paths = ["/a", "b/"];
    for &p in &paths { trie.insert(p); }
    let mut r1 = trie.list("", Some('/'));
    r1.sort();
    let exp1 = vec![
        Entry::CommonPrefix("/".to_string()),
        Entry::CommonPrefix("b/".to_string()),
    ];
    assert_eq!(r1, exp1);
    let r2 = trie.list("/", Some('/'));
    assert_eq!(r2, vec![Entry::Key("/a".to_string())]);
}

#[test]
fn only_delimiter_key() {
    let mut trie = Trie::new();
    trie.insert("/");
    // Plain listing
    let plain = trie.list("", None);
    assert_eq!(plain, vec![Entry::Key("/".to_string())]);
    // Grouping at root
    let grp = trie.list("", Some('/'));
    assert_eq!(grp, vec![Entry::CommonPrefix("/".to_string())]);
    // Listing after prefix '/'
    let after_grp = trie.list("/", Some('/'));
    assert_eq!(after_grp, vec![Entry::CommonPrefix("/".to_string())]);
    let after_plain = trie.list("/", None);
    assert_eq!(after_plain, vec![Entry::Key("/".to_string())]);
}

#[test]
fn trailing_delimiter_key() {
    let mut trie = Trie::new();
    trie.insert("a/");
    // Grouping at root should yield the full 'a/' as a common prefix
    let grp = trie.list("", Some('/'));
    assert_eq!(grp, vec![Entry::CommonPrefix("a/".to_string())]);
    // Listing after prefix 'a/'
    let after = trie.list("a/", Some('/'));
    assert_eq!(after, vec![Entry::CommonPrefix("a/".to_string())]);
    // Plain listing still returns the key
    let plain = trie.list("", None);
    assert_eq!(plain, vec![Entry::Key("a/".to_string())]);
}

#[test]
fn delimiter_at_start_and_end() {
    let mut trie = Trie::new();
    let key = "/start/";
    trie.insert(key);
    // Grouping at root picks up first '/'
    let root_grp = trie.list("", Some('/'));
    assert_eq!(root_grp, vec![Entry::CommonPrefix("/".to_string())]);
    // After consuming first '/', grouping yields the full '/start/'
    let mid_grp = trie.list("/", Some('/'));
    assert_eq!(mid_grp, vec![Entry::CommonPrefix(key.to_string())]);
    // After full prefix, listing yields the key as a common-prefix (ends with '/')
    let end_grp = trie.list(key, Some('/'));
    assert_eq!(end_grp, vec![Entry::CommonPrefix(key.to_string())]);
}

#[test]
fn multiple_leading_delimiters() {
    let mut trie = Trie::new();
    trie.insert("//foo");
    // At root, first slash groups '/'
    let r1 = trie.list("", Some('/'));
    assert_eq!(r1, vec![Entry::CommonPrefix("/".to_string())]);
    // After one '/', grouping yields '//' as common prefix
    let r2 = trie.list("/", Some('/'));
    assert_eq!(r2, vec![Entry::CommonPrefix("//".to_string())]);
    // After '//' prefix, the remaining key is 'foo'
    let r3 = trie.list("//", Some('/'));
    assert_eq!(r3, vec![Entry::Key("//foo".to_string())]);
}