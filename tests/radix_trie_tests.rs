use foliant::{Database, Entry};
use foliant::Streamer;
use tempfile::tempdir;

// Tests for expected behavior of a radix (compressed) trie interface.
// These should pass on a standard Trie implementation and remain valid after compression.

#[test]
fn long_common_prefix_keys() {
    let mut db = Database::new();
    let words = ["compression", "complete", "companion"];
    for &w in &words {
        db.insert(w, None::<Vec<u8>>);
    }
    let mut result: Vec<Entry> = db.list("com", None::<char>).collect();
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
    let mut db = Database::new();
    let keys = ["", "a", "ab", "abc", "compression", "companion"];
    for &k in &keys {
        db.insert(k, None::<Vec<u8>>);
    }
    // Serialize to buffer, write to temp file, and reload via mmap
    let dir = tempdir().expect("temp dir");
    let base = dir.path().join("db");
    db.save(&base).unwrap();
    let db2 = Database::open(&base).unwrap();
    // Compare listings
    let mut orig: Vec<Entry> = db.list("", None::<char>).collect();
    orig.sort();
    let mut de: Vec<Entry> = db2.list("", None::<char>).collect();
    de.sort();
    assert_eq!(orig, de);
    // Also test some prefixes
    for &pref in &["", "a", "ab", "comp"] {
        let mut o: Vec<Entry> = db.list(pref, Some('/')).collect();
        o.sort();
        let mut d: Vec<Entry> = db2.list(pref, Some('/')).collect();
        d.sort();
        assert_eq!(o, d, "mismatch on prefix {}", pref);
    }
}

#[test]
fn listing_mid_edge_prefix() {
    let mut db = Database::new();
    let words = ["compression", "complete"];
    for &w in &words {
        db.insert(w, None::<Vec<u8>>);
    }
    // Prefix that ends in the middle of an edge label in a compressed db
    let mut result: Vec<Entry> = db.list("comp", None::<char>).collect();
    result.sort();
    let mut expected: Vec<Entry> = words
        .iter()
        .map(|&s| Entry::Key(s.to_string()))
        .collect();
    expected.sort();
    assert_eq!(result, expected);

    // Longer prefix matching only one key
    let mut res2: Vec<Entry> = db.list("compre", None::<char>).collect();
    res2.sort();
    let expected2: Vec<Entry> = vec!["compression"]
        .into_iter()
        .map(|s| Entry::Key(s.to_string()))
        .collect();
    assert_eq!(res2, expected2);
}

#[test]
fn splitting_on_partial_overlap() {
    let mut db = Database::new();
    db.insert("test", None::<Vec<u8>>);
    db.insert("team", None::<Vec<u8>>);
    let mut result: Vec<Entry> = db.list("te", None::<char>).collect();
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
    let mut db = Database::new();
    let paths = [
        "dir1/file1",
        "dir1/subdir/file2",
        "dir2/file3",
    ];
    for &p in &paths {
        db.insert(p, None::<Vec<u8>>);
    }
    // Top-level grouping on '/'
    let mut top: Vec<Entry> = db.list("", Some('/')).collect();
    top.sort();
    let expected_top: Vec<Entry> = vec![
        Entry::CommonPrefix("dir1/".to_string()),
        Entry::CommonPrefix("dir2/".to_string()),
    ];
    assert_eq!(top, expected_top);

    // Grouping within dir1
    let mut dir1: Vec<Entry> = db.list("dir1/", Some('/')).collect();
    dir1.sort();
    let expected_dir1: Vec<Entry> = vec![
        Entry::Key("dir1/file1".to_string()),
        Entry::CommonPrefix("dir1/subdir/".to_string()),
    ];
    assert_eq!(dir1, expected_dir1);
}
// Additional edge-case tests for radix db behavior

#[test]
fn empty_key() {
    let mut db = Database::new();
    db.insert("", None::<Vec<u8>>);
    let mut result: Vec<Entry> = db.list("", None::<char>).collect();
    result.sort();
    assert_eq!(result, vec![Entry::Key("".to_string())]);
}

#[test]
fn duplicate_insert() {
    let mut db = Database::new();
    db.insert("repeat", None::<Vec<u8>>);
    db.insert("repeat", None::<Vec<u8>>);
    let mut result: Vec<Entry> = db.list("", None::<char>).collect();
    result.sort();
    assert_eq!(result, vec![Entry::Key("repeat".to_string())]);
}

#[test]
fn prefix_key_cases() {
    let mut db = Database::new();
    let keys = ["a", "ab", "abc"];
    for &k in &keys { db.insert(k, None::<Vec<u8>>); }
    let mut r1: Vec<Entry> = db.list("a", None::<char>).collect();
    r1.sort();
    let expected1: Vec<Entry> = keys.iter().map(|&s| Entry::Key(s.to_string())).collect();
    assert_eq!(r1, expected1);
    let mut r2: Vec<Entry> = db.list("ab", None::<char>).collect();
    r2.sort();
    let expected2: Vec<Entry> = ["ab", "abc"].iter().map(|&s| Entry::Key(s.to_string())).collect();
    assert_eq!(r2, expected2);
    let mut r3: Vec<Entry> = db.list("abc", None::<char>).collect();
    r3.sort();
    let expected3 = vec![Entry::Key("abc".to_string())];
    assert_eq!(r3, expected3);
    let mut r4: Vec<Entry> = db.list("abcd", None::<char>).collect();
    r4.sort();
    assert!(r4.is_empty());
}

#[test]
fn ordering_of_keys() {
    let mut db = Database::new();
    for &k in &["b", "a", "c"] { db.insert(k, None::<Vec<u8>>); }
    let mut result: Vec<Entry> = db.list("", None::<char>).collect();
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
    let mut db = Database::new();
    db.insert("hello", None::<Vec<u8>>);
    let mut result: Vec<Entry> = db.list("helz", None::<char>).collect();
    result.sort();
    assert!(result.is_empty());
}

#[test]
fn unicode_keys() {
    let mut db = Database::new();
    let words = ["こんにちは", "こんばんは", "こん"];
    for &w in &words { db.insert(w, None::<Vec<u8>>); }
    let mut result: Vec<Entry> = db.list("こん", None::<char>).collect();
    result.sort();
    let mut expected: Vec<Entry> = words.iter().map(|&s| Entry::Key(s.to_string())).collect();
    expected.sort();
    assert_eq!(result, expected);
}

#[test]
fn unicode_common_prefix() {
    let mut db = Database::new();
    let words = ["🤖robot", "🤖romantic"];
    for &w in &words { db.insert(w, None::<Vec<u8>>); }
    let mut r1: Vec<Entry> = db.list("🤖", None::<char>).collect();
    r1.sort();
    let mut exp1: Vec<Entry> = words.iter().map(|&s| Entry::Key(s.to_string())).collect();
    exp1.sort();
    assert_eq!(r1, exp1);
    let mut r2: Vec<Entry> = db.list("🤖rob", None::<char>).collect();
    r2.sort();
    assert_eq!(r2, vec![Entry::Key("🤖robot".to_string())]);
}

#[test]
fn multi_delimiter_grouping() {
    let mut db = Database::new();
    let paths = ["foo/bar/baz", "foo/bar/qux"];
    for &p in &paths { db.insert(p, None::<Vec<u8>>); }
    let mut r1: Vec<Entry> = db.list("foo/", Some('/')).collect();
    r1.sort();
    let exp1 = vec![Entry::CommonPrefix("foo/bar/".to_string())];
    assert_eq!(r1, exp1);
    let mut r2: Vec<Entry> = db.list("foo/bar/", Some('/')).collect();
    r2.sort();
    let exp2: Vec<Entry> = paths.iter().map(|&s| Entry::Key(s.to_string())).collect();
    assert_eq!(r2, exp2);
}

#[test]
fn delimiter_edge_cases() {
    let mut db = Database::new();
    let paths = ["/a", "b/"];
    for &p in &paths { db.insert(p, None::<Vec<u8>>); }
    let mut r1: Vec<Entry> = db.list("", Some('/')).collect();
    r1.sort();
    let exp1 = vec![
        Entry::CommonPrefix("/".to_string()),
        Entry::CommonPrefix("b/".to_string()),
    ];
    assert_eq!(r1, exp1);
    let mut r2: Vec<Entry> = db.list("/", Some('/')).collect();
    r2.sort();
    assert_eq!(r2, vec![Entry::Key("/a".to_string())]);
}

#[test]
fn only_delimiter_key() {
    let mut db = Database::new();
    db.insert("/", None::<Vec<u8>>);
    // Plain listing
    let mut plain: Vec<Entry> = db.list("", None::<char>).collect();
    plain.sort();
    assert_eq!(plain, vec![Entry::Key("/".to_string())]);
    // Grouping at root
    let mut grp: Vec<Entry> = db.list("", Some('/')).collect();
    grp.sort();
    assert_eq!(grp, vec![Entry::CommonPrefix("/".to_string())]);
    // Listing after prefix '/'
    let mut after_grp: Vec<Entry> = db.list("/", Some('/')).collect();
    after_grp.sort();
    assert_eq!(after_grp, vec![Entry::CommonPrefix("/".to_string())]);
    let mut after_plain: Vec<Entry> = db.list("/", None::<char>).collect();
    after_plain.sort();
    assert_eq!(after_plain, vec![Entry::Key("/".to_string())]);
}

#[test]
fn trailing_delimiter_key() {
    let mut db = Database::new();
    db.insert("a/", None::<Vec<u8>>);
    // Grouping at root should yield the full 'a/' as a common prefix
    let mut grp: Vec<Entry> = db.list("", Some('/')).collect();
    grp.sort();
    assert_eq!(grp, vec![Entry::CommonPrefix("a/".to_string())]);
    // Listing after prefix 'a/'
    let mut after: Vec<Entry> = db.list("a/", Some('/')).collect();
    after.sort();
    assert_eq!(after, vec![Entry::CommonPrefix("a/".to_string())]);
    // Plain listing still returns the key
    let mut plain: Vec<Entry> = db.list("", None::<char>).collect();
    plain.sort();
    assert_eq!(plain, vec![Entry::Key("a/".to_string())]);
}

#[test]
fn delimiter_at_start_and_end() {
    let mut db = Database::new();
    let key = "/start/";
    db.insert(key, None::<Vec<u8>>);
    // Grouping at root picks up first '/'
    let mut root_grp: Vec<Entry> = db.list("", Some('/')).collect();
    root_grp.sort();
    assert_eq!(root_grp, vec![Entry::CommonPrefix("/".to_string())]);
    // After consuming first '/', grouping yields the full '/start/'
    let mut mid_grp: Vec<Entry> = db.list("/", Some('/')).collect();
    mid_grp.sort();
    assert_eq!(mid_grp, vec![Entry::CommonPrefix(key.to_string())]);
    // After full prefix, listing yields the key as a common-prefix (ends with '/')
    let mut end_grp: Vec<Entry> = db.list(key, Some('/')).collect();
    end_grp.sort();
    assert_eq!(end_grp, vec![Entry::CommonPrefix(key.to_string())]);
}

#[test]
fn multiple_leading_delimiters() {
    let mut db = Database::new();
    db.insert("//foo", None::<Vec<u8>>);
    // At root, first slash groups '/'
    let mut r1: Vec<Entry> = db.list("", Some('/')).collect();
    r1.sort();
    assert_eq!(r1, vec![Entry::CommonPrefix("/".to_string())]);
    // After one '/', grouping yields '//' as common prefix
    let mut r2: Vec<Entry> = db.list("/", Some('/')).collect();
    r2.sort();
    assert_eq!(r2, vec![Entry::CommonPrefix("//".to_string())]);
    // After '//' prefix, the remaining key is 'foo'
    let mut r3: Vec<Entry> = db.list("//", Some('/')).collect();
    r3.sort();
    assert_eq!(r3, vec![Entry::Key("//foo".to_string())]);
}