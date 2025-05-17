use index::{Trie, Entry};
mod common;
use common::collect_sorted;

#[test]
fn insert_and_list_no_delimiter() {
    let mut trie = Trie::new();
    let words = ["app", "apple", "appetite", "banana", "band", "bandage", "bandana"];
    for &w in &words {
        trie.insert(w);
    }
    let result = collect_sorted(trie.list("", None));
    let mut expected: Vec<Entry> = words
        .iter()
        .map(|&s| Entry::Key(s.to_string()))
        .collect();
    expected.sort();
    assert_eq!(result, expected);

    let app_list = collect_sorted(trie.list("app", None));
    let mut expected_app: Vec<Entry> = ["app", "apple", "appetite"]
        .iter()
        .map(|&s| Entry::Key(s.to_string()))
        .collect();
    expected_app.sort();
    assert_eq!(app_list, expected_app);
}

#[test]
fn list_with_delimiter() {
    let mut trie = Trie::new();
    trie.insert("foo/bar/1");
    trie.insert("foo/bar/2");
    trie.insert("foo/baz/1");
    trie.insert("foobar");
    let list_all = collect_sorted(trie.list("foo", None));
    let expected_all: Vec<Entry> = vec![
        Entry::Key("foo/bar/1".to_string()),
        Entry::Key("foo/bar/2".to_string()),
        Entry::Key("foo/baz/1".to_string()),
        Entry::Key("foobar".to_string()),
    ];
    assert_eq!(list_all, expected_all);

    let list_grp = collect_sorted(trie.list("foo", Some('/')));
    let expected_grp: Vec<Entry> = vec![
        Entry::CommonPrefix("foo/".to_string()),
        Entry::Key("foobar".to_string()),
    ];
    assert_eq!(list_grp, expected_grp);
}

#[test]
fn nonexistent_prefix() {
    let mut trie = Trie::new();
    trie.insert("hello");
    let result = collect_sorted(trie.list("world", None));
    assert!(result.is_empty());
}

#[test]
fn sample_path_delimiter_query() {
    let mut trie = Trie::new();
    let paths = [
        "dir1/file1.txt",
        "dir1/subdir/file2.txt",
        "dir2/file3.txt",
        "readme.md",
    ];
    for &p in &paths {
        trie.insert(p);
    }
    let top = collect_sorted(trie.list("", Some('/')));
    let expected_top: Vec<Entry> = vec![
        Entry::CommonPrefix("dir1/".to_string()),
        Entry::CommonPrefix("dir2/".to_string()),
        Entry::Key("readme.md".to_string()),
    ];
    assert_eq!(top, expected_top);

    let dir1 = collect_sorted(trie.list("dir1/", Some('/')));
    let expected_dir1: Vec<Entry> = vec![
        Entry::Key("dir1/file1.txt".to_string()),
        Entry::CommonPrefix("dir1/subdir/".to_string()),
    ];
    assert_eq!(dir1, expected_dir1);

    let subdir = collect_sorted(trie.list("dir1/subdir/", Some('/')));
    let expected_subdir: Vec<Entry> = vec![
        Entry::Key("dir1/subdir/file2.txt".to_string()),
    ];
    assert_eq!(subdir, expected_subdir);
}