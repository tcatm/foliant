use foliant::{Database, Entry};
use foliant::Streamer;

#[test]
fn insert_and_list_no_delimiter() {
    let mut trie = Database::new();
    let words = ["app", "apple", "appetite", "banana", "band", "bandage", "bandana"];
    for &w in &words {
        trie.insert(w, None::<Vec<u8>>);
    }
    let mut result: Vec<Entry> = trie.list("", None::<char>).collect();
    result.sort();
    let mut expected: Vec<Entry> = words
        .iter()
        .map(|&s| Entry::Key(s.to_string()))
        .collect();
    expected.sort();
    assert_eq!(result, expected);

    let mut app_list: Vec<Entry> = trie.list("app", None::<char>).collect();
    app_list.sort();
    let mut expected_app: Vec<Entry> = ["app", "apple", "appetite"]
        .iter()
        .map(|&s| Entry::Key(s.to_string()))
        .collect();
    expected_app.sort();
    assert_eq!(app_list, expected_app);
}

#[test]
fn list_with_delimiter() {
    let mut trie = Database::new();
    trie.insert("foo/bar/1", None::<Vec<u8>>);
    trie.insert("foo/bar/2", None::<Vec<u8>>);
    trie.insert("foo/baz/1", None::<Vec<u8>>);
    trie.insert("foobar", None::<Vec<u8>>);
    let mut list_all: Vec<Entry> = trie.list("foo", None::<char>).collect();
    list_all.sort();
    let expected_all: Vec<Entry> = vec![
        Entry::Key("foo/bar/1".to_string()),
        Entry::Key("foo/bar/2".to_string()),
        Entry::Key("foo/baz/1".to_string()),
        Entry::Key("foobar".to_string()),
    ];
    assert_eq!(list_all, expected_all);

    let mut list_grp: Vec<Entry> = trie.list("foo", Some('/')).collect();
    list_grp.sort();
    let expected_grp: Vec<Entry> = vec![
        Entry::CommonPrefix("foo/".to_string()),
        Entry::Key("foobar".to_string()),
    ];
    assert_eq!(list_grp, expected_grp);
}

#[test]
fn nonexistent_prefix() {
    let mut trie = Database::new();
    trie.insert("hello", None::<Vec<u8>>);
    let mut result: Vec<Entry> = trie.list("world", None::<char>).collect();
    result.sort();
    assert!(result.is_empty());
}

#[test]
fn sample_path_delimiter_query() {
    let mut trie = Database::new();
    let paths = [
        "dir1/file1.txt",
        "dir1/subdir/file2.txt",
        "dir2/file3.txt",
        "readme.md",
    ];
    for &p in &paths {
        trie.insert(p, None::<Vec<u8>>);
    }
    let mut top: Vec<Entry> = trie.list("", Some('/')).collect();
    top.sort();
    let expected_top: Vec<Entry> = vec![
        Entry::CommonPrefix("dir1/".to_string()),
        Entry::CommonPrefix("dir2/".to_string()),
        Entry::Key("readme.md".to_string()),
    ];
    assert_eq!(top, expected_top);

    let mut dir1: Vec<Entry> = trie.list("dir1/", Some('/')).collect();
    dir1.sort();
    let expected_dir1: Vec<Entry> = vec![
        Entry::Key("dir1/file1.txt".to_string()),
        Entry::CommonPrefix("dir1/subdir/".to_string()),
    ];
    assert_eq!(dir1, expected_dir1);

    let mut subdir: Vec<Entry> = trie.list("dir1/subdir/", Some('/')).collect();
    subdir.sort();
    let expected_subdir: Vec<Entry> = vec![
        Entry::Key("dir1/subdir/file2.txt".to_string()),
    ];
    assert_eq!(subdir, expected_subdir);
}