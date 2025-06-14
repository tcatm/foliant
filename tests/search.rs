use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, Streamer, SearchIndexBuilder};
use serde_json::Value;
use std::error::Error;
use tempfile::tempdir;

/// Before building a search index, searching yields no results (empty stream).
#[test]
fn search_without_index_returns_empty() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("alpha", None);
    builder.insert("beta", None);
    builder.insert("alphabet", None);
    builder.close()?;

    let db = Database::<Value>::open(&base)?;
    let results: Vec<Entry<Value>> = db.search(None, "alp")?.collect();
    assert!(
        results.is_empty(),
        "expected no search results before index build"
    );
    Ok(())
}

/// Build a search index and verify substring queries return matching keys.
#[test]
fn search_with_index_returns_matches() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("alpha", None);
    builder.insert("beta", None);
    builder.insert("alphabet", None);
    builder.insert("bet", None);
    builder.close()?;

    // Build the search index
    {
        let mut db = Database::<Value>::open(&base)?;
        SearchIndexBuilder::build_index(&mut db, None)?;
    }

    let db = Database::<Value>::open(&base)?;
    // Search for substring "alp"
    let entries: Vec<Entry<Value>> = db.search(None, "alp")?.collect();
    let mut keys: Vec<String> = entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Key(s, _, _) => Some(s.clone()),
            _ => None,
        })
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["alpha".to_string(), "alphabet".to_string()]);
    Ok(())
}

/// Test prefix filtering on search results.
#[test]
fn search_with_prefix_filters_results() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("foo/bar", None);
    builder.insert("foo/baz", None);
    builder.insert("bar/foo", None);
    builder.insert("baz/bar", None);
    builder.close()?;

    // Build the search index
    {
        let mut db = Database::<Value>::open(&base)?;
        SearchIndexBuilder::build_index(&mut db, None)?;
    }

    let db = Database::<Value>::open(&base)?;
    
    // Search for "bar" without prefix - should find all
    let all_entries: Vec<Entry<Value>> = db.search(None, "bar")?.collect();
    let all_keys: Vec<String> = all_entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Key(s, _, _) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(all_keys.len(), 3); // foo/bar, bar/foo, baz/bar
    
    // Search for "bar" with prefix "foo/" - should only find foo/bar
    let prefix_entries: Vec<Entry<Value>> = db.search(Some("foo/"), "bar")?.collect();
    let prefix_keys: Vec<String> = prefix_entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Key(s, _, _) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(prefix_keys, vec!["foo/bar".to_string()]);
    Ok(())
}

/// Test case-insensitive search.
#[test]
fn search_is_case_insensitive() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("HelloWorld", None);
    builder.insert("helloworld", None);
    builder.insert("HELLOWORLD", None);
    builder.close()?;

    // Build the search index
    {
        let mut db = Database::<Value>::open(&base)?;
        SearchIndexBuilder::build_index(&mut db, None)?;
    }

    let db = Database::<Value>::open(&base)?;
    // Search for "hello" should find all variants
    let entries: Vec<Entry<Value>> = db.search(None, "hello")?.collect();
    assert_eq!(entries.len(), 3);
    
    // Search for "WORLD" should also find all variants
    let entries2: Vec<Entry<Value>> = db.search(None, "WORLD")?.collect();
    assert_eq!(entries2.len(), 3);
    Ok(())
}
