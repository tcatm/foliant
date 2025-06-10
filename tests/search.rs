use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, Streamer, TantivyIndexBuilder};
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
        let mut dbb = Database::<Value>::open(&base)?;
        TantivyIndexBuilder::build_index(&mut dbb, None)?;
    }

    let db = Database::<Value>::open(&base)?;
    // Search for the terms "alpha" OR "alphabet"
    let entries: Vec<Entry<Value>> = db.search(None, "alpha alphabet")?.collect();
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
    builder.insert("band", None);
    builder.insert("banana", None);
    builder.insert("bandana", None);
    builder.insert("an", None);
    builder.close()?;

    // Build the search index
    {
        let mut dbb = Database::<Value>::open(&base)?;
        TantivyIndexBuilder::build_index(&mut dbb, None)?;
    }

    let db = Database::<Value>::open(&base)?;
    // Search for "band" OR "banana" OR "bandana" yields all three keys
    let entries: Vec<Entry<Value>> = db.search(None, "band banana bandana")?.collect();
    let mut all: Vec<String> = entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Key(s, _, _) => Some(s.clone()),
            _ => None,
        })
        .collect();
    all.sort();
    assert_eq!(
        all,
        vec![
            "banana".to_string(),
            "band".to_string(),
            "bandana".to_string()
        ]
    );

    // Restrict prefix to "band" yields band and bandana
    let entries_pref: Vec<Entry<Value>> = db.search(Some("band"), "band banana bandana")?.collect();
    let mut prefixed: Vec<String> = entries_pref
        .iter()
        .filter_map(|entry| match entry {
            Entry::Key(s, _, _) => Some(s.clone()),
            _ => None,
        })
        .collect();
    prefixed.sort();
    assert_eq!(prefixed, vec!["band".to_string(), "bandana".to_string()]);
    Ok(())
}
