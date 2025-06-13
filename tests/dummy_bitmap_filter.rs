use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, Streamer, Shard};
use foliant::multi_list::{MultiShardListStreamer, LazyShardFilter};
use roaring::RoaringBitmap;
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

/// Test that a dummy bitmap filter prunes entries not in the bitmap.
#[test]
fn dummy_bitmap_filter_prunes_unmatched_keys() -> Result<(), Box<dyn Error>> {
    // Build a single-shard database with three keys: aa, bb, cc
    let td = tempdir()?;
    let idx = td.path().join("shard.idx");
    let mut dbb = DatabaseBuilder::<Value>::new(&idx, PAYLOAD_STORE_VERSION_V3)?;
    dbb.insert("aa", Some(Value::Text("X".into())));
    dbb.insert("bb", Some(Value::Text("Y".into())));
    dbb.insert("cc", Some(Value::Text("Z".into())));
    dbb.close()?;

    // Load the shard
    let mut db = Database::<Value>::new();
    db.add_shard(&idx)?;

    // Collect all entries to discover the LUT-ID for "bb"
    let all: Vec<Entry<Value>> = db.list("", None)?.collect();
    let bb_id = all
        .into_iter()
        .find_map(|e| match e {
            Entry::Key(k, ptr, _) if k == "bb" => Some(ptr as u32),
            _ => None,
        })
        .expect("entry 'bb' must exist");

    // Build a dummy filter that only allows the 'bb' ID
    struct DummyFilter(RoaringBitmap);
    impl<V, C> LazyShardFilter<V, C> for DummyFilter
    where
        V: serde::de::DeserializeOwned,
        C: foliant::payload_store::PayloadCodec,
    {
        fn compute_bitmap(&self, _shard: &Shard<V, C>) -> Result<Option<RoaringBitmap>, Box<dyn Error>> {
            Ok(Some(self.0.clone()))
        }
        
        fn debug_name(&self) -> &str {
            "DummyFilter"
        }
    }

    let mut bm = RoaringBitmap::new();
    bm.insert(bb_id);
    let filter = DummyFilter(bm);

    // Apply the dummy filter: only 'bb' should pass
    let stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        None,
        Some(Box::new(filter)),
    )?;
    let results: Vec<Entry<Value>> = stream.collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "bb");
    Ok(())
}

/// A more complex bitmap filter test: irregular depths in the FST and multiple matches.
#[test]
fn complex_bitmap_filter_irregular_depths() -> Result<(), Box<dyn Error>> {
    // Build a single-shard database with keys of irregular depths.
    let td = tempdir()?;
    let idx = td.path().join("shard.idx");
    let mut dbb = DatabaseBuilder::<Value>::new(&idx, PAYLOAD_STORE_VERSION_V3)?;
    let all_keys = vec![
        "alpha", "alphabet", "alphanumeric",
        "beta", "bet", "betamax", "betting",
        "gamma", "gamut", "gambit",
        "theta", "the", "then",
        "omicron", "omicronomic",
        "pi", "pie", "pig",
        "rho", "rhodium",
    ];
    for k in &all_keys {
        dbb.insert(k, None);
    }
    dbb.close()?;

    // Load the shard
    let mut db = Database::<Value>::new();
    db.add_shard(&idx)?;

    // Collect all entries to discover LUT-IDs for keys to filter
    let all_entries: Vec<Entry<Value>> = db.list("", None)?.collect();

    // Define which keys to include in the filter
    let mut filtered_keys = vec![
        String::from("alpha"), String::from("betamax"), String::from("betting"),
        String::from("gamut"), String::from("gambit"), String::from("theta"),
        String::from("omicronomic"), String::from("pie"), String::from("pig"),
        String::from("rho"), String::from("rhodium"),
    ];
    filtered_keys.sort();

    // Build a RoaringBitmap with the selected keys' IDs
    let mut bm = RoaringBitmap::new();
    for entry in all_entries {
        if let Entry::Key(ref k, ptr, _) = entry {
            if filtered_keys.contains(k) {
                bm.insert(ptr as u32);
            }
        }
    }
    assert_eq!(bm.len(), filtered_keys.len() as u64);

    // Dummy filter that returns the same bitmap for each shard
    struct DummyFilter(RoaringBitmap);
    impl<V, C> LazyShardFilter<V, C> for DummyFilter
    where
        V: serde::de::DeserializeOwned,
        C: foliant::payload_store::PayloadCodec,
    {
        fn compute_bitmap(&self, _shard: &Shard<V, C>) -> Result<Option<RoaringBitmap>, Box<dyn Error>> {
            Ok(Some(self.0.clone()))
        }
        
        fn debug_name(&self) -> &str {
            "DummyFilter"
        }
    }

    let filter = DummyFilter(bm);

    // Apply the dummy filter: only filtered_keys should pass
    let stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        None,
        Some(Box::new(filter)),
    )?;
    let results: Vec<Entry<Value>> = stream.collect();

    // Extract the result keys, sort, and compare
    let mut result_keys: Vec<String> = results
        .into_iter()
        .filter_map(|e| match e {
            Entry::Key(k, _, _) => Some(k),
            _ => None,
        })
        .collect();
    result_keys.sort();

    assert_eq!(result_keys.len(), filtered_keys.len());
    assert_eq!(result_keys, filtered_keys);
    Ok(())
}

/// Even more complex bitmap filter test that exercises delimiter grouping.
#[test]
fn complex_bitmap_filter_with_delimiter() -> Result<(), Box<dyn Error>> {
    // Build a single-shard database with keys at varying depths and delimiters.
    let td = tempdir()?;
    let idx = td.path().join("shard.idx");
    let mut dbb = DatabaseBuilder::<Value>::new(&idx, PAYLOAD_STORE_VERSION_V3)?;
    let all_keys = vec![
        "a/one", "a/two", "a/deeper/first", "a/deeper/second",
        "b/one", "b/two", "b/three",
        "c/solo",
        "d/deeper/x/y/z",
        "e", // no delimiter
    ];
    for k in &all_keys {
        dbb.insert(k, None);
    }
    dbb.close()?;

    // Load the shard and collect all entries to get LUT-IDs
    let mut db = Database::<Value>::new();
    db.add_shard(&idx)?;
    let all_entries: Vec<Entry<Value>> = db.list("", None)?.collect();

    // Pick a selection of keys to include in the bitmap filter
    let mut filtered_keys = vec![
        String::from("a/one"), String::from("a/deeper/second"),
        String::from("b/two"), String::from("b/three"),
        String::from("d/deeper/x/y/z"),
        String::from("e"),
    ];
    filtered_keys.sort();

    // Build a RoaringBitmap with the selected keys' IDs
    let mut bm = RoaringBitmap::new();
    for entry in all_entries {
        if let Entry::Key(ref k, ptr, _) = entry {
            if filtered_keys.contains(k) {
                bm.insert(ptr as u32);
            }
        }
    }
    assert_eq!(bm.len(), filtered_keys.len() as u64);

    // Dummy filter that returns the same bitmap for each shard
    struct DummyFilter(RoaringBitmap);
    impl<V, C> LazyShardFilter<V, C> for DummyFilter
    where
        V: serde::de::DeserializeOwned,
        C: foliant::payload_store::PayloadCodec,
    {
        fn compute_bitmap(&self, _shard: &Shard<V, C>) -> Result<Option<RoaringBitmap>, Box<dyn Error>> {
            Ok(Some(self.0.clone()))
        }
        
        fn debug_name(&self) -> &str {
            "DummyFilter"
        }
    }

    let filter = DummyFilter(bm);

    // Apply the dummy filter with '/' delimiter: expect grouping at first slash
    let stream = MultiShardListStreamer::new_with_filter(
        &db.shards(),
        Vec::new(),
        Some(b'/'),
        Some(Box::new(filter)),
    )?;
    let mut results: Vec<Entry<Value>> = stream.collect();
    // Sort by string for deterministic order
    results.sort_by(|a, b| a.as_str().cmp(b.as_str()));

    // We expect common prefixes for 'a/', 'b/', 'd/' and a standalone 'e'
    let expected_prefixes = vec![
        String::from("a/"), String::from("b/"),
        String::from("d/"), String::from("e"),
    ];
    let got: Vec<String> = results.iter().map(Entry::as_str).map(String::from).collect();
    assert_eq!(got, expected_prefixes);
    // Verify child counts match number of filtered keys under each prefix
    let mut counts = results.iter().filter_map(|e| match e {
        Entry::CommonPrefix(_, Some(n)) => Some(*n as usize),
        _ => None,
    });
    assert_eq!(counts.next(), Some(2)); // a/one + a/deeper/second
    assert_eq!(counts.next(), Some(2)); // b/two + b/three
    assert_eq!(counts.next(), Some(1)); // d/deeper/x/y/z
    // 'e' has no delimiter, appears as Key, count not provided
    Ok(())
}