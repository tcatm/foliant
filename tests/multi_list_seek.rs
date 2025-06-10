//! Integration tests for MultiShardListStreamer cursor/seek functionality.
use foliant::multi_list::MultiShardListStreamer;
use foliant::payload_store::{CborPayloadCodec, PayloadStoreVersion};
use foliant::{DatabaseBuilder, Streamer};
use tempfile::tempdir;

/// Helper: build an on-disk database with the given keys (no payloads) and return its shards.
fn make_shards(keys: &[&str]) -> Vec<foliant::Shard<serde_cbor::Value, CborPayloadCodec>> {
    let dir = tempdir().unwrap();
    let idx_path = dir.path().join("testdb");
    let mut builder: DatabaseBuilder<serde_cbor::Value, CborPayloadCodec> =
        DatabaseBuilder::new(&idx_path, PayloadStoreVersion::V2).unwrap();
    for &k in keys {
        builder.insert(k, None);
    }
    // finalize index files
    builder.close().unwrap();
    // open the single shard from disk
    let idx_file = idx_path.with_extension("idx");
    vec![foliant::Shard::open(&idx_file).unwrap()]
}

/// Extract the string keys or common-prefixes from a stream.
fn collect_strs(
    stream: &mut MultiShardListStreamer<'_, serde_cbor::Value, CborPayloadCodec>,
) -> Vec<String> {
    let mut v = Vec::new();
    while let Some(e) = stream.next() {
        v.push(e.as_str().to_string());
    }
    v
}

#[test]
fn single_shard_cursor_seek() {
    let keys = ["a", "b", "c", "d", "e"];
    let shards = make_shards(&keys);
    let mut st = MultiShardListStreamer::new(&shards, Vec::new(), None);
    // page off two
    assert_eq!(st.next().unwrap().as_str(), "a");
    assert_eq!(st.next().unwrap().as_str(), "b");
    let c1 = st.cursor();
    // resume after 'b'
    st.seek(c1.clone());
    let rest: Vec<String> = collect_strs(&mut st);
    assert_eq!(rest, vec!["c", "d", "e"]);
    // seek to end
    let c_end = st.cursor();
    st.seek(c_end);
    assert!(st.next().is_none());
}

#[test]
fn multi_shard_interleaving_cursor_seek() {
    // Shard A has aa, ac, ad; Shard B has ab, ae, af
    let mut shards = make_shards(&["aa", "ac", "ad"]);
    shards.extend(make_shards(&["ab", "ae", "af"]));
    let mut st = MultiShardListStreamer::new(&shards, Vec::new(), None);
    // first two in lex order: aa, ab
    assert_eq!(st.next().unwrap().as_str(), "aa");
    assert_eq!(st.next().unwrap().as_str(), "ab");
    let c1 = st.cursor();
    st.seek(c1.clone());
    // next four: ac, ad, ae, af
    let next_vals = collect_strs(&mut st);
    assert_eq!(next_vals, vec!["ac", "ad", "ae", "af"]);
}

#[test]
fn prefix_restricted_cursor_seek() {
    let keys = ["foo", "foobar", "foox", "bar"];
    let shards = make_shards(&keys);
    // prefix = "foo"
    let pref = b"foo".to_vec();
    let mut st = MultiShardListStreamer::new(&shards, pref.clone(), None);
    // first emit "foo"
    assert_eq!(st.next().unwrap().as_str(), "foo");
    let c = st.cursor();
    st.seek(c.clone());
    // then foobar, foox
    assert_eq!(collect_strs(&mut st), vec!["foobar", "foox"]);
    // prefix not present: "fooa"
    let mut st2 = MultiShardListStreamer::new(&shards, b"fooa".to_vec(), None);
    assert_eq!(collect_strs(&mut st2), vec!["foobar", "foox"]);
}

#[test]
fn delimiter_cursor_seek() {
    let keys = ["a/1", "a/2", "b/1", "b/2"];
    let shards = make_shards(&keys);
    // group by '/'
    let mut st = MultiShardListStreamer::new(&shards, Vec::new(), Some(b'/'));
    // first CommonPrefix("a/")
    let e1 = st.next().unwrap();
    assert_eq!(e1.kind(), "CommonPrefix");
    assert_eq!(e1.as_str(), "a/");
    let c1 = st.cursor();
    st.seek(c1.clone());
    // next CommonPrefix("b/")
    let e2 = st.next().unwrap();
    assert_eq!(e2.as_str(), "b/");
    let c2 = st.cursor();
    st.seek(c2);
    // no more
    assert!(st.next().is_none());
}

#[test]
fn edge_cases_cursor_seek() {
    // very long key
    let long = "x".repeat(512);
    let shards = make_shards(&[&long]);
    let mut st = MultiShardListStreamer::new(&shards, Vec::new(), None);
    assert_eq!(st.next().unwrap().as_str(), long.as_str());
    let c = st.cursor();
    // seek shorter than prefix (empty)
    st.seek(Vec::new());
    // should re-emit full key
    assert_eq!(st.next().unwrap().as_str(), long.as_str());
    // seek beyond end
    st.seek(c.clone());
    assert!(st.next().is_none());
}

#[test]
fn repeated_seeks_cursor_seek() {
    let keys = ["1", "2", "3"];
    let shards = make_shards(&keys);
    let mut st = MultiShardListStreamer::new(&shards, Vec::new(), None);
    // next one
    assert_eq!(st.next().unwrap().as_str(), "1");
    let c1 = st.cursor();
    // seek back to same cursor (no-op)
    st.seek(c1.clone());
    assert_eq!(st.next().unwrap().as_str(), "2");
    let c2 = st.cursor();
    // seek backwards to first cursor
    st.seek(c1);
    assert_eq!(st.next().unwrap().as_str(), "2");
    // then to c2
    st.seek(c2);
    assert_eq!(st.next().unwrap().as_str(), "3");
}

// Test that `resume()` produces the same sequence as `new()` + `seek()`
#[test]
fn resume_initial_seek_equivalence() {
    let keys = ["a", "b", "c", "d"];
    let shards = make_shards(&keys);
    // Consume first two entries using new() + seek()
    let mut st1 = MultiShardListStreamer::new(&shards, Vec::new(), None);
    assert_eq!(st1.next().unwrap().as_str(), "a");
    assert_eq!(st1.next().unwrap().as_str(), "b");
    let cursor = st1.cursor();
    st1.seek(cursor.clone());
    let rest1: Vec<String> = collect_strs(&mut st1);

    // Use resume() to start directly after the same cursor
    let mut st2 = MultiShardListStreamer::resume(&shards, Vec::new(), None, cursor);
    let rest2: Vec<String> = collect_strs(&mut st2);

    assert_eq!(rest1, rest2);
}

/// Test that resume() correctly pages across delimiter groups (one-shot resume after first group)
#[test]
fn resume_delimiter_paging() {
    let keys = ["a/1", "a/2", "b/1", "b/2"];
    let shards = make_shards(&keys);
    // group by '/'
    let mut st1 = MultiShardListStreamer::new(&shards, Vec::new(), Some(b'/'));
    // first group
    let first = st1.next().unwrap().as_str().to_string();
    assert_eq!(first, "a/");
    let cursor = st1.cursor();
    // resume directly after that group via resume()
    let mut st2 = MultiShardListStreamer::resume(&shards, Vec::new(), Some(b'/'), cursor.clone());
    let second = st2.next().unwrap().as_str().to_string();
    assert_eq!(second, "b/");
    let cursor2 = st2.cursor();
    // resume past last group: no more entries
    let mut st3 = MultiShardListStreamer::resume(&shards, Vec::new(), Some(b'/'), cursor2);
    assert!(st3.next().is_none());
}

/// Test resume() with a non-empty prefix ending in the delimiter
/// to ensure grouped prefixes under that prefix page correctly.
#[test]
fn resume_delimiter_with_prefix() {
    let keys = ["pre/a/1", "pre/b/2", "pre/c/3"];
    let shards = make_shards(&keys);
    let prefix = b"pre/".to_vec();
    let delim = Some(b'/');
    // First group under prefix 'pre/'
    let mut st1 = MultiShardListStreamer::new(&shards, prefix.clone(), delim);
    let first = st1.next().unwrap().as_str().to_string();
    assert_eq!(first, "pre/a/");
    let cur1 = st1.cursor();
    // Resume directly after that group
    let mut st2 = MultiShardListStreamer::resume(&shards, prefix.clone(), delim, cur1.clone());
    let second = st2.next().unwrap().as_str().to_string();
    assert_eq!(second, "pre/b/");
    let cur2 = st2.cursor();
    // Resume after second group
    let mut st3 = MultiShardListStreamer::resume(&shards, prefix.clone(), delim, cur2.clone());
    let third = st3.next().unwrap().as_str().to_string();
    assert_eq!(third, "pre/c/");
    let cur3 = st3.cursor();
    // Resume after last group: no more entries
    let mut st4 = MultiShardListStreamer::resume(&shards, prefix, delim, cur3);
    assert!(st4.next().is_none());
}
