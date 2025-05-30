use foliant::{Database, DatabaseBuilder};
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn common_prefix_empty_shard() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_empty");
    let db: Database<Value> = DatabaseBuilder::<Value>::new(&base)?.into_database()?;
    let shards = db.shards();
    assert_eq!(shards.len(), 1);
    let shard = &shards[0];
    assert_eq!(shard.len(), 0);
    assert_eq!(shard.common_prefix(), "");
    Ok(())
}

#[test]
fn common_prefix_single_key() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_single");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("hello", None);
    let db = builder.into_database()?;
    let shard = &db.shards()[0];
    assert_eq!(shard.len(), 1);
    assert_eq!(shard.common_prefix(), "hello");
    Ok(())
}

#[test]
fn common_prefix_multiple_full_prefix() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_full");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("prefix/one", None);
    builder.insert("prefix/two", None);
    builder.insert("prefix/three", None);
    let db = builder.into_database()?;
    let shard = &db.shards()[0];
    assert_eq!(shard.len(), 3);
    assert_eq!(shard.common_prefix(), "prefix/");
    Ok(())
}

#[test]
fn common_prefix_multiple_partial_prefix() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_partial");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("foo123", None);
    builder.insert("foo456", None);
    builder.insert("foo789", None);
    let db = builder.into_database()?;
    let shard = &db.shards()[0];
    assert_eq!(shard.len(), 3);
    assert_eq!(shard.common_prefix(), "foo");
    Ok(())
}

#[test]
fn common_prefix_multiple_no_prefix() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_noprefix");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("alpha", None);
    builder.insert("beta", None);
    builder.insert("gamma", None);
    let db = builder.into_database()?;
    let shard = &db.shards()[0];
    assert_eq!(shard.len(), 3);
    assert_eq!(shard.common_prefix(), "");
    Ok(())
}

#[test]
fn common_prefix_unicode() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_unicode");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    builder.insert("こんにちは", None);
    builder.insert("こんにちわ", None);
    builder.insert("こんばんは", None);
    let db = builder.into_database()?;
    let shard = &db.shards()[0];
    assert_eq!(shard.len(), 3);
    assert_eq!(shard.common_prefix(), "こん");
    Ok(())
}

#[test]
fn shard_len_counts_inserted_keys() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db_len");
    let mut builder = DatabaseBuilder::<Value>::new(&base)?;
    let keys = ["a", "b", "c", ""];
    for &k in &keys {
        builder.insert(k, None);
    }
    let db = builder.into_database()?;
    let shard = &db.shards()[0];
    assert_eq!(shard.len(), keys.len());
    Ok(())
}