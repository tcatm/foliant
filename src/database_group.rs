use std::fs;
use std::path::Path;
use std::marker::PhantomData;
use serde::de::DeserializeOwned;
use crate::{Database, Entry, Streamer, Result, IndexError, ListStream};

/// A group of disjoint on-disk databases (shards), stored in a directory.
/// Each shard is represented by `<dir>/<name>.idx` and `<dir>/<name>.payload`.
/// Exposes the same API as a single Database, treating shards as virtual folders
/// (you can prefix with `name/` to drill into a specific shard).
pub struct DatabaseGroup<V>
where V: DeserializeOwned,
{
    shards: Vec<(String, Database<V>)>,
    /// Delimiter for splitting shard prefix (e.g. '/')
    delimiter: char,
}

impl<V> DatabaseGroup<V>
where V: DeserializeOwned,
{
    /// Create an empty group with the given delimiter (no shards).
    pub fn new(delimiter: char) -> Self {
        DatabaseGroup { shards: Vec::new(), delimiter }
    }

    /// Scan a directory for `.idx`/`.payload` shard pairs, open them, and return a sorted list.
    fn scan_dir<P: AsRef<Path>>(dir: P) -> Result<Vec<(String, Database<V>)>> {
        let dir = dir.as_ref();
        let mut shards = Vec::new();
        for entry in fs::read_dir(dir)? {
            let ent = entry?;
            let path = ent.path();
            if path.extension().and_then(|s| s.to_str()) != Some("idx") {
                continue;
            }
            let stem = path.file_stem()
                .and_then(|s| s.to_str())
                .ok_or(IndexError::InvalidFormat("invalid shard file name"))?;
            let base = dir.join(stem);
            if !base.with_extension("payload").exists() {
                continue;
            }
            let db = Database::open(&base)?;
            shards.push((stem.to_string(), db));
        }
        shards.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(shards)
    }

    /// Construct a DatabaseGroup by scanning `dir` for shard files, using `delimiter`.
    pub fn from_dir<P: AsRef<Path>>(dir: P, delimiter: char) -> Result<Self> {
        let shards = Self::scan_dir(dir)?;
        Ok(DatabaseGroup { shards, delimiter })
    }

    /// Open a directory of shards (legacy name for `from_dir`).
    pub fn open_dir<P: AsRef<Path>>(dir: P, delimiter: char) -> Result<Self> {
        Self::from_dir(dir, delimiter)
    }

    /// Add or replace a shard with the given `name` and `db` handle.
    /// If a shard by that name existed, it is removed.
    pub fn add_shard(&mut self, name: String, db: Database<V>) {
        if let Some(pos) = self.shards.iter().position(|(n, _)| n == &name) {
            self.shards.remove(pos);
        }
        self.shards.push((name, db));
        self.shards.sort_by(|a, b| a.0.cmp(&b.0));
    }

    /// Remove the shard named `name`, returning its Database handle if present.
    pub fn remove_shard(&mut self, name: &str) -> Option<Database<V>> {
        if let Some(pos) = self.shards.iter().position(|(n, _)| n == name) {
            let (_, db) = self.shards.remove(pos);
            Some(db)
        } else {
            None
        }
    }

    /// Re-scan the given directory and replace the shard list.
    pub fn reload_dir<P: AsRef<Path>>(&mut self, dir: P) -> Result<()> {
        self.shards = Self::scan_dir(dir)?;
        Ok(())
    }
}

/// Adapter to turn a simple `FnMut() -> Option<Entry<V>>` into our `Streamer` trait.
struct FromFnStreamer<'a, V, F>
where
    V: DeserializeOwned + 'a,
    F: FnMut() -> Option<Entry<V>> + 'a,
{
    f: F,
    _marker: PhantomData<&'a ()>,
}
impl<'a, V, F> Streamer for FromFnStreamer<'a, V, F>
where
    V: DeserializeOwned + 'a,
    F: FnMut() -> Option<Entry<V>> + 'a,
{
    type Item = Entry<V>;
    fn next(&mut self) -> Option<Self::Item> {
        (self.f)()
    }
}
/// Streamer that recursively lists across all shards (no delimiter grouping).
pub struct FlattenStreamer<'a, V>
where V: DeserializeOwned + 'a,
{
    shards: &'a [(String, Database<V>)],
    prefix: &'a str,
    idx: usize,
    inner: Option<ListStream<'a, V>>,
}
impl<'a, V> Streamer for FlattenStreamer<'a, V>
where V: DeserializeOwned + 'a,
{
    type Item = Entry<V>;
    fn next(&mut self) -> Option<Self::Item> {
        while self.idx < self.shards.len() {
            // Initialize inner if needed
            if self.inner.is_none() {
                let (shard_name, db) = &self.shards[self.idx];
                // Determine rest prefix (strip "shard/" if present)
                let shard_pref = shard_name.clone() + "/";
                let rest = if self.prefix.starts_with(&shard_pref) {
                    &self.prefix[shard_pref.len()..]
                } else {
                    self.prefix
                };
                // Open inner stream recursively (no grouping)
                self.inner = Some(db.list(rest, None));
            }
            // Consume inner stream
            if let Some(ref mut s) = self.inner {
                if let Some(entry) = s.next() {
                    // Prefix shard name
                    let shard_name = &self.shards[self.idx].0;
                    let pre = shard_name.clone() + "/";
                    return Some(match entry {
                        Entry::Key(k, v) => Entry::Key(pre.clone() + &k, v),
                        Entry::CommonPrefix(p) => Entry::CommonPrefix(pre.clone() + &p),
                    });
                }
            }
            // Move to next shard
            self.inner = None;
            self.idx += 1;
        }
        None
    }
}

/// Common trait for both single databases and sharded groups.
pub trait DatabaseAccess<V>
where V: DeserializeOwned,
{
    /// List entries under `prefix`, using optional `delimiter` for grouping.
    fn list<'a>(&'a self, prefix: &'a str, delimiter: Option<char>) -> Box<dyn Streamer<Item = Entry<V>> + 'a>;
    /// Grep (regex) under optional `prefix`.
    fn grep<'a>(
        &'a self,
        prefix: Option<&'a str>,
        re: &str,
    ) -> Result<Box<dyn Streamer<Item = Entry<V>> + 'a>>;
    /// Get the payload for a given key; for sharded groups, key must be `shard/inner_key`.
    fn get_value(&self, key: &str, delimiter: Option<char>) -> Result<Option<V>>;
    /// Total number of keys (for groups, sum across shards).
    fn len(&self) -> usize;
}

// Implement DatabaseAccess for a single Database
impl<V> DatabaseAccess<V> for Database<V>
where V: DeserializeOwned,
{
    fn list<'a>(&'a self, prefix: &'a str, delimiter: Option<char>) -> Box<dyn Streamer<Item = Entry<V>> + 'a> {
        Box::new(self.list(prefix, delimiter))
    }
    fn grep<'a>(
        &'a self,
        prefix: Option<&'a str>,
        re: &str,
    ) -> Result<Box<dyn Streamer<Item = Entry<V>> + 'a>> {
        let s = self.grep(prefix, re)?;
        Ok(Box::new(s))
    }
    fn get_value(&self, key: &str, _delimiter: Option<char>) -> Result<Option<V>> {
        self.get_value(key)
    }
    fn len(&self) -> usize {
        self.len()
    }
}

// Implement DatabaseAccess for a sharded group
impl<V> DatabaseAccess<V> for DatabaseGroup<V>
where V: DeserializeOwned,
{
    fn list<'a>(&'a self, prefix: &'a str, delimiter: Option<char>) -> Box<dyn Streamer<Item = Entry<V>> + 'a> {
        // If no delimiter grouping, flatten across all shards
        if delimiter.is_none() {
            return Box::new(FlattenStreamer {
                shards: &self.shards,
                prefix,
                idx: 0,
                inner: None,
            });
        }
        // Delimiter grouping mode: use delimiter (default '/')
        let d = delimiter.unwrap_or('/');
        // If prefix selects a specific shard, drill into that shard
        if let Some((shard, rest)) = prefix.split_once(d) {
            if let Some((name, db)) = self.shards.iter().find(|(n, _)| n == shard) {
                let mut inner = db.list(rest, Some(d));
                let pre = format!("{}{}", name, d);
                let streamer = FromFnStreamer { f: move || {
                    inner.next().map(|e| match e {
                        Entry::Key(k, v) => Entry::Key(pre.clone() + &k, v),
                        Entry::CommonPrefix(p) => Entry::CommonPrefix(pre.clone() + &p),
                    })
                }, _marker: PhantomData };
                return Box::new(streamer);
            }
            // unknown shard => empty
            return Box::new(FromFnStreamer { f: || None, _marker: PhantomData });
        }
        // Top-level: list shard names as common prefixes
        let mut idx = 0;
        let shards = &self.shards;
        let pat = prefix.to_string();
        let streamer = FromFnStreamer { f: move || {
            while idx < shards.len() {
                let name = &shards[idx].0;
                idx += 1;
                if name.starts_with(&pat) {
                    return Some(Entry::CommonPrefix(name.clone() + &d.to_string()));
                }
            }
            None
        }, _marker: PhantomData };
        Box::new(streamer)
    }

    fn grep<'a>(
        &'a self,
        prefix: Option<&'a str>,
        re: &str,
    ) -> Result<Box<dyn Streamer<Item = Entry<V>> + 'a>> {
        let d = self.delimiter;
        // If prefix selects a specific shard (shard_name/rest), delegate to that shard
        if let Some(pfx) = prefix {
            if let Some((shard, rest)) = pfx.split_once(d) {
                if let Some((name, db)) = self.shards.iter().find(|(n, _)| n == shard) {
                    let mut inner = db.grep(Some(rest), re)?;
                    let pre = format!("{}{}", name, d);
                    let streamer = FromFnStreamer {
                        f: move || {
                            inner.next().map(|e| match e {
                                Entry::Key(k, v) => Entry::Key(pre.clone() + &k, v),
                                Entry::CommonPrefix(p) => Entry::CommonPrefix(pre.clone() + &p),
                            })
                        },
                        _marker: PhantomData,
                    };
                    return Ok(Box::new(streamer));
                }
            }
        }
        // Full-group grep across all shards, ignoring any prefix filtering
        // Build a grep stream for each shard
        let mut streams: Vec<(String, Box<dyn Streamer<Item = Entry<V>> + 'a>)> = Vec::new();
        for (name, db) in &self.shards {
            let inner = db.grep(None, re)?;
            // Box each shard's grep stream for dynamic dispatch
            streams.push((name.clone(), Box::new(inner) as Box<dyn Streamer<Item = Entry<V>> + 'a>));
        }
        let mut iter = streams.into_iter();
        let mut current: Option<(String, Box<dyn Streamer<Item = Entry<V>> + 'a>)> = None;
        let streamer = FromFnStreamer {
            f: move || {
                loop {
                    // If we have an active shard stream, drain it first
                    if let Some((ref name, ref mut s)) = current {
                        if let Some(entry) = s.next() {
                            let pre = name.clone() + &d.to_string();
                            return Some(match entry {
                                Entry::Key(k, v) => Entry::Key(pre.clone() + &k, v),
                                Entry::CommonPrefix(p) => Entry::CommonPrefix(pre.clone() + &p),
                            });
                        }
                    }
                    // Move to next shard's stream
                    match iter.next() {
                        Some((name, s)) => {
                            current = Some((name, s));
                            continue;
                        }
                        None => return None,
                    }
                }
            },
            _marker: PhantomData,
        };
        Ok(Box::new(streamer))
    }

    fn get_value(&self, key: &str, delimiter: Option<char>) -> Result<Option<V>> {
        let d = delimiter.unwrap_or('/');
        if let Some((shard, rest)) = key.split_once(d) {
            if let Some((_, db)) = self.shards.iter().find(|(n, _)| n == shard) {
                return db.get_value(rest);
            }
        }
        Ok(None)
    }

    fn len(&self) -> usize {
        self.shards.iter().map(|(_, db)| db.len()).sum()
    }
}