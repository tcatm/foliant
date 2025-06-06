use std::collections::HashMap;
use std::io;
use std::path::Path;

use fst::automaton::Str;
use fst::map::OpBuilder;
use fst::Automaton;
use fst::IntoStreamer;
use regex_automata::sparse::SparseDFA;
use serde::de::DeserializeOwned;

use crate::entry::Entry;
use crate::error::{IndexError, Result};
use crate::multi_list::MultiShardListStreamer;
use crate::payload_store::{CborPayloadCodec, PayloadCodec};
use crate::shard::Shard;
use crate::streamer::{PrefixStream, Streamer as DbStreamer};
use roaring::RoaringBitmap;

/// Read-only database: union of one or more shards (FST maps + payload stores)
/// Read-only database: union of one or more shards (FST maps + payload stores)
pub struct Database<V = serde_cbor::Value, C: PayloadCodec = CborPayloadCodec>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    shards: Vec<Shard<V, C>>,
}

/// Mode for combining multiple tags in `list_by_tags`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum TagMode {
    /// Entries must match ALL specified tags (intersection).
    And,
    /// Entries may match ANY of the specified tags (union).
    Or,
}

impl<V, C> Database<V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// Create an empty database (no shards).
    pub fn new() -> Self {
        Database { shards: Vec::new() }
    }

    /// Add one shard by specifying the full `.idx` file path and corresponding `.payload` file.
    pub fn add_shard<P: AsRef<Path>>(&mut self, idx_path: P) -> Result<()> {
        let shard = Shard::<V, C>::open(idx_path.as_ref())?;
        self.shards.push(shard);
        Ok(())
    }

    /// Open a database by creating a new instance and adding the given shard.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut db = Database::<V, C>::new();
        db.add_shard(path)?;
        Ok(db)
    }

    /// List entries under `prefix`, grouping by `delimiter` if provided.
    ///
    /// Returns a stream of `Entry<V>` (keys or common prefixes), or an error if
    /// listing cannot be constructed.
    pub fn list<'a>(
        &'a self,
        prefix: &'a str,
        delimiter: Option<char>,
    ) -> Result<Box<dyn DbStreamer<Item = Entry<V>> + 'a>> {
        let delim_u8 = delimiter.map(|c| c as u8);
        let prefix_buf = prefix.as_bytes().to_vec();
        let ms = MultiShardListStreamer::new(&self.shards, prefix_buf, delim_u8);
        Ok(Box::new(ms))
    }

    /// Search for keys matching a regular expression, optionally restricted to a prefix.
    ///
    /// `prefix`: if `Some(s)`, only keys starting with `s` are considered.
    /// `re`: a regex string; matches are anchored to the full key.
    ///
    /// Returns a stream of `Entry<V>` (keys matching the regex).
    pub fn grep<'a>(&'a self, prefix: Option<&'a str>, re: &str) -> Result<PrefixStream<'a, V, C>> {
        let dfa = SparseDFA::new(re).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("regex error: {}", e),
            ))
        })?;
        let prefix_str = prefix.unwrap_or("");
        let start = Str::new(prefix_str).starts_with();
        let relevant_shards: Vec<&Shard<V, C>> = if prefix_str.is_empty() {
            self.shards.iter().collect()
        } else {
            self.shards
                .iter()
                .filter(|s| s.has_prefix(prefix_str))
                .collect()
        };
        let automaton = dfa.intersection(start);
        let mut op = OpBuilder::new();
        for shard in &relevant_shards {
            op = op.add(shard.fst.search(automaton.clone()));
        }
        let stream = op.union().into_stream();
        Ok(PrefixStream {
            stream,
            shards: relevant_shards,
        })
    }

    /// Fuzzy substring search using shard-local Tantivy search indexes,
    /// optionally restricted to keys starting with `prefix`.
    pub fn search<'a>(
        &'a self,
        prefix: Option<&'a str>,
        query: &str,
    ) -> Result<Box<dyn DbStreamer<Item = Entry<V>> + 'a>> {
        let mut streams: Vec<Box<dyn DbStreamer<Item = Entry<V>> + 'a>> = Vec::new();
        for shard in &self.shards {
            if shard.search.is_some() {
                streams.push(shard.stream_by_search(query, prefix)?);
            }
        }
        struct ChainStreamer<'a, V>
        where
            V: DeserializeOwned,
        {
            streams: Vec<Box<dyn DbStreamer<Item = Entry<V>> + 'a>>,
            idx: usize,
        }
        impl<'a, V> DbStreamer for ChainStreamer<'a, V>
        where
            V: DeserializeOwned,
        {
            type Item = Entry<V>;
            fn next(&mut self) -> Option<Self::Item> {
                while self.idx < self.streams.len() {
                    if let Some(item) = self.streams[self.idx].next() {
                        return Some(item);
                    }
                    self.idx += 1;
                }
                None
            }
        }
        Ok(Box::new(ChainStreamer { streams, idx: 0 }))
    }

    /// Retrieve the payload for `key`, if any.
    pub fn get_value(&self, key: &str) -> Result<Option<V>> {
        for shard in &self.shards {
            if let Some(v) = shard.get_value(key)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// Total number of keys across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    /// Reverse lookup by raw pointer: retrieve the key and optional value.
    pub fn get_key(&self, ptr: u64) -> Result<Option<crate::entry::Entry<V>>> {
        for shard in &self.shards {
            if let Some(entry) = shard.get_entry(ptr)? {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    /// List entries filtered by tags (include/exclude) under a given tag-mode, optionally restricted to a key prefix.
    ///
    /// `include_tags`: positive tags to match (combined by `mode`), or empty for no includes.
    /// `exclude_tags`: tags to exclude from the result (entries having any of these tags are dropped).
    pub fn list_by_tags<'a>(
        &'a self,
        include_tags: &[&str],
        exclude_tags: &[&str],
        mode: TagMode,
        prefix: Option<&'a str>,
    ) -> Result<Box<dyn DbStreamer<Item = Entry<V>> + 'a>> {
        let mut streams: Vec<Box<dyn DbStreamer<Item = Entry<V>> + 'a>> = Vec::new();
        for shard in &self.shards {
            streams.push(shard.stream_by_tags(include_tags, exclude_tags, mode, prefix)?);
        }
        struct ChainStreamer<'a, V>
        where
            V: DeserializeOwned,
        {
            streams: Vec<Box<dyn DbStreamer<Item = Entry<V>> + 'a>>,
            idx: usize,
        }
        impl<'a, V> DbStreamer for ChainStreamer<'a, V>
        where
            V: DeserializeOwned,
        {
            type Item = Entry<V>;
            fn next(&mut self) -> Option<Self::Item> {
                while self.idx < self.streams.len() {
                    if let Some(item) = self.streams[self.idx].next() {
                        return Some(item);
                    }
                    self.idx += 1;
                }
                None
            }
        }
        Ok(Box::new(ChainStreamer { streams, idx: 0 }))
    }

    /// List all tags present in the database, returning their counts.
    pub fn list_tags(&self) -> Result<Vec<(String, usize)>> {
        let mut tag_map: HashMap<String, RoaringBitmap> = HashMap::new();
        for shard in &self.shards {
            if let Some(idx) = &shard.tags {
                for tag in idx.list_tags()? {
                    if let Ok(Some(bm)) = idx.get(&tag) {
                        tag_map
                            .entry(tag)
                            .and_modify(|acc| *acc |= &bm)
                            .or_insert(bm);
                    }
                }
            }
        }
        let mut res: Vec<(String, usize)> = tag_map
            .into_iter()
            .map(|(tag, bm)| (tag, bm.len() as usize))
            .collect();
        res.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(res)
    }

    /// Return a slice of shards in the database.
    pub fn shards(&self) -> &[Shard<V, C>] {
        &self.shards
    }
    /// Mutable access to shards for attaching tag indices.
    pub fn shards_mut(&mut self) -> &mut [Shard<V, C>] {
        &mut self.shards
    }
}
