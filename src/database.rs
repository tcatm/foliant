mod tags;
mod prefix_index;

use std::io;
use std::path::Path;

use fst::automaton::Str;
use fst::map::OpBuilder;
use fst::{Automaton, IntoStreamer};
use regex_automata::sparse::SparseDFA;
use serde::de::DeserializeOwned;

use crate::entry::Entry;
use crate::error::{IndexError, Result};
use crate::multi_list::MultiShardListStreamer;
use crate::shard_provider::ShardProvider;
use crate::payload_store::{CborPayloadCodec, PayloadCodec};
use crate::shard::Shard;
use crate::streamer::{PrefixStream, Streamer as DbStreamer};
use self::prefix_index::PrefixIndex;


/// Filter shards to only those that contain the given prefix
fn shards_with_prefix<'a, V, C>(
    database: &'a Database<V, C>, 
    prefix: &[u8]
) -> Vec<&'a Shard<V, C>>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    // Get candidate shard indices from prefix index
    let candidate_bitmap = database.prefix_index.find_candidate_shards(prefix);
    
    // Now verify each candidate by walking its FST
    candidate_bitmap
        .iter()
        .filter_map(|idx| {
            let shard = &database.shards[idx as usize];
            let fst = shard.fst.as_fst();
            let mut node = fst.root();
            for &byte in prefix {
                if let Some(idx) = node.find_input(byte) {
                    let tr = node.transition(idx);
                    node = fst.node(tr.addr);
                } else {
                    return None;
                }
            }
            Some(shard)
        })
        .collect()
}

/// Generic wrapper to turn any Iterator into a DbStreamer
pub struct IterStreamer<I>(pub I);

impl<I, T> DbStreamer for IterStreamer<I>
where
    I: Iterator<Item = T>,
{
    type Item = T;
    type Cursor = Vec<u8>;
    
    fn cursor(&self) -> Self::Cursor {
        // TODO: implement proper cursor tracking
        Vec::new()
    }
    
    fn seek(&mut self, _cursor: Self::Cursor) {
        // TODO: implement proper seeking
        // For now, this is a stub - many use cases don't need cursor/seek
    }
    
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

/// Read-only database: union of one or more shards (FST maps + payload stores)
pub struct Database<V = serde_cbor::Value, C: PayloadCodec = CborPayloadCodec>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    shards: Vec<Shard<V, C>>,
    prefix_index: PrefixIndex,
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
        Database { 
            shards: Vec::new(),
            prefix_index: PrefixIndex::new(),
        }
    }

    /// Add one shard by specifying the full `.idx` file path and corresponding `.payload` file.
    pub fn add_shard<P: AsRef<Path>>(&mut self, idx_path: P) -> Result<()> {
        let shard = Shard::<V, C>::open(idx_path.as_ref())?;
        let shard_idx = self.shards.len();
        self.prefix_index.add_shard(&shard, shard_idx);
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
    ) -> Result<Box<dyn DbStreamer<Item = Entry<V>, Cursor = Vec<u8>> + 'a>> {
        let delim_u8 = delimiter.map(|c| c as u8);
        let prefix_buf = prefix.as_bytes().to_vec();
        
        // MultiShardListStreamer will use the prefix index to filter shards
        let ms = MultiShardListStreamer::new(self, prefix_buf.clone(), delim_u8);
        Ok(Box::new(ms)
            as Box<
                dyn DbStreamer<Item = Entry<V>, Cursor = Vec<u8>> + 'a,
            >)
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
    ) -> Result<Box<dyn DbStreamer<Item = Entry<V>, Cursor = Vec<u8>> + 'a>> {
        // Pre-walk prefix across shards to skip those without matching keys
        let shards_to_query: Vec<&Shard<V, C>> = if let Some(pref) = prefix {
            shards_with_prefix(self, pref.as_bytes())
        } else {
            self.shards.iter().collect()
        };
        let mut streams: Vec<Box<dyn DbStreamer<Item = Entry<V>, Cursor = Vec<u8>> + 'a>> =
            Vec::new();
        for shard in shards_to_query {
            if shard.search.is_some() {
                streams.push(shard.stream_by_search(query, prefix)?);
            }
        }
        struct ChainStreamer<'a, V>
        where
            V: DeserializeOwned,
        {
            streams: Vec<Box<dyn DbStreamer<Item = Entry<V>, Cursor = Vec<u8>> + 'a>>,
            idx: usize,
        }
        impl<'a, V> DbStreamer for ChainStreamer<'a, V>
        where
            V: DeserializeOwned,
        {
            type Item = Entry<V>;
            type Cursor = Vec<u8>;

            fn cursor(&self) -> Self::Cursor {
                unimplemented!("cursor not implemented");
            }

            fn seek(&mut self, _cursor: Self::Cursor) {
                unimplemented!("seek not implemented");
            }

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
        Ok(Box::new(ChainStreamer { streams, idx: 0 })
            as Box<
                dyn DbStreamer<Item = Entry<V>, Cursor = Vec<u8>> + 'a,
            >)
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

    /// Return a slice of shards in the database.
    pub fn shards(&self) -> &[Shard<V, C>] {
        &self.shards
    }
    
    /// Mutable access to shards for attaching tag indices.
    pub fn shards_mut(&mut self) -> &mut [Shard<V, C>] {
        &mut self.shards
    }
    
    /// Get a reference to the prefix index
    pub fn prefix_index(&self) -> &PrefixIndex {
        &self.prefix_index
    }
    
    /// Get candidate shards for a given prefix using the prefix index
    pub fn get_candidate_shards_for_prefix(&self, prefix: &[u8]) -> roaring::RoaringBitmap {
        self.prefix_index.find_candidate_shards(prefix)
    }
}

impl<V, C> ShardProvider<V, C> for Database<V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    fn get_all_shards(&self) -> &[Shard<V, C>] {
        &self.shards
    }
    
    fn get_shards_for_prefix(&self, prefix: &[u8]) -> roaring::RoaringBitmap {
        self.get_candidate_shards_for_prefix(prefix)
    }
}
