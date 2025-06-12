use std::io;
use std::path::Path;

use fst::automaton::Str;
use fst::map::{OpBuilder, Union};
use fst::Automaton;
use fst::IntoStreamer;
use fst::Streamer;
use regex_automata::sparse::SparseDFA;
use serde::de::DeserializeOwned;

use crate::entry::Entry;
use crate::error::{IndexError, Result};
use crate::multi_list::MultiShardListStreamer;
use crate::payload_store::{CborPayloadCodec, PayloadCodec};
use crate::shard::Shard;
use crate::streamer::{PrefixStream, Streamer as DbStreamer};
use crate::tag_index::TagIndex;

/// Filter shards to only those that contain the given prefix
fn shards_with_prefix<'a, V, C>(shards: &'a [Shard<V, C>], prefix: &[u8]) -> Vec<&'a Shard<V, C>>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    shards
        .iter()
        .filter(|shard| {
            let fst = shard.fst.as_fst();
            let mut node = fst.root();
            for &byte in prefix {
                if let Some(idx) = node.find_input(byte) {
                    let tr = node.transition(idx);
                    node = fst.node(tr.addr);
                } else {
                    return false;
                }
            }
            true
        })
        .collect()
}

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
    ) -> Result<Box<dyn DbStreamer<Item = Entry<V>, Cursor = Vec<u8>> + 'a>> {
        let delim_u8 = delimiter.map(|c| c as u8);
        let prefix_buf = prefix.as_bytes().to_vec();
        let ms = MultiShardListStreamer::new(&self.shards, prefix_buf.clone(), delim_u8);
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
            shards_with_prefix(&self.shards, pref.as_bytes())
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


    /// Stream all tags present in the database, optionally restricted to keys
    /// under the given prefix, yielding each tag and its total count
    /// (unioned across all shards' tag indexes).
    pub fn list_tags<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> Result<Box<dyn DbStreamer<Item = (String, usize), Cursor = Vec<u8>> + 'a>> {
        use std::collections::HashMap;
        use roaring::RoaringBitmap;
        
        if prefix.is_none() {
            // No prefix filtering - use the original optimized approach
            let tag_indices: Vec<&TagIndex> = self.shards
                .iter()
                .filter_map(|sh| sh.tags.as_ref())
                .collect();
            if tag_indices.is_empty() {
                struct EmptyTagCount;
                impl DbStreamer for EmptyTagCount {
                    type Item = (String, usize);
                    type Cursor = Vec<u8>;
                    fn cursor(&self) -> Self::Cursor { unimplemented!() }
                    fn seek(&mut self, _cursor: Self::Cursor) { unimplemented!() }
                    fn next(&mut self) -> Option<Self::Item> { None }
                }
                return Ok(Box::new(EmptyTagCount));
            }

            let mut op = OpBuilder::new();
            for idx in &tag_indices {
                op = op.add(idx.list_tags());
            }
            let stream = op.union().into_stream();

            struct TagCountStreamer<'a> {
                stream: Union<'a>,
                tag_indices: Vec<&'a TagIndex>,
            }
            impl<'a> DbStreamer for TagCountStreamer<'a> {
                type Item = (String, usize);
                type Cursor = Vec<u8>;
                fn cursor(&self) -> Self::Cursor { unimplemented!() }
                fn seek(&mut self, _cursor: Self::Cursor) { unimplemented!() }
                fn next(&mut self) -> Option<Self::Item> {
                    self.stream.next().map(|(tag_bytes, ivs)| {
                        let lowercase_tag = String::from_utf8_lossy(tag_bytes).into_owned();
                        
                        // Get original case from first tag index (hot path optimization)
                        let display_tag = if let Some(first_iv) = ivs.first() {
                            self.tag_indices[first_iv.index]
                                .get_original_case(&lowercase_tag)
                                .ok()
                                .flatten()
                                .unwrap_or_else(|| lowercase_tag.clone())
                        } else {
                            lowercase_tag.clone()
                        };
                        
                        let mut count = 0;
                        for iv in ivs {
                            let sub_bm = self.tag_indices[iv.index]
                                .get_bitmap(iv.value)
                                .expect("failed to decode roaring bitmap");
                            count += sub_bm.len() as usize;
                        }
                        (display_tag, count)
                    })
                }
            }

            return Ok(Box::new(TagCountStreamer { stream, tag_indices }));
        }
        
        // Prefix filtering required - collect all tags with proper prefix filtering
        let prefix_str = prefix.unwrap();
        let mut tag_counts: HashMap<String, usize> = HashMap::new();
        
        // For each shard, get the bitmap of entries matching the prefix
        for shard in &self.shards {
            if let Some(tag_index) = &shard.tags {
                // Build bitmap of document IDs that match the prefix
                let prefix_bitmap = {
                    let mut bitmap = RoaringBitmap::new();
                    let prefix_automaton = fst::automaton::Str::new(prefix_str).starts_with();
                    let mut stream = shard.fst.search(prefix_automaton).into_stream();
                    while let Some((_, output)) = stream.next() {
                        bitmap.insert(output as u32);
                    }
                    bitmap
                };
                
                // If no entries match the prefix in this shard, skip it
                if prefix_bitmap.is_empty() {
                    continue;
                }
                
                // For each tag in this shard, intersect with prefix bitmap
                let mut tag_stream = tag_index.list_tags().into_stream();
                while let Some((tag_bytes, packed)) = tag_stream.next() {
                    let lowercase_tag = String::from_utf8_lossy(tag_bytes).into_owned();
                    
                    // Get original case, fall back to lowercase if not available
                    let display_tag = tag_index.get_original_case(&lowercase_tag)
                        .unwrap_or(None)
                        .unwrap_or(lowercase_tag.clone());
                    
                    let tag_bitmap = tag_index.get_bitmap(packed)
                        .expect("failed to decode tag bitmap");
                    
                    // Count only entries that match both the tag and the prefix
                    let intersection = &tag_bitmap & &prefix_bitmap;
                    let count = intersection.len() as usize;
                    
                    if count > 0 {
                        *tag_counts.entry(display_tag).or_insert(0) += count;
                    }
                }
            }
        }
        
        // Convert to a sorted vector and create a simple streaming iterator
        let mut sorted_tags: Vec<(String, usize)> = tag_counts.into_iter().collect();
        sorted_tags.sort_by(|a, b| a.0.cmp(&b.0));
        
        struct VecTagStreamer {
            tags: std::vec::IntoIter<(String, usize)>,
        }
        impl DbStreamer for VecTagStreamer {
            type Item = (String, usize);
            type Cursor = Vec<u8>;
            fn cursor(&self) -> Self::Cursor { unimplemented!() }
            fn seek(&mut self, _cursor: Self::Cursor) { unimplemented!() }
            fn next(&mut self) -> Option<Self::Item> { self.tags.next() }
        }

        Ok(Box::new(VecTagStreamer { tags: sorted_tags.into_iter() }))
    }

    /// Stream all tag names present in the database, optionally restricted to keys
    /// under the given prefix, *without* computing per-tag counts. This is much faster
    /// for use-cases like shell autocompletion.
    pub fn list_tag_names<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> Result<Box<dyn DbStreamer<Item = String, Cursor = Vec<u8>> + 'a>> {
        // Pre-walk prefix across shards to skip those without matching keys
        let shards_to_query: Vec<&Shard<V, C>> = if let Some(pref) = prefix {
            shards_with_prefix(&self.shards, pref.as_bytes())
        } else {
            self.shards.iter().collect()
        };
        // Collect only shards that actually have a TagIndex
        let tag_indices: Vec<&TagIndex> = shards_to_query
            .into_iter()
            .filter_map(|sh| sh.tags.as_ref())
            .collect();
        if tag_indices.is_empty() {
            struct EmptyTagNames;
            impl DbStreamer for EmptyTagNames {
                type Item = String;
                type Cursor = Vec<u8>;

                fn cursor(&self) -> Self::Cursor {
                    unimplemented!("cursor not implemented");
                }

                fn seek(&mut self, _cursor: Self::Cursor) {
                    unimplemented!("seek not implemented");
                }

                fn next(&mut self) -> Option<Self::Item> {
                    None
                }
            }
            return Ok(Box::new(EmptyTagNames)
                as Box<dyn DbStreamer<Item = String, Cursor = Vec<u8>> + 'a>);
        }

        // Build a union of all tag-FST streams (empty prefix = all tags)
        let mut op = OpBuilder::new();
        for idx in &tag_indices {
            op = op.add(idx.list_tags());
        }
        let stream = op.union().into_stream();

        // Streamer that walks the merged FST and yields only the tag string
        struct TagNameStreamer<'a> {
            stream: Union<'a>,
            tag_indices: Vec<&'a TagIndex>,
        }
        impl<'a> DbStreamer for TagNameStreamer<'a> {
            type Item = String;
            type Cursor = Vec<u8>;

            fn cursor(&self) -> Self::Cursor {
                unimplemented!("cursor not implemented");
            }

            fn seek(&mut self, _cursor: Self::Cursor) {
                unimplemented!("seek not implemented");
            }

            fn next(&mut self) -> Option<Self::Item> {
                self.stream.next().map(|(tag_bytes, ivs)| {
                    let lowercase_tag = String::from_utf8_lossy(tag_bytes).into_owned();
                    
                    // Get original case from first tag index (hot path optimization)
                    if let Some(first_iv) = ivs.first() {
                        self.tag_indices[first_iv.index]
                            .get_original_case(&lowercase_tag)
                            .ok()
                            .flatten()
                            .unwrap_or(lowercase_tag)
                    } else {
                        lowercase_tag
                    }
                })
            }
        }

        Ok(Box::new(TagNameStreamer { stream, tag_indices })
            as Box<
                dyn DbStreamer<Item = String, Cursor = Vec<u8>> + 'a,
            >)
    }

    /// Return a slice of shards in the database.
    pub fn shards(&self) -> &[Shard<V, C>] {
        &self.shards
    }
    /// Mutable access to shards for attaching tag indices.
    pub fn shards_mut(&mut self) -> &mut [Shard<V, C>] {
        &mut self.shards
    }

    /// Load on-disk tag indexes (.tags) for each shard, attaching them if present.
    /// Shards without a .tags file are silently skipped.
    pub fn load_tag_index(&mut self) -> Result<()> {
        for shard in &mut self.shards {
            if let Ok(ti) = TagIndex::open(shard.idx_path()) {
                shard.tags = Some(ti);
            }
        }
        Ok(())
    }
}
