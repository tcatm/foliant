use std::collections::HashMap;
use std::fs::{read_dir, File};
use std::io;
use std::path::Path;

use fst::automaton::Str;
use fst::map::{Map, OpBuilder};
use fst::Automaton;
use fst::IntoStreamer;
use fst::Streamer;
use regex_automata::sparse::SparseDFA;
use serde::de::DeserializeOwned;

use crate::entry::Entry;
use crate::error::{IndexError, Result};
use crate::lookup_table_store::LookupTableStore;
use crate::multi_list::MultiShardListStreamer;
use crate::payload_store::PayloadStore;
use crate::shard::Shard;
use crate::streamer::{PrefixStream, Streamer as DbStreamer};
use crate::tag_index::TagIndexBuilder;
use memmap2::Mmap;
use roaring::RoaringBitmap;

/// Read-only database: union of one or more shards (FST maps + payload stores)
pub struct Database<V = serde_cbor::Value>
where
    V: DeserializeOwned,
{
    shards: Vec<Shard<V>>,
}

/// Mode for combining multiple tags in `list_by_tags`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum TagMode {
    /// Entries must match ALL specified tags (intersection).
    And,
    /// Entries may match ANY of the specified tags (union).
    Or,
}

impl<V> Database<V>
where
    V: DeserializeOwned,
{
    /// Create an empty database (no shards).
    pub fn new() -> Self {
        Database { shards: Vec::new() }
    }

    /// Add one shard (opened from `<base>.idx` and `<base>.payload`).
    pub fn add_shard<P: AsRef<Path>>(&mut self, base: P) -> Result<()> {
        let shard = Shard::open(base)?;
        self.shards.push(shard);
        Ok(())
    }

    /// Open a database from either a single shard (file) or a directory of shards.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let base = path.as_ref();
        let mut db = Database::new();
        if base.is_dir() {
            for entry in read_dir(base)? {
                let ent = entry?;
                let p = ent.path();
                if p.extension().and_then(|s| s.to_str()) != Some("idx") {
                    continue;
                }
                let stem = p
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .ok_or(IndexError::InvalidFormat("invalid shard file name"))?;
                let b = base.join(stem);
                if !b.with_extension("payload").exists() {
                    continue;
                }

                if let Err(e) = db.add_shard(&b) {
                    eprintln!("warning: failed to add shard {:?}: {}", b, e);
                    continue;
                }
            }
        } else {
            db.add_shard(base)?;
        }
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
    pub fn grep<'a>(&'a self, prefix: Option<&'a str>, re: &str) -> Result<PrefixStream<'a, V>> {
        let dfa = SparseDFA::new(re).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("regex error: {}", e),
            ))
        })?;
        let prefix_str = prefix.unwrap_or("");
        let start = Str::new(prefix_str).starts_with();
        let relevant_shards: Vec<&Shard<V>> = if prefix_str.is_empty() {
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
    pub fn shards(&self) -> &[Shard<V>] {
        &self.shards
    }
}

/// Build or rebuild the tag index files by scanning existing JSON payloads for the given tag field.
///
/// This implements a two-pass tag-index process: if you deferred tag extraction during
/// initial indexing (or omitted --tag-field), you can generate the `<base>.tags` files
/// later by invoking this function with the name of the JSON field storing an array of tags.
pub fn build_tags_index<P: AsRef<Path>>(path: P, tag_field: &str) -> Result<()> {
    let base = path.as_ref();
    if base.is_dir() {
        for entry in read_dir(base)? {
            let ent = entry?;
            let p = ent.path();
            if p.extension().and_then(|s| s.to_str()) != Some("idx") {
                continue;
            }
            let stem = p
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or(IndexError::InvalidFormat("invalid shard file name"))?;
            let shard_base = base.join(stem);
            build_tags_index_file(&shard_base, tag_field)?;
        }
    } else {
        build_tags_index_file(base, tag_field)?;
    }
    Ok(())
}

/// Helper: scan the payloads for a single shard and write its `<base>.tags` file.
fn build_tags_index_file(base: &Path, tag_field: &str) -> Result<()> {
    let idx_path = base.with_extension("idx");
    let idx_file = File::open(&idx_path)?;
    let idx_mmap = unsafe { Mmap::map(&idx_file)? };
    let idx_map = Map::new(idx_mmap)?;

    let payload = PayloadStore::<serde_json::Value>::open(&base.with_extension("payload"))?;
    let lookup = LookupTableStore::open(&base.with_extension("lookup"))?;

    let mut builder = TagIndexBuilder::new(base);
    let mut stream = idx_map.stream();
    while let Some((_, weight)) = stream.next() {
        let shard_ptr = weight as u32;
        let lut = lookup.get(shard_ptr)?;
        let payload_ptr = lut.payload_ptr;
        if let Some(val) = payload.get(payload_ptr)? {
            if let Some(arr) = val.get(tag_field).and_then(|v| v.as_array()) {
                builder.insert_tags(shard_ptr, arr.iter().filter_map(|v| v.as_str()));
            }
        }
    }
    builder.finish()?;
    Ok(())
}
