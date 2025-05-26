use std::fs::read_dir;
use std::path::Path;
use std::io;

use serde::de::DeserializeOwned;
use regex_automata::sparse::SparseDFA;
use fst::automaton::Str;
use fst::Automaton;
use fst::map::OpBuilder;
use fst::IntoStreamer;
use fst::Streamer as FstStreamer;

use crate::error::{IndexError, Result};
use crate::shard::Shard;
use crate::multi_list::MultiShardListStreamer;
use crate::streamer::{PrefixStream, Streamer};
use crate::entry::Entry;

/// Read-only database: union of one or more shards (FST maps + payload stores)
pub struct Database<V = serde_cbor::Value>
where
    V: DeserializeOwned,
{
    shards: Vec<Shard<V>>,
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
    ) -> Result<Box<dyn Streamer<Item = Entry<V>> + 'a>> {
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
        let dfa = SparseDFA::new(re)
            .map_err(|e| IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("regex error: {}", e),
            )))?;
        let prefix_str = prefix.unwrap_or("");
        let start = Str::new(prefix_str).starts_with();
        let mut relevant_shards: Vec<&Shard<V>> = Vec::new();
        if prefix_str.is_empty() {
            for shard in &self.shards {
                relevant_shards.push(shard);
            }
        } else {
            for shard in &self.shards {
                let mut s = shard.fst.search(start.clone()).into_stream();
                if s.next().is_some() {
                    relevant_shards.push(shard);
                }
            }
        }
        let automaton = dfa.intersection(start);
        let mut op = OpBuilder::new();
        for shard in &relevant_shards {
            op = op.add(shard.fst.search(automaton.clone()));
        }
        let stream = op.union().into_stream();
        Ok(PrefixStream { stream, shards: relevant_shards })
    }

    /// Retrieve the payload for `key`, if any.
    pub fn get_value(&self, key: &str) -> Result<Option<V>> {
        for shard in &self.shards {
            if let Some(lut_ptr) = shard.fst.get(key) {
                let lookup = shard.lookup.get(lut_ptr)
                    .map_err(|e| IndexError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        format!("lookup error: {}", e),
                    )))?;
                let v = shard.payload.get(lookup.payload_ptr)
                    .map_err(|e| IndexError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        format!("payload error: {}", e),
                    )))?;
                return Ok(v);
            }
        }
        Ok(None)
    }

    /// Total number of keys across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.fst.len()).sum()
    }
}
