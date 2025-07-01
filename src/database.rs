mod tags;
mod prefix_index;

use std::path::Path;

use serde::de::DeserializeOwned;

use crate::entry::Entry;
use crate::error::Result;
use crate::shard_provider::ShardProvider;
use crate::payload_store::{CborPayloadCodec, PayloadCodec};
use crate::shard::Shard;
use crate::streamer::Streamer as DbStreamer;
use self::prefix_index::PrefixIndex;



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
        let ms = crate::multi_list::MultiShardListStreamer::new(self, prefix_buf.clone(), delim_u8);
        Ok(Box::new(ms)
            as Box<
                dyn DbStreamer<Item = Entry<V>, Cursor = Vec<u8>> + 'a,
            >)
    }
    
    /// List entries under `prefix` with optional lazy filter applied.
    ///
    /// Returns a stream of `Entry<V>` (keys or common prefixes), or an error if
    /// listing cannot be constructed.
    pub fn list_with_filter<'a>(
        &'a self,
        prefix: &'a str,
        delimiter: Option<char>,
        filter: Box<dyn crate::multi_list::LazyShardFilter<V, C>>,
    ) -> Result<Box<dyn DbStreamer<Item = Entry<V>, Cursor = Vec<u8>> + 'a>> {
        let delim_u8 = delimiter.map(|c| c as u8);
        let prefix_buf = prefix.as_bytes().to_vec();
        
        // MultiShardListStreamer will use the prefix index to filter shards
        let ms = crate::multi_list::MultiShardListStreamer::new_with_lazy_filter(
            self, 
            prefix_buf.clone(), 
            delim_u8,
            Some(filter)
        ).map_err(|e| crate::error::IndexError::Io(
            std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
        ))?;
        
        Ok(Box::new(ms)
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

    /// Reverse lookup by raw pointer from a specific shard.
    pub fn get_key_from_shard(&self, shard_idx: usize, ptr: u64) -> Result<Option<crate::entry::Entry<V>>> {
        if shard_idx >= self.shards.len() {
            return Ok(None);
        }
        self.shards[shard_idx].get_entry(ptr)
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
