mod all_shards_provider;

pub use all_shards_provider::AllShardsProvider;

use crate::Shard;
use crate::payload_store::PayloadCodec;
use serde::de::DeserializeOwned;

/// Trait for providing shards to MultiShardListStreamer
pub trait ShardProvider<V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// Get all shards
    fn get_all_shards(&self) -> &[Shard<V, C>];
    
    /// Get candidate shards for a given prefix
    fn get_shards_for_prefix(&self, prefix: &[u8]) -> roaring::RoaringBitmap;
}