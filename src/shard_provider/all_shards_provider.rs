use crate::Shard;
use crate::payload_store::PayloadCodec;
use serde::de::DeserializeOwned;
use super::ShardProvider;

/// Simple implementation that provides all shards without filtering
pub struct AllShardsProvider<'a, V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    shards: &'a [Shard<V, C>],
}

impl<'a, V, C> AllShardsProvider<'a, V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    pub fn new(shards: &'a [Shard<V, C>]) -> Self {
        Self { shards }
    }
}

impl<'a, V, C> ShardProvider<V, C> for AllShardsProvider<'a, V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    fn get_all_shards(&self) -> &[Shard<V, C>] {
        self.shards
    }
    
    fn get_shards_for_prefix(&self, _prefix: &[u8]) -> roaring::RoaringBitmap {
        // Return all shard indices
        let mut bitmap = roaring::RoaringBitmap::new();
        for i in 0..self.shards.len() {
            bitmap.insert(i as u32);
        }
        bitmap
    }
}