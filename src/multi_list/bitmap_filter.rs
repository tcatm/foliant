use roaring::RoaringBitmap;
use serde::de::DeserializeOwned;
use crate::shard::Shard;
use crate::payload_store::PayloadCodec;
use std::error::Error;

/// Builds an optional RoaringBitmap for each shard to drive pruning and filtering.
///
/// For each shard:
/// - `Ok(Some(bitmap))` indicates this shard has matches; the streamer will
///   use `bitmap` to prune and filter by LUT-ID.
/// - `Ok(None)` indicates this shard has no matches and can be dropped entirely.
pub trait ShardBitmapFilter<V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// Build exactly one optional bitmap per shard.
    fn build_shard_bitmaps(
        &self,
        shards: &[Shard<V, C>],
    ) -> Result<Vec<Option<RoaringBitmap>>, Box<dyn Error>>;
}