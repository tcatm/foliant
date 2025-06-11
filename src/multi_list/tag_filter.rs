use serde::de::DeserializeOwned;
use roaring::RoaringBitmap;
use crate::payload_store::PayloadCodec;
use crate::shard::Shard;
use crate::TagMode;
use super::bitmap_filter::ShardBitmapFilter;
use std::error::Error;

/// Configuration for tag filtering.
#[derive(Debug, Clone)]
pub struct TagFilterConfig {
    pub include_tags: Vec<String>,
    pub exclude_tags: Vec<String>,
    pub mode: TagMode,
}

/// Per-shard Roaring bitmap filter for FST output IDs.
#[derive(Clone)]
pub struct TagFilterBitmap {
    pub(crate) shard_bms: Vec<RoaringBitmap>,
}

impl TagFilterBitmap {
    /// Construct one roaring bitmap per shard, union/intersecting the include_tags
    /// and subtracting the exclude_tags per the TagMode (And/Or).
    pub fn new<V, C>(
        shards: &[Shard<V, C>],
        include_tags: &[String],
        exclude_tags: &[String],
        mode: TagMode,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        V: DeserializeOwned,
        C: PayloadCodec,
    {
        let mut shard_bms = Vec::with_capacity(shards.len());
        for shard in shards {
            // Build the include set, or the full range if empty
            let mut bm = if !include_tags.is_empty() {
                let mut acc: Option<RoaringBitmap> = None;
                if let Some(idx) = &shard.tags {
                    for tag in include_tags {
                        if let Some(sub) = idx.get(tag)? {
                            acc = Some(match acc {
                                Some(prev) if mode == TagMode::And => prev & &sub,
                                Some(prev) => prev | &sub,
                                None => sub,
                            });
                        } else if mode == TagMode::And {
                            acc = Some(RoaringBitmap::new());
                            break;
                        }
                    }
                }
                acc.unwrap_or_else(RoaringBitmap::new)
            } else {
                let mut full = RoaringBitmap::new();
                let max_id = shard.fst.len() as u32;
                if max_id > 0 {
                    full.insert_range(0..max_id);
                }
                full
            };

            // Subtract any exclude sets
            if let Some(idx) = &shard.tags {
                for tag in exclude_tags {
                    if let Some(sub) = idx.get(tag)? {
                        bm -= &sub;
                    }
                }
            }
            shard_bms.push(bm);
        }
        Ok(Self { shard_bms })
    }
}

// Allow TagFilterBitmap to drive shard pruning by producing optional bitmaps.
impl<V, C> ShardBitmapFilter<V, C> for TagFilterBitmap
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    fn build_shard_bitmaps(
        &self,
        _shards: &[Shard<V, C>],
    ) -> Result<Vec<Option<RoaringBitmap>>, Box<dyn Error>> {
        let mut out = Vec::with_capacity(self.shard_bms.len());
        for bm in &self.shard_bms {
            if bm.is_empty() {
                out.push(None);
            } else {
                out.push(Some(bm.clone()));
            }
        }
        Ok(out)
    }
}