use fst::{IntoStreamer, Streamer};
use fst::map::{OpBuilder, Union};

use crate::error::Result;
use crate::streamer::Streamer as DbStreamer;
use crate::tag_index::TagIndex;
use crate::payload_store::PayloadCodec;
use crate::shard_provider::ShardProvider;
use crate::frame::{ShardInfo, build_frame_states, FrameState};

use serde::de::DeserializeOwned;

/// Streamer that lists tags from shards using a ShardProvider, similar to MultiShardListStreamer
pub struct TagStreamer<'a, V, C> 
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    union_stream: Union<'a>,
    tag_indices: Vec<&'a TagIndex>,
    frame_bounds: Vec<(u32, u32)>,
    last_tag: Vec<u8>,
    _phantom: std::marker::PhantomData<(V, C)>,
}

impl<'a, V, C> TagStreamer<'a, V, C>
where
    V: DeserializeOwned + 'a,
    C: PayloadCodec + 'a,
{
    /// Create a new TagStreamer from a ShardProvider and optional prefix
    pub fn new<P: ShardProvider<V, C>>(
        provider: &'a P,
        prefix: Option<&str>,
    ) -> Result<Self> {
        let prefix_str = prefix.unwrap_or("");
        let bounds = Self::collect_shard_bounds(provider, prefix_str);
        
        if bounds.is_empty() {
            // Return an empty streamer
            return Ok(TagStreamer {
                union_stream: OpBuilder::new().union().into_stream(),
                tag_indices: Vec::new(),
                frame_bounds: Vec::new(),
                last_tag: Vec::with_capacity(64),
                _phantom: std::marker::PhantomData,
            });
        }
        
        let mut op = OpBuilder::new();
        let mut tag_indices = Vec::with_capacity(bounds.len());
        let mut frame_bounds = Vec::with_capacity(bounds.len());
        
        for (frame_state, tag_index) in bounds {
            op = op.add(tag_index.list_tags());
            tag_indices.push(tag_index);
            frame_bounds.push((frame_state.lb as u32, frame_state.ub as u32));
        }
        
        Ok(TagStreamer {
            union_stream: op.union().into_stream(),
            tag_indices,
            frame_bounds,
            last_tag: Vec::with_capacity(64),
            _phantom: std::marker::PhantomData,
        })
    }
    
    /// Walk prefix across shards and collect bounds for matching entries
    /// Uses lockstep traversal for efficiency - all FSTs are walked together
    fn collect_shard_bounds<P: ShardProvider<V, C>>(
        provider: &'a P, 
        prefix: &str
    ) -> Vec<(FrameState<'a>, &'a TagIndex)> {
        // Use provider's prefix index to get candidate shards
        let candidate_bitmap = provider.get_shards_for_prefix(prefix.as_bytes());
        let shards = provider.get_all_shards();
        
        // Build ShardInfo only for candidate shards that have a tag index
        let mut shard_infos = Vec::new();
        let mut idx_mapping = Vec::new(); // Maps shard_infos index to actual shard index
        
        for idx in candidate_bitmap.iter() {
            let shard = &shards[idx as usize];
            if shard.tags.is_some() {
                shard_infos.push(ShardInfo {
                    fst: shard.fst.as_fst(),
                    shard,
                    bitmap: None,
                });
                idx_mapping.push(idx as usize);
            }
        }
        
        if shard_infos.is_empty() {
            return Vec::new();
        }
        
        // Use the frame infrastructure to walk the prefix and get proper bounds
        let frame_states = build_frame_states(&shard_infos, prefix.as_bytes());
        
        // Convert FrameState results to tuples with tag indices
        // Need to map back to original shard indices
        frame_states.into_iter()
            .filter_map(|state| {
                // The state.shard_idx is relative to shard_infos, need to get actual shard index
                let actual_shard_idx = idx_mapping[state.shard_idx];
                shards[actual_shard_idx].tags.as_ref().map(|tag_index| {
                    (state, tag_index)
                })
            })
            .collect()
    }
}

impl<'a, V, C> DbStreamer for TagStreamer<'a, V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    type Item = (String, usize);
    type Cursor = Vec<u8>;
    
    fn cursor(&self) -> Self::Cursor {
        self.last_tag.clone()
    }
    
    fn seek(&mut self, cursor: Self::Cursor) {
        // Seek is not implemented for tag streaming
        // This would require recreating the union with seek
        let _ = cursor;
    }
    
    fn next(&mut self) -> Option<Self::Item> {
        while let Some((tag_bytes, indexed_values)) = self.union_stream.next() {
            let mut total_count = 0u64;
            let mut display_tag_opt = None;
            
            // For each shard that contains this tag
            for iv in indexed_values {
                let tag_index = self.tag_indices[iv.index];
                let (lb, ub) = self.frame_bounds[iv.index];
                
                // Get bitmap and count in one operation
                let bitmap = if display_tag_opt.is_none() {
                    // Need both bitmap and original case
                    if let Ok((bitmap, original_case)) = tag_index.get_bitmap_and_original_case(iv.value) {
                        if let Some(original) = original_case {
                            display_tag_opt = Some(original);
                        }
                        Some(bitmap)
                    } else {
                        None
                    }
                } else {
                    // Just need the bitmap
                    tag_index.get_bitmap(iv.value).ok()
                };
                
                if let Some(bitmap) = bitmap {
                    let count = bitmap.range_cardinality(lb..=ub);
                    total_count += count;
                }
            }
            
            if total_count > 0 {
                // Reuse buffer instead of allocating new Vec
                self.last_tag.clear();
                self.last_tag.extend_from_slice(tag_bytes);
                
                // Only convert to String once we know we'll return
                let display_tag = display_tag_opt.unwrap_or_else(|| {
                    // Fallback: convert bytes to string only if needed
                    String::from_utf8_lossy(tag_bytes).into_owned()
                });
                
                return Some((display_tag, total_count as usize));
            }
        }
        None
    }
}