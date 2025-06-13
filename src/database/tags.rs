use fst::{IntoStreamer, Streamer};
use fst::map::{OpBuilder, Union};

use crate::error::Result;
use crate::streamer::Streamer as DbStreamer;
use crate::tag_index::TagIndex;
use crate::Database;
use crate::payload_store::PayloadCodec;

use super::IterStreamer;
use serde::de::DeserializeOwned;
use crate::frame::{ShardInfo, build_frame_states, FrameState};

/// Streamer that processes FST union output on-the-fly and yields tags with counts
struct TagStreamer<'a> {
    union_stream: Union<'a>,
    tag_indices: Vec<&'a TagIndex>,
    frame_bounds: Vec<(u32, u32)>,
    last_tag: Vec<u8>,
}

impl<'a> TagStreamer<'a> {
    fn new(bounds: Vec<(FrameState<'a>, &'a TagIndex)>) -> Result<Self> {
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
        })
    }
}

impl<'a> DbStreamer for TagStreamer<'a> {
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

impl<V, C> Database<V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// Walk prefix across all shards and collect bounds for matching entries
    /// Uses lockstep traversal for efficiency - all FSTs are walked together
    fn collect_shard_bounds(&self, prefix: &str) -> Vec<(FrameState, &TagIndex)> {
        // Build ShardInfo for each shard that has a tag index
        let shard_infos: Vec<ShardInfo<V, C>> = self.shards
            .iter()
            .filter(|shard| shard.tags.is_some())
            .map(|shard| ShardInfo {
                fst: shard.fst.as_fst(),
                shard,
                bitmap: None,
            })
            .collect();
        
        if shard_infos.is_empty() {
            return Vec::new();
        }
        
        // Use the frame infrastructure to walk the prefix and get proper bounds
        let frame_states = build_frame_states(&shard_infos, prefix.as_bytes());
        
        // Convert FrameState results to tuples with tag indices
        frame_states.into_iter()
            .filter_map(|state| {
                // Get the tag index for this shard
                self.shards[state.shard_idx].tags.as_ref().map(|tag_index| {
                    (state, tag_index)
                })
            })
            .collect()
    }

    /// Stream all tags present in the database, optionally restricted to keys
    /// under the given prefix, yielding each tag and its total count
    /// (unioned across all shards' tag indexes).
    pub fn list_tags<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> Result<Box<dyn DbStreamer<Item = (String, usize), Cursor = Vec<u8>> + 'a>> {
        // Always use frame-based approach for both prefix and non-prefix cases
        let prefix_str = prefix.unwrap_or("");
        let bounds = self.collect_shard_bounds(prefix_str);
        
        if bounds.is_empty() {
            return Ok(Box::new(IterStreamer(std::iter::empty())));
        }
        
        Ok(Box::new(TagStreamer::new(bounds)?))
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