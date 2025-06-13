use crate::{Entry, Shard, Streamer};
/// Maximum expected key length for reserve hints
const MAX_KEY_LEN: usize = 256;

use super::frame::{FrameMulti, FrameState};
use super::frame_ops::{ShardInfo, build_frame_states, build_frame_transitions_and_bounds, 
                        create_frame_with_transitions, count_children_for_transition_refs};

use crate::payload_store::PayloadCodec;
use serde::de::DeserializeOwned;
use super::lazy_filter::LazyShardFilter;

impl<'a, V, C> MultiShardListStreamer<'a, V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// Common byte-by-byte traversal logic used by both walk_prefix and replay_seek_internal.
    /// Traverses the given bytes starting from the current frame stack, building frames with proper bounds.
    fn traverse_bytes_with_frames(
        &mut self, 
        bytes: &[u8],
        update_prefix: bool
    ) -> bool {
        for &byte in bytes {
            let frame = self.stack.last_mut().unwrap();
            if frame.trans_idx == 0 {
                build_frame_transitions_and_bounds(frame, &self.shard_infos);
            }
            
            if let Some((i, (_, refs))) = frame
                .trans
                .iter()
                .enumerate()
                .find(|(_, bucket)| bucket.0 == byte)
            {
                frame.trans_idx = i + 1;
                if update_prefix {
                    self.prefix.push(byte);
                }
                
                let new_frame = create_frame_with_transitions(
                    &self.shard_infos,
                    Some(&mut self.frame_pool),
                    self.shard_infos.len(),
                    self.prefix.len(),
                    frame,
                    &refs
                );
                self.stack.push(new_frame);
            } else {
                return false; // Byte not found, traversal failed
            }
        }
        true // Successfully traversed all bytes
    }


    /// Build a streamer and immediately fast‑forward so that the first `next()` yields
    /// the first entry > `cursor`.  Avoids a second prefix walk when resuming.
    pub fn resume(
        shards: &'a [Shard<V, C>],
        prefix: Vec<u8>,
        delim: Option<u8>,
        cursor: Vec<u8>,
    ) -> Self {
        Self::resume_with_lazy_filter(shards, prefix, delim, cursor, None).unwrap()
    }

    /// Internal helper: replay raw‐FST descent on each byte of `cursor`, advancing the DFS
    /// branch indices and marking yielded state so `next()` resumes after the cursor.
    fn replay_seek_internal(&mut self, cursor: &[u8]) {
        // Only replay the bytes after the original search prefix (start_prefix)
        let slice = if !self.start_prefix.is_empty() && cursor.starts_with(&self.start_prefix) {
            &cursor[self.start_prefix.len()..]
        } else {
            cursor
        };
        
        // Use common traversal logic
        self.traverse_bytes_with_frames(slice, true);
        
        if self.prefix == cursor {
            if let Some(frame) = self.stack.last_mut() {
                frame.yielded = true;
            }
        }
        if let Some(d) = self.delim {
            // if cursor ends on a grouping delimiter (not just the original prefix), backtrack one frame
            if cursor.last() == Some(&d) && cursor != self.start_prefix.as_slice() {
                let old = self.stack.pop().unwrap();
                self.frame_pool.push(old);
                let new_len = self.stack.last().map(|f| f.prefix_len).unwrap_or(0);
                self.prefix.truncate(new_len);
            }
        }
    }
}

/// Multi-shard DFS streamer that groups at a delimiter, yielding keys and common prefixes
pub struct MultiShardListStreamer<'a, V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// Per-shard info: raw FST, shard handle, and optional bitmap
    shard_infos: Vec<ShardInfo<'a, V, C>>,
    /// Optional delimiter byte for grouping
    delim: Option<u8>,
    /// Shared buffer of the current key prefix bytes
    prefix: Vec<u8>,
    /// Original search prefix bytes for resetting on seek
    start_prefix: Vec<u8>,
    /// Raw bytes of the last item returned (for cursor)
    last_bytes: Vec<u8>,
    /// DFS stack of active frames
    stack: Vec<FrameMulti<'a>>,
    /// Pool of spare frames for reuse to avoid allocations
    frame_pool: Vec<FrameMulti<'a>>,
}

/// Walks each shard’s FST by `prefix`, keeping only those that match.
impl<'a, V, C> MultiShardListStreamer<'a, V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// Create a new multi-shard listing streamer with lazy filtering.
    /// 
    /// This method performs the prefix walk first, then applies the lazy filter
    /// only to shards that match the prefix, significantly reducing bitmap computations.
    pub fn new_with_lazy_filter(
        shards: &'a [Shard<V, C>],
        prefix: Vec<u8>,
        delim: Option<u8>,
        filter: Option<Box<dyn LazyShardFilter<V, C>>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let start_prefix = prefix.clone();
        let mut initial_frame = FrameMulti::with_capacity(shards.len());
        initial_frame.prefix_len = start_prefix.len();
        
        // Build initial shard_infos without any filtering
        let mut shard_infos = Vec::with_capacity(shards.len());
        for shard in shards {
            let fst = shard.fst.as_fst();
            shard_infos.push(ShardInfo { fst, shard, bitmap: None });
        }
        
        // Build initial frame states by walking the prefix across all shards
        initial_frame.states = build_frame_states(&shard_infos, &start_prefix);

        // Handle case where literal prefix isn't found - return empty streamer
        if initial_frame.states.is_empty() && !start_prefix.is_empty() {
            // Return a streamer with empty shard_infos so next() will always return None
            return Ok(Self {
                shard_infos: Vec::new(),
                delim,
                prefix: start_prefix.clone(),
                start_prefix,
                last_bytes: Vec::new(),
                stack: Vec::new(),
                frame_pool: Vec::new(),
            });
        }
        
        // Apply lazy filtering if provided
        if let Some(flt) = filter {
            // Build a mapping from old shard index to new shard index (or None if filtered out)
            let mut shard_index_map: Vec<Option<usize>> = vec![None; shard_infos.len()];
            let mut filtered_shard_infos = Vec::new();
            let mut new_shard_idx = 0;
            
            for (old_idx, info) in shard_infos.iter().enumerate() {
                // Only compute bitmap for shards that had states after prefix walk
                let has_state = initial_frame.states.iter().any(|state| state.shard_idx == old_idx);
                
                if has_state {
                    match flt.compute_bitmap(info.shard)? {
                        Some(bm) if !bm.is_empty() => {
                            filtered_shard_infos.push(ShardInfo {
                                fst: info.fst,
                                shard: info.shard,
                                bitmap: Some(bm),
                            });
                            shard_index_map[old_idx] = Some(new_shard_idx);
                            new_shard_idx += 1;
                        }
                        _ => {
                            // Skip this shard (no matches or empty bitmap)
                        }
                    }
                }
            }
            
            // If no shards pass the filter, return empty streamer
            if filtered_shard_infos.is_empty() {
                return Ok(Self {
                    shard_infos: Vec::new(),
                    delim,
                    prefix: start_prefix.clone(),
                    start_prefix,
                    last_bytes: Vec::new(),
                    stack: Vec::new(),
                    frame_pool: Vec::new(),
                });
            }
            
            // Filter and remap frame states instead of rebuilding
            let mut filtered_states = Vec::new();
            for state in initial_frame.states {
                if let Some(new_idx) = shard_index_map[state.shard_idx] {
                    filtered_states.push(FrameState {
                        shard_idx: new_idx,
                        node: state.node,
                        output: state.output,
                        lb: state.lb,
                        ub: state.ub,
                    });
                }
            }
            
            // Update shard_infos and frame states
            shard_infos = filtered_shard_infos;
            initial_frame.states = filtered_states;
        }

        // Build streamer
        let mut streamer = Self {
            shard_infos,
            delim,
            prefix: start_prefix.clone(),
            start_prefix,
            last_bytes: Vec::new(),
            stack: vec![initial_frame],
            frame_pool: Vec::new(),
        };
        
        // Pre-reserve buffers
        streamer.prefix.reserve(MAX_KEY_LEN);
        streamer.stack.reserve(MAX_KEY_LEN);
        streamer.frame_pool.reserve(MAX_KEY_LEN);
        
        Ok(streamer)
    }
    
    /// Resume from cursor with lazy filtering.
    pub fn resume_with_lazy_filter(
        shards: &'a [Shard<V, C>],
        prefix: Vec<u8>,
        delim: Option<u8>,
        cursor: Vec<u8>,
        filter: Option<Box<dyn LazyShardFilter<V, C>>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut streamer = Self::new_with_lazy_filter(shards, prefix, delim, filter)?;
        streamer.replay_seek_internal(&cursor);
        streamer.last_bytes = cursor;
        Ok(streamer)
    }


    /// Create a new multi-shard listing streamer without filtering.
    pub fn new(
        shards: &'a [Shard<V, C>],
        prefix: Vec<u8>,
        delim: Option<u8>,
    ) -> Self {
        Self::new_with_lazy_filter(shards, prefix, delim, None).unwrap()
    }

    /// Create a new multi-shard listing streamer with an optional lazy filter.
    pub fn new_with_filter(
        shards: &'a [Shard<V, C>],
        prefix: Vec<u8>,
        delim: Option<u8>,
        filter: Option<Box<dyn LazyShardFilter<V, C>>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_lazy_filter(shards, prefix, delim, filter)
    }


    /// Resume from cursor with an optional lazy filter.
    pub fn resume_with_filter(
        shards: &'a [Shard<V, C>],
        prefix: Vec<u8>,
        delim: Option<u8>,
        cursor: Vec<u8>,
        filter: Option<Box<dyn LazyShardFilter<V, C>>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::resume_with_lazy_filter(shards, prefix, delim, cursor, filter)
    }

}

impl<'a, V, C> Streamer for MultiShardListStreamer<'a, V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    type Item = Entry<V>;
    type Cursor = Vec<u8>;

    /// Return the raw bytes of the last item returned by `next()`, or empty if none.
    fn cursor(&self) -> Self::Cursor {
        self.last_bytes.clone()
    }

    /// Position this streamer so that the next `next()` yields the first key > `cursor`.
    fn seek(&mut self, cursor: Self::Cursor) {
        // Reset to initial search prefix using current filtered shards rather than all_shards
        let start = self.start_prefix.clone();
        
        // Rebuild initial frame using current filtered shards and bitmaps
        let mut initial_frame = FrameMulti::with_capacity(self.shard_infos.len());
        initial_frame.prefix_len = start.len();
        initial_frame.states = build_frame_states(&self.shard_infos, &start);
        
        self.prefix = start.clone();
        self.stack.clear();
        self.stack.push(initial_frame);
        self.frame_pool.clear();
        self.replay_seek_internal(&cursor);
        // record last-emitted bytes for cursor()
        self.last_bytes = cursor;
    }

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(frame) = self.stack.last_mut() {
            // 1) Yield complete key if this node is final and not yet yielded
            if !frame.yielded {
                frame.yielded = true;
                if let Some(state) = frame.states.iter().find(|s| s.node.is_final()) {
                    let lut_id = state.lb;
                    let shard_i = state.shard_idx;
                    let payload_ptr = self.shard_infos[shard_i]
                        .shard
                        .lookup
                        .get(lut_id as u32)
                        .ok()
                        .map(|e| e.payload_ptr)
                        .unwrap_or(0);
                    let val = self.shard_infos[shard_i]
                        .shard
                        .payload
                        .get(payload_ptr)
                        .ok()
                        .flatten();
                    // record and emit full key
                    let bytes = self.prefix.clone();
                    self.last_bytes = bytes.clone();
                    let key = String::from_utf8(bytes).expect("invalid utf8 in key");
                    
                    // Skip keys whose LUT-ID is not in the shard's bitmap
                    if let Some(ref bm) = self.shard_infos[shard_i].bitmap {
                        if !bm.contains(lut_id as u32) {
                            continue;
                        }
                    }
                    
                    return Some(Entry::Key(key, lut_id, val));
                }
            }
            // 2) Build grouped transitions once, using optimized algorithm
                if frame.trans_idx == 0 {
                build_frame_transitions_and_bounds(frame, &self.shard_infos);
            }
            // 3) Process next transition if available
            if frame.trans_idx < frame.trans.len() {
                // extract input byte and associated refs, advancing the index
                let (inp_byte, refs) = {
                    let f = &mut *frame;
                    let (b, r) = f.trans[f.trans_idx].clone();
                    f.trans_idx += 1;
                    (b, r)
                };

                if self.shard_infos.iter().any(|info| info.bitmap.is_some()) {
                    // prune this bucket by checking each ref's precomputed [lb..=ub]
                    let mut keep = false;
                    for &(shard_i, _, output, ub) in &refs {
                        if let Some(ref bm) = self.shard_infos[shard_i].bitmap {
                            let lb = output.value();
                            if bm.range(lb as u32..=ub as u32).next().is_some() {
                                keep = true;
                                break;
                            }
                        } else {
                            keep = true;
                            break;
                        }
                    }
                    if !keep {
                        continue;
                    }
                }

                // a) Delimiter grouping: yield common prefix
                if Some(inp_byte) == self.delim {
                    let mut bytes = self.prefix.clone();
                    bytes.push(inp_byte);
                    self.last_bytes = bytes.clone();
                    let s = String::from_utf8(bytes).expect("invalid utf8 in prefix");

                    let child_count = count_children_for_transition_refs(&refs, &self.shard_infos);

                    // skip empty prefixes with no matching children
                    if child_count.unwrap_or(0) == 0 {
                        continue;
                    }

                    return Some(Entry::CommonPrefix(s, child_count));
                }

                // b) Descend into child nodes for byte inp_byte
                self.prefix.push(inp_byte);
                let new_frame = create_frame_with_transitions(
                    &self.shard_infos,
                    Some(&mut self.frame_pool),
                    self.shard_infos.len(),
                    self.prefix.len(),
                    frame,
                    &refs
                );
                self.stack.push(new_frame);
                continue;
            }
            // 4) Backtrack when no more transitions: recycle frame and revert prefix
            let old_frame = self.stack.pop().unwrap();
            self.frame_pool.push(old_frame);
            let new_len = self.stack.last().map(|f| f.prefix_len).unwrap_or(0);
            self.prefix.truncate(new_len);
        }
        None
    }
}

