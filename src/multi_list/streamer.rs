use crate::shard::SharedMmap;
use crate::{Entry, Shard, Streamer};
use fst::raw::{Fst, Output};
/// Maximum expected key length for reserve hints
const MAX_KEY_LEN: usize = 256;

use super::frame::{FrameState, FrameMulti, TransitionRef};

use crate::payload_store::PayloadCodec;
use serde::de::DeserializeOwned;
use super::bitmap_filter::ShardBitmapFilter;
use roaring::RoaringBitmap;
/// Combined per-shard context for streamlined iteration
struct ShardInfo<'a, V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    fst: &'a Fst<SharedMmap>,
    shard: &'a Shard<V, C>,
    bitmap: Option<RoaringBitmap>,
}
impl<'a, V, C> MultiShardListStreamer<'a, V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// Build frame states for the current shard_infos and byte sequence.
    /// Uses the same vectorized traversal logic as `next()` to descend all shards in parallel.
    fn build_frame_states(
        infos: &[ShardInfo<'a, V, C>],
        bytes: &[u8],
    ) -> Vec<FrameState<'a>> {
        let shards = infos.len();
        let mut frame = FrameMulti::with_capacity(shards);
        frame.prefix_len = 0;
        frame.states = infos.iter().enumerate().map(|(i, info)| {
            let node = info.fst.root();
            let output = node.final_output();
            let lb = output.cat(node.final_output()).value();
            let ub = info.fst.len() as u64;
            FrameState { shard_idx: i, node, output, lb, ub }
        }).collect();
        for &b in bytes {
            frame.trans_idx = 0;
            Self::build_frame_transitions_fast(&mut frame, infos);
            if let Some((_, refs)) = frame.trans.iter().find(|bucket| bucket.0 == b) {
                frame = Self::create_frame_with_transitions(
                    infos,
                    None,
                    shards,
                    frame.prefix_len + 1,
                    &frame,
                    &refs,
                );
            } else {
                return Vec::new();
            }
        }
        frame.states
    }

    /// Build all grouped transitions and compute sibling bounds in O(T + T log T) time
    /// (where T is the total number of outgoing transitions for this frame).
    fn build_frame_transitions_fast(
        frame: &mut FrameMulti<'a>,
        infos: &[ShardInfo<'a, V, C>],
    ) {
        
        // 1. Clear any existing state
        for bucket in frame.trans_table.iter_mut().flatten() {
            bucket.clear();
        }
        
        // 2. Collect states first to avoid borrowing issues
        let state_info: Vec<(usize, Vec<(u8, usize, Output)>)> = frame.states.iter().map(|state| {
            let transitions: Vec<(u8, usize, Output)> = (0..state.node.len())
                .map(|ti| {
                    let tr = state.node.transition(ti);
                    let out_cat = state.output.cat(tr.out);
                    (tr.inp, tr.addr, out_cat)
                })
                .collect();
            (state.shard_idx, transitions)
        }).collect();
        
        // 3. Emit transitions into fixed 256-slot table (O(1) insertion)
        for (shard_idx, transitions) in state_info {
            for (inp, addr, out_cat) in transitions {
                let bucket = frame.trans_by_byte(inp);
                bucket.push((shard_idx, addr, out_cat, 0)); // ub computed later
            }
        }
        
        // 4. Convert the fixed table into compact sorted Vec
        frame.flush_trans_table();
        
        // 5. Compute bounds efficiently in O(T·log T) using sorted order
        Self::compute_transition_bounds_efficiently(frame, infos);
    }
    
    /// Efficiently compute upper bounds for all transitions in O(S·B log(S·B)) time.
    /// This processes transitions in sorted order to set sibling bounds correctly using a single pass.
    fn compute_transition_bounds_efficiently(
        frame: &mut FrameMulti<'a>,
        infos: &[ShardInfo<'a, V, C>],
    ) {
        // Helper to get shard's final upper bound
        let shard_end = |shard_idx: usize| -> u64 {
            let ub = frame.states
                .iter()
                .find(|s| s.shard_idx == shard_idx)
                .map(|s| s.ub)
                .unwrap_or(infos[shard_idx].fst.len() as u64);
            
            
            ub
        };
        
        // Collect ALL transitions across all buckets and sort them globally
        let mut all_transitions: Vec<(u8, usize, usize, usize, Output, u64)> = Vec::new(); // (byte, bucket_idx, trans_idx, shard_idx, output, full_lb)
        
        for (bucket_idx, (_byte, refs)) in frame.trans.iter_mut().enumerate() {
            for (trans_idx, (shard_idx, addr, output, _)) in refs.iter().enumerate() {
                let child_node = infos[*shard_idx].fst.node(*addr);
                let full_lb = output.cat(child_node.final_output()).value();
                all_transitions.push((*_byte, bucket_idx, trans_idx, *shard_idx, *output, full_lb));
            }
        }
        
        // Sort all transitions by (shard_idx, full_lb) to find true siblings
        all_transitions.sort_by_key(|(_, _, _, shard_idx, _, full_lb)| (*shard_idx, *full_lb));
        
        // Single forward pass to set upper bounds - O(T) where T = total transitions
        let mut prev_by_shard: Vec<Option<(usize, usize)>> = vec![None; infos.len()];
        
        
        for (_byte, bucket_idx, trans_idx, shard_idx, _output, full_lb) in all_transitions {
            // Close previous sibling of this shard, if any
            if let Some((prev_bucket, prev_trans)) = prev_by_shard[shard_idx].replace((bucket_idx, trans_idx)) {
                let new_ub = full_lb.saturating_sub(1);
                frame.trans[prev_bucket].1[prev_trans].3 = new_ub;
                
            }
        }
        
        // Finish shards whose last transition had no younger sibling
        for (shard_idx, opt) in prev_by_shard.into_iter().enumerate() {
            if let Some((bucket_idx, trans_idx)) = opt {
                let final_ub = shard_end(shard_idx);
                frame.trans[bucket_idx].1[trans_idx].3 = final_ub;
            }
        }
    }

    /// Create a new frame and populate it with states for the given transition references.
    /// Uses precomputed bounds from transition refs for O(1) operation.
    fn create_frame_with_transitions(
        infos: &[ShardInfo<'a, V, C>],
        frame_pool: Option<&mut Vec<FrameMulti<'a>>>,
        shards_len: usize,
        prefix_len: usize,
        _frame: &FrameMulti<'a>,
        refs: &[TransitionRef]
    ) -> FrameMulti<'a> {
        let mut new_frame = if let Some(pool) = frame_pool {
            pool.pop().unwrap_or_else(|| FrameMulti::with_capacity(shards_len))
        } else {
            FrameMulti::with_capacity(shards_len)
        };
        new_frame.reset(prefix_len);
        
        for &(shard_idx, addr, out, ub) in refs {
            let node = infos[shard_idx].fst.node(addr);
            let new_out = out.cat(node.final_output());
            let lb = new_out.value();
            
            new_frame.states.push(FrameState {
                shard_idx,
                node,
                output: new_out,
                lb,
                ub, // Use precomputed upper bound
            });
        }
        
        new_frame
    }

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
                Self::build_frame_transitions_fast(frame, &self.shard_infos);
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
                
                let new_frame = Self::create_frame_with_transitions(
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

    /// Count descendant keys under transition references with precomputed bounds (O(1) per call).
    fn count_children_for_transition_refs(&self, refs: &[TransitionRef]) -> Option<usize> {
        let mut total = 0usize;
        let use_bitmaps = self.shard_infos.iter().any(|info| info.bitmap.is_some());
        
        
        for &(shard_idx, _addr, output, ub) in refs {
            let should_count = if use_bitmaps {
                self.shard_infos[shard_idx].bitmap.is_some()
            } else {
                true
            };
            
            if should_count {
                let lb = output.value();
                
                if ub >= lb {
                    let count = if use_bitmaps {
                        if let Some(ref bm) = self.shard_infos[shard_idx].bitmap {
                            bm.range(lb as u32..=ub as u32).count() as usize
                        } else {
                            0
                        }
                    } else {
                        (ub - lb + 1) as usize
                    };
                    total = total.saturating_add(count);
                }
            }
        }
        
        if total > 0 { Some(total) } else { None }
    }

    /// Build a streamer and immediately fast‑forward so that the first `next()` yields
    /// the first entry > `cursor`.  Avoids a second prefix walk when resuming.
    pub fn resume(
        shards: &'a [Shard<V, C>],
        prefix: Vec<u8>,
        delim: Option<u8>,
        cursor: Vec<u8>,
    ) -> Self {
        Self::resume_with_bitmaps(shards, prefix, delim, cursor, None).unwrap()
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
    /// Replace shards and FSTs by only those with a matching bitmap, storing the bitmaps.
    /// Also remaps any existing frame shard indices to the new indices.
    pub fn apply_shard_bitmaps(
        &mut self,
        shards: &'a [Shard<V, C>],
        bitmaps: Vec<Option<RoaringBitmap>>,
    ) {
        // Build new shard_infos from provided bitmaps filter
        let mut new_infos = Vec::new();
        for (i, bm_opt) in bitmaps.into_iter().enumerate() {
            if let Some(bm) = bm_opt {
                let shard_ref = &shards[i];
                new_infos.push(ShardInfo { fst: shard_ref.fst.as_fst(), shard: shard_ref, bitmap: Some(bm) });
            }
        }
        self.shard_infos = new_infos;
        
        
        // Rebuild frame states from scratch with the new FST array
        // This ensures all Node references point to the correct FSTs
        let current_prefix = self.prefix.clone();
        
        let mut initial_frame = FrameMulti::with_capacity(self.shard_infos.len());
        initial_frame.prefix_len = current_prefix.len();
        initial_frame.states = Self::build_frame_states(&self.shard_infos, &current_prefix);
        
        // Replace the stack with the rebuilt initial frame
        self.stack.clear();
        self.stack.push(initial_frame);
        self.frame_pool.clear();
    }

    /// Create a new multi-shard listing streamer without filtering.
    pub fn new(
        shards: &'a [Shard<V, C>],
        prefix: Vec<u8>,
        delim: Option<u8>,
    ) -> Self {
        Self::new_with_bitmaps(shards, prefix, delim, None).unwrap()
    }

    /// Create a new multi-shard listing streamer with an optional generic bitmap filter.
    pub fn new_with_filter<BF>(
        shards: &'a [Shard<V, C>],
        prefix: Vec<u8>,
        delim: Option<u8>,
        filter: Option<BF>,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        BF: ShardBitmapFilter<V, C> + 'a,
    {
        if let Some(flt) = filter {
            // Apply filtering before the prefix walk to avoid double walking
            let bitmaps = flt.build_shard_bitmaps(shards)?;
            Self::new_with_bitmaps(shards, prefix, delim, Some(bitmaps))
        } else {
            Self::new_with_bitmaps(shards, prefix, delim, None)
        }
    }

    /// Create a new multi-shard listing streamer with optional pre-computed shard bitmaps.
    /// This is the unified implementation that handles both filtered and unfiltered cases.
    fn new_with_bitmaps(
        shards: &'a [Shard<V, C>],
        prefix: Vec<u8>,
        delim: Option<u8>,
        bitmaps: Option<Vec<Option<RoaringBitmap>>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (selected_shards, selected_bitmaps): (Vec<&Shard<V, C>>, Vec<Option<RoaringBitmap>>) = 
            if let Some(ref bm_vec) = bitmaps {
                // Filter shards based on bitmaps
                let mut filtered_shards = Vec::new();
                let mut filtered_bitmaps = Vec::new();
                for (i, bm_opt) in bm_vec.iter().enumerate() {
                    if let Some(bm) = bm_opt {
                        filtered_shards.push(&shards[i]);
                        filtered_bitmaps.push(Some(bm.clone()));
                    }
                }
                (filtered_shards, filtered_bitmaps)
            } else {
                // Use all shards with no filtering
                let all_shards: Vec<&Shard<V, C>> = shards.iter().collect();
                let no_bitmaps = vec![None; shards.len()];
                (all_shards, no_bitmaps)
            };

        // Single prefix walk on selected shards and collect per-shard info
        let start_prefix = prefix.clone();
        let mut initial_frame = FrameMulti::with_capacity(selected_shards.len());
        initial_frame.prefix_len = start_prefix.len();
        let mut shard_infos = Vec::with_capacity(selected_shards.len());
        for (shard, bitmap) in selected_shards.into_iter().zip(selected_bitmaps.into_iter()) {
            let fst = shard.fst.as_fst();
            shard_infos.push(ShardInfo { fst, shard, bitmap });
        }
        // Build initial frame states by walking the prefix across all shards
        initial_frame.states = Self::build_frame_states(&shard_infos, &start_prefix);

        // Handle case where literal prefix isn't found
        if initial_frame.states.is_empty() && !start_prefix.is_empty() {
            let mut fallback = Self::new_with_bitmaps(shards, Vec::new(), delim, bitmaps.clone())?;
            fallback.seek(start_prefix.clone());
            let _ = fallback.next();
            return Ok(fallback);
        }

        // Build streamer with unified shard_infos
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

    /// Resume from cursor with optional pre-computed shard bitmaps.
    /// This is the unified implementation that handles both filtered and unfiltered resume cases.
    fn resume_with_bitmaps(
        shards: &'a [Shard<V, C>],
        prefix: Vec<u8>,
        delim: Option<u8>,
        cursor: Vec<u8>,
        bitmaps: Option<Vec<Option<RoaringBitmap>>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut streamer = Self::new_with_bitmaps(shards, prefix, delim, bitmaps)?;
        streamer.replay_seek_internal(&cursor);
        streamer.last_bytes = cursor;
        Ok(streamer)
    }

    /// Resume from cursor with an optional generic bitmap filter.
    pub fn resume_with_filter<BF>(
        shards: &'a [Shard<V, C>],
        prefix: Vec<u8>,
        delim: Option<u8>,
        cursor: Vec<u8>,
        filter: Option<BF>,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        BF: ShardBitmapFilter<V, C> + 'a,
    {
        if let Some(flt) = filter {
            let bitmaps = flt.build_shard_bitmaps(shards)?;
            Self::resume_with_bitmaps(shards, prefix, delim, cursor, Some(bitmaps))
        } else {
            Self::resume_with_bitmaps(shards, prefix, delim, cursor, None)
        }
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
        initial_frame.states = Self::build_frame_states(&self.shard_infos, &start);
        
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
                Self::build_frame_transitions_fast(frame, &self.shard_infos);
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

                    let child_count = self.count_children_for_transition_refs(&refs);

                    // skip empty prefixes with no matching children
                    if child_count.unwrap_or(0) == 0 {
                        continue;
                    }

                    return Some(Entry::CommonPrefix(s, child_count));
                }

                // b) Descend into child nodes for byte inp_byte
                self.prefix.push(inp_byte);
                let new_frame = Self::create_frame_with_transitions(
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

