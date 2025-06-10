use crate::prefix::walk_prefix_shards;
use crate::shard::SharedMmap;
use crate::{Entry, Shard, Streamer};
use fst::raw::{Fst, Node, Output};
use smallvec::SmallVec;
/// Maximum expected key length for reserve hints
const MAX_KEY_LEN: usize = 256;
/// Transition bucket: input byte and per-shard (shard_idx, node_addr, output)
type Bucket = (u8, Vec<(usize, usize, Output)>);
/// Fixed-capacity small buffer for transitions (inline up to 8 distinct bytes)
type Transitions = SmallVec<[Bucket; 8]>;
use crate::payload_store::{CborPayloadCodec, PayloadCodec};
use serde::de::DeserializeOwned;
/// Per-frame per-shard traversal state: shard index, current node, and accumulated output
struct FrameState<'a> {
    shard_idx: usize,
    node: Node<'a>,
    output: Output,
}

impl<'a, V, C> MultiShardListStreamer<'a, V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// Find the minimum output value in a subtree by following the leftmost path.
    fn find_min_output_for_node(&self, fst: &Fst<SharedMmap>, node: Node, output: Output) -> Option<u64> {
        // If this node is final, it could be the minimum
        if node.is_final() {
            return Some(output.cat(node.final_output()).value());
        }
        
        // Otherwise, follow the first (leftmost) transition
        if node.len() > 0 {
            let first_transition = node.transition(0);
            let child_node = fst.node(first_transition.addr);
            let child_output = output.cat(first_transition.out);
            return self.find_min_output_for_node(fst, child_node, child_output);
        }
        
        None
    }
    
    /// Find the maximum output value in a subtree by following the rightmost path.
    fn find_max_output_for_node(&self, fst: &Fst<SharedMmap>, node: Node, output: Output) -> Option<u64> {
        // If this node is final, check if it's better than any child
        let mut max_output = if node.is_final() {
            Some(output.cat(node.final_output()).value())
        } else {
            None
        };
        
        // Follow the last (rightmost) transition to find the maximum in children
        if node.len() > 0 {
            let last_transition = node.transition(node.len() - 1);
            let child_node = fst.node(last_transition.addr);
            let child_output = output.cat(last_transition.out);
            if let Some(child_max) = self.find_max_output_for_node(fst, child_node, child_output) {
                max_output = Some(max_output.map_or(child_max, |current| current.max(child_max)));
            }
        }
        
        max_output
    }

    /// Count children under a specific transition by finding output value bounds.
    fn count_children_for_transition_refs(&self, refs: &[(usize, usize, Output)]) -> Option<usize> {
        let mut global_min = u64::MAX;
        let mut global_max = 0u64;
        let mut found_any = false;

        // Find output bounds across all shards for the given transition references
        for (shard_idx, addr, output) in refs {
            let fst = self.fsts[*shard_idx];
            let node = fst.node(*addr);
            
            if let Some(min) = self.find_min_output_for_node(fst, node, *output) {
                global_min = global_min.min(min);
                found_any = true;
            }
            
            if let Some(max) = self.find_max_output_for_node(fst, node, *output) {
                global_max = global_max.max(max);
                found_any = true;
            }
        }

        if found_any && global_max >= global_min {
            Some((global_max - global_min + 1) as usize)
        } else {
            None
        }
    }

    /// Build a streamer and immediately fast‑forward so that the first `next()` yields
    /// the first entry > `cursor`.  Avoids a second prefix walk when resuming.
    pub fn resume(
        shards: &'a [Shard<V, C>],
        prefix: Vec<u8>,
        delim: Option<u8>,
        cursor: Vec<u8>,
    ) -> Self {
        let start_prefix = prefix.clone();
        let (fsts, shards_f, initial_frame) = Self::walk_prefix(shards, &start_prefix);
        // If literal prefix not found, fall back to scanning empty prefix then resume
        if initial_frame.states.is_empty() && !start_prefix.is_empty() {
            return Self::resume(shards, Vec::new(), delim, cursor);
        }
        let mut streamer = Self {
            all_shards: shards,
            fsts,
            shards: shards_f,
            delim,
            prefix_str: String::from_utf8(start_prefix.clone()).expect("invalid utf8 in prefix"),
            prefix: start_prefix.clone(),
            start_prefix,
            last_bytes: Vec::new(),
            stack: vec![initial_frame],
            frame_pool: Vec::new(),
        };
        streamer.prefix.reserve(MAX_KEY_LEN);
        streamer.prefix_str.reserve(MAX_KEY_LEN);
        streamer.stack.reserve(MAX_KEY_LEN);
        streamer.frame_pool.reserve(MAX_KEY_LEN);
        streamer.replay_seek_internal(&cursor);
        streamer.last_bytes = cursor;
        streamer
    }

    /// Internal helper: replay raw‐FST descent on each byte of `cursor`, advancing the DFS
    /// branch indices and marking yielded state so `next()` resumes after the cursor.
    fn replay_seek_internal(&mut self, cursor: &[u8]) {
        // Only replay the bytes after the original search prefix (start_prefix)
        let mut slice = cursor;
        if !self.start_prefix.is_empty() && cursor.starts_with(&self.start_prefix) {
            slice = &cursor[self.start_prefix.len()..];
        }
        for &b in slice {
            let frame = self.stack.last_mut().unwrap();
            if frame.trans_idx == 0 {
                frame.trans.clear();
                for state in &frame.states {
                    for ti in 0..state.node.len() {
                        let tr = state.node.transition(ti);
                        let out = state.output.cat(tr.out);
                        if let Some(bucket) =
                            frame.trans.iter_mut().find(|bucket| bucket.0 == tr.inp)
                        {
                            bucket.1.push((state.shard_idx, tr.addr, out));
                        } else {
                            let mut v = Vec::with_capacity(self.shards.len());
                            v.push((state.shard_idx, tr.addr, out));
                            frame.trans.push((tr.inp, v));
                        }
                    }
                }
                frame.trans.sort_unstable_by_key(|b| b.0);
            }
            if let Some((i, (_, refs))) = frame
                .trans
                .iter()
                .enumerate()
                .find(|(_, bucket)| bucket.0 == b)
            {
                frame.trans_idx = i + 1;
                self.prefix.push(b);
                let mut new_frame = self
                    .frame_pool
                    .pop()
                    .unwrap_or_else(|| FrameMulti::with_capacity(self.shards.len()));
                new_frame.reset(self.prefix.len());
                for (shard_i, addr, out) in refs.iter() {
                    let node = self.fsts[*shard_i].node(*addr);
                    new_frame.states.push(FrameState {
                        shard_idx: *shard_i,
                        node,
                        output: *out,
                    });
                }
                self.stack.push(new_frame);
            } else {
                break;
            }
        }
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

/// Frame for multi-shard DFS traversal with delimiter grouping
pub(crate) struct FrameMulti<'a> {
    /// Per-shard state at this depth (shard index, node, and accumulated output)
    states: Vec<FrameState<'a>>,
    /// Cached grouped transitions by input byte (inline small vector)
    trans: Transitions,
    /// Next transition index in `trans` to process
    trans_idx: usize,
    /// Whether we've yielded the final key at this node
    yielded: bool,
    /// Prefix buffer length before descending into this frame
    prefix_len: usize,
}
// Poolable frame with reusable buffers
impl<'a> FrameMulti<'a> {
    /// Construct a frame with pre-allocated capacity
    fn with_capacity(max_states: usize) -> Self {
        FrameMulti {
            states: Vec::with_capacity(max_states),
            trans: Transitions::new(),
            trans_idx: 0,
            yielded: false,
            prefix_len: 0,
        }
    }

    /// Reset this frame for reuse with cleared per-shard states and prefix length
    fn reset(&mut self, prefix_len: usize) {
        self.states.clear();
        self.trans.clear();
        self.trans_idx = 0;
        self.yielded = false;
        self.prefix_len = prefix_len;
    }
}

/// Multi-shard DFS streamer that groups at a delimiter, yielding keys and common prefixes
pub struct MultiShardListStreamer<'a, V, C: PayloadCodec = CborPayloadCodec>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// All shards (for resetting on seek)
    all_shards: &'a [Shard<V, C>],
    /// Raw FSTs for each matching shard
    fsts: Vec<&'a Fst<SharedMmap>>,
    /// References to shards corresponding to each FST
    shards: Vec<&'a Shard<V, C>>,
    /// Optional delimiter byte for grouping
    delim: Option<u8>,
    /// Shared buffer of the current key prefix bytes
    prefix: Vec<u8>,
    /// Shared UTF-8 prefix string (aligned with `prefix` bytes)
    prefix_str: String,
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
    /// Walks each shard’s FST by `prefix`, returning the FSTs, shards, and the initial frame.
    fn walk_prefix(
        shards: &'a [Shard<V, C>],
        prefix: &[u8],
    ) -> (
        Vec<&'a Fst<SharedMmap>>,
        Vec<&'a Shard<V, C>>,
        FrameMulti<'a>,
    ) {
        let (fsts, shards_f, prefix_states) = walk_prefix_shards(shards, prefix);
        let mut initial_frame = FrameMulti::with_capacity(shards.len());
        initial_frame.states = prefix_states
            .into_iter()
            .map(|ps| FrameState {
                shard_idx: ps.shard_idx,
                node: ps.node,
                output: ps.output,
            })
            .collect();
        initial_frame.prefix_len = prefix.len();
        (fsts, shards_f, initial_frame)
    }

    /// Create a new multi-shard listing streamer for prefix bytes and optional delimiter
    pub fn new(shards: &'a [Shard<V, C>], prefix: Vec<u8>, delim: Option<u8>) -> Self {
        let start_prefix = prefix.clone();
        let (fsts, shards_f, initial_frame) = Self::walk_prefix(shards, &start_prefix);
        // if the literal prefix isn't found, fall back to a full scan then seek
        if initial_frame.states.is_empty() && !start_prefix.is_empty() {
            let mut fallback = Self::new(shards, Vec::new(), delim);
            fallback.seek(start_prefix.clone());
            // drop any key ≤ start_prefix (including shorter prefix matches)
            let _ = fallback.next();
            return fallback;
        }

        let mut streamer = Self {
            all_shards: shards,
            fsts,
            shards: shards_f,
            delim,
            // Build initial UTF-8 prefix string from bytes
            prefix_str: String::from_utf8(start_prefix.clone()).expect("invalid utf8 in prefix"),
            // DFS cursor: starts at the search prefix
            prefix: start_prefix.clone(),
            // Original search prefix for resetting
            start_prefix,
            // Last-emitted item bytes (empty until first next())
            last_bytes: Vec::new(),
            // DFS stack and frame pool
            stack: vec![initial_frame],
            frame_pool: Vec::new(),
        };
        // Pre-reserve buffers to avoid mid-stream reallocations
        streamer.prefix.reserve(MAX_KEY_LEN);
        streamer.prefix_str.reserve(MAX_KEY_LEN);
        streamer.stack.reserve(MAX_KEY_LEN);
        streamer.frame_pool.reserve(MAX_KEY_LEN);
        streamer
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
        // reset to initial search prefix, then resume efficiently via DFS replay
        let start = self.start_prefix.clone();
        let (fsts, shards_f, initial_frame) = Self::walk_prefix(self.all_shards, &start);
        self.fsts = fsts;
        self.shards = shards_f;
        self.prefix = start.clone();
        self.prefix_str = String::from_utf8(start.clone()).expect("invalid utf8 in prefix");
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
                    let lut_id = state.output.value();
                    let shard_i = state.shard_idx;
                    let payload_ptr = self.shards[shard_i]
                        .lookup
                        .get(lut_id as u32)
                        .ok()
                        .map(|e| e.payload_ptr)
                        .unwrap_or(0);
                    let val = self.shards[shard_i].payload.get(payload_ptr).ok().flatten();
                    // record and emit full key
                    let bytes = self.prefix.clone();
                    self.last_bytes = bytes.clone();
                    let key = String::from_utf8(bytes).expect("invalid utf8 in key");
                    return Some(Entry::Key(key, lut_id, val));
                }
            }
            // 2) Build grouped transitions once, reusing inline buckets
            if frame.trans_idx == 0 {
                frame.trans.clear();
                for state in &frame.states {
                    for ti in 0..state.node.len() {
                        let tr = state.node.transition(ti);
                        let out = state.output.cat(tr.out);
                        if let Some(bucket) = frame.trans.iter_mut().find(|b| b.0 == tr.inp) {
                            bucket.1.push((state.shard_idx, tr.addr, out));
                        } else {
                            let mut v = Vec::with_capacity(self.shards.len());
                            v.push((state.shard_idx, tr.addr, out));
                            frame.trans.push((tr.inp, v));
                        }
                    }
                }
                frame.trans.sort_unstable_by_key(|b| b.0);
            }
            // 3) Process next transition if available
            if frame.trans_idx < frame.trans.len() {
                let (b, refs) = &frame.trans[frame.trans_idx];
                frame.trans_idx += 1;
                // a) Delimiter grouping: yield common prefix
                if Some(*b) == self.delim {
                    // record and emit common prefix
                    let mut bytes = self.prefix.clone();
                    bytes.push(*b);
                    self.last_bytes = bytes.clone();
                    let s = String::from_utf8(bytes).expect("invalid utf8 in prefix");
                    
                    // Count children under this prefix by looking at the nodes we would descend into
                    let refs_clone = refs.clone();
                    let child_count = self.count_children_for_transition_refs(&refs_clone);
                    
                    return Some(Entry::CommonPrefix(s, child_count));
                }
                // b) Descend into child nodes across all shards for byte b
                self.prefix.push(*b);
                // Recycle or allocate a new frame, then repopulate its states in-place
                let mut new_frame = self
                    .frame_pool
                    .pop()
                    .unwrap_or_else(|| FrameMulti::with_capacity(self.shards.len()));
                new_frame.reset(self.prefix.len());
                for (shard_i, addr, out) in refs.iter() {
                    let node = self.fsts[*shard_i].node(*addr);
                    new_frame.states.push(FrameState {
                        shard_idx: *shard_i,
                        node,
                        output: *out,
                    });
                }
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
