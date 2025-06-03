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
        // Filter shards by prefix and build per-shard state, pre-allocating for efficiency
        let mut fsts: Vec<&'a Fst<SharedMmap>> = Vec::with_capacity(shards.len());
        let mut shards_f: Vec<&'a Shard<V, C>> = Vec::with_capacity(shards.len());
        let mut states: Vec<FrameState<'a>> = Vec::with_capacity(shards.len());
        for shard in shards {
            let raw_fst = shard.fst.as_fst();
            let mut node = raw_fst.root();
            let mut out = node.final_output();
            let mut ok = true;

            for &b in prefix {
                if let Some(idx) = node.find_input(b) {
                    let tr = node.transition(idx);
                    out = out.cat(tr.out);
                    node = raw_fst.node(tr.addr);
                } else {
                    ok = false;
                    break;
                }
            }

            if ok {
                let idx = fsts.len();
                fsts.push(raw_fst);
                shards_f.push(shard);
                states.push(FrameState {
                    shard_idx: idx,
                    node,
                    output: out,
                });
            }
        }

        // Build initial frame with pre-reserved buffers and set its state
        let mut initial_frame = FrameMulti::with_capacity(shards.len());
        // Populate initial per-shard states and prefix length
        initial_frame.states = states;
        initial_frame.prefix_len = prefix.len();

        (fsts, shards_f, initial_frame)
    }

    /// Create a new multi-shard listing streamer for prefix bytes and optional delimiter
    pub(crate) fn new(shards: &'a [Shard<V, C>], prefix: Vec<u8>, delim: Option<u8>) -> Self {
        let (fsts, shards_f, initial_frame) = Self::walk_prefix(shards, &prefix);

        let mut streamer = Self {
            fsts,
            shards: shards_f,
            delim,
            // Build initial UTF-8 prefix string from bytes
            prefix_str: String::from_utf8(prefix.clone()).expect("invalid utf8 in prefix"),
            prefix,
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
                    let key = String::from_utf8(self.prefix.clone()).expect("invalid utf8 in key");
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
                    let mut s =
                        String::from_utf8(self.prefix.clone()).expect("invalid utf8 in prefix");
                    s.push(*b as char);
                    return Some(Entry::CommonPrefix(s));
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
