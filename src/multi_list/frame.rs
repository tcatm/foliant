use fst::raw::{Node, Output};
use smallvec::SmallVec;

/// Transition reference with precomputed bounds: (shard_idx, node_addr, output, upper_bound).
pub(super) type TransitionRef = (usize, usize, Output, u64);

/// Transition bucket: input byte and per-shard references with bounds.
pub(super) type Bucket = (u8, SmallVec<[TransitionRef; 4]>);

/// Fixed-capacity small buffer for transitions (inline up to 8 distinct bytes).
pub(super) type Transitions = SmallVec<[Bucket; 8]>;

/// Per-frame per-shard traversal state: shard index, current node, and accumulated output.
pub(super) struct FrameState<'a> {
    pub(super) shard_idx: usize,
    pub(super) node: Node<'a>,
    pub(super) output: Output,
    pub(super) lb: u64,
    pub(super) ub: u64,
}

/// Frame for multi-shard DFS traversal with delimiter grouping.
pub(super) struct FrameMulti<'a> {
    /// Per-shard state at this depth.
    pub(super) states: Vec<FrameState<'a>>,
    /// Cached grouped transitions by input byte.
    pub(super) trans: Transitions,
    /// Next transition index in `trans` to process.
    pub(super) trans_idx: usize,
    /// Whether we've yielded the final key at this node.
    pub(super) yielded: bool,
    /// Prefix buffer length before descending into this frame.
    pub(super) prefix_len: usize,
}

impl<'a> FrameMulti<'a> {
    /// Construct a frame with pre-allocated capacity.
    pub(super) fn with_capacity(max_states: usize) -> Self {
        FrameMulti {
            states: Vec::with_capacity(max_states),
            trans: Transitions::new(),
            trans_idx: 0,
            yielded: false,
            prefix_len: 0,
        }
    }

    /// Reset this frame for reuse with cleared per-shard states and prefix length.
    pub(super) fn reset(&mut self, prefix_len: usize) {
        self.states.clear();
        self.trans.clear();
        self.trans_idx = 0;
        self.yielded = false;
        self.prefix_len = prefix_len;
    }

}