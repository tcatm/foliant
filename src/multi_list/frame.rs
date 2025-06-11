use fst::raw::{Node, Output};
use smallvec::SmallVec;

/// Transition reference with precomputed bounds: (shard_idx, node_addr, output, upper_bound).
pub(super) type TransitionRef = (usize, usize, Output, u64);

/// Transition bucket: input byte and per-shard references with bounds.
pub(super) type Bucket = (u8, SmallVec<[TransitionRef; 4]>);

/// Fixed-capacity small buffer for transitions (inline up to 8 distinct bytes).
pub(super) type Transitions = SmallVec<[Bucket; 8]>;

/// Fast 256-slot transition table for O(1) bucket building.
pub(super) type TransitionTable = [Option<SmallVec<[TransitionRef; 4]>>; 256];

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
    /// Fast transition table for O(1) bucket building (reused across frames).
    pub(super) trans_table: Box<TransitionTable>,
}

impl<'a> FrameMulti<'a> {
    /// Construct a frame with pre-allocated capacity.
    pub(super) fn with_capacity(max_states: usize) -> Self {
        // Initialize the transition table with None values
        let trans_table = Box::new([const { None }; 256]);
        FrameMulti {
            states: Vec::with_capacity(max_states),
            trans: Transitions::new(),
            trans_idx: 0,
            yielded: false,
            prefix_len: 0,
            trans_table,
        }
    }

    /// Reset this frame for reuse with cleared per-shard states and prefix length.
    pub(super) fn reset(&mut self, prefix_len: usize) {
        self.states.clear();
        self.trans.clear();
        self.trans_idx = 0;
        self.yielded = false;
        self.prefix_len = prefix_len;
        // Clear the transition table
        for bucket in self.trans_table.iter_mut().flatten() {
            bucket.clear();
        }
    }

    /// Get or create a bucket for the given byte in the transition table.
    pub(super) fn trans_by_byte(&mut self, byte: u8) -> &mut SmallVec<[TransitionRef; 4]> {
        self.trans_table[byte as usize].get_or_insert_with(SmallVec::new)
    }

    /// Convert the transition table into the sorted transitions vector.
    pub(super) fn flush_trans_table(&mut self) {
        self.trans.clear();
        for (byte, bucket_opt) in self.trans_table.iter_mut().enumerate() {
            if let Some(bucket) = bucket_opt.take() {
                if !bucket.is_empty() {
                    self.trans.push((byte as u8, bucket));
                }
            }
        }
        // trans is already sorted by byte value since we iterate 0..256
    }
}