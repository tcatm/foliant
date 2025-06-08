//! Common prefix-walk utilities for multi-shard operations.

use crate::shard::SharedMmap;
use crate::shard::Shard;
use fst::raw::{Fst, Node, Output};

/// Per-shard state after walking an FST by a prefix.
pub(crate) struct PrefixShardState<'a> {
    /// Index of the shard in the input slice.
    pub shard_idx: usize,
    /// FST node at the end of the prefix.
    pub node: Node<'a>,
    /// Accumulated output at this node.
    pub output: Output,
}

/// Walk each shard's FST in parallel by the given prefix bytes, returning
/// the per-shard states for those shards where the prefix is present.
pub(crate) fn walk_prefix_shards<'a, V, C>(
    shards: &'a [Shard<V, C>],
    prefix: &[u8],
) -> (Vec<&'a Fst<SharedMmap>>, Vec<&'a Shard<V, C>>, Vec<PrefixShardState<'a>>)
where
    V: serde::de::DeserializeOwned,
    C: crate::payload_store::PayloadCodec,
{
    let mut fsts = Vec::with_capacity(shards.len());
    let mut shards_f = Vec::with_capacity(shards.len());
    let mut states = Vec::with_capacity(shards.len());
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
            let idx_shard = fsts.len();
            fsts.push(raw_fst);
            shards_f.push(shard);
            states.push(PrefixShardState {
                shard_idx: idx_shard,
                node,
                output: out,
            });
        }
    }
    (fsts, shards_f, states)
}