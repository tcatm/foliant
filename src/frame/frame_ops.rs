use super::frame::{FrameMulti, FrameState, TransitionRef};
use crate::shard::SharedMmap;
use crate::payload_store::PayloadCodec;
use crate::Shard;
use fst::raw::{Fst, Output};
use roaring::RoaringBitmap;
use serde::de::DeserializeOwned;
use smallvec::SmallVec;

/// Combined per-shard context for streamlined iteration
pub struct ShardInfo<'a, V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    pub fst: &'a Fst<SharedMmap>,
    pub shard: &'a Shard<V, C>,
    pub bitmap: Option<RoaringBitmap>,
}

/// Build frame states for the given shard_infos and byte sequence.
/// Uses the same vectorized traversal logic as streamer's `next()` to descend all shards in parallel.
pub fn build_frame_states<'a, V, C>(
    infos: &[ShardInfo<'a, V, C>],
    bytes: &[u8],
) -> Vec<FrameState<'a>>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
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
        build_frame_transitions_and_bounds(&mut frame, infos);
        if let Some((_, refs)) = frame.trans.iter().find(|bucket| bucket.0 == b) {
            frame = create_frame_with_transitions(
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

/// Fused function that builds frame transitions and computes bounds in a single pass.
/// For each shard: collect transitions, sort by lower bound to compute upper bounds,
/// then merge into frame.trans buckets sorted by input byte.
pub fn build_frame_transitions_and_bounds<'a, V, C>(
    frame: &mut FrameMulti<'a>,
    infos: &[ShardInfo<'a, V, C>],
)
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    // 1. Prepare empty buckets + small index-map for grouping by byte
    let mut buckets: SmallVec<[(u8, SmallVec<[TransitionRef; 4]>); 8]> = SmallVec::new();
    let mut byte_to_bucket_idx: [Option<usize>; 256] = [None; 256];

    // 2. For each shard-state, collect, sort-by-LB, assign UBs, then merge into buckets
    for state in &frame.states {
        let s = state.shard_idx;
        let node = &state.node;
        let base_out = state.output;
        let shard_ub = state.ub;

        // 2a. Build a tiny Vec of (inp, addr, out, full_lb)
        let mut tmp: Vec<(u8, usize, Output, u64)> = Vec::with_capacity(node.len());
        for ti in 0..node.len() {
            let tr = node.transition(ti);
            let out_cat = base_out.cat(tr.out);
            let child_node = infos[s].fst.node(tr.addr);
            let full_lb = out_cat.cat(child_node.final_output()).value();
            tmp.push((tr.inp, tr.addr, out_cat, full_lb));
        }

        // 2b. Sort by full_lb to find siblings
        tmp.sort_unstable_by_key(|&(_, _, _, lb)| lb);

        // 2c. Compute ubs by peeking at next sibling's lb (or shard_ub for last)
        let mut with_bounds: Vec<(u8, usize, Output, u64, u64)> = Vec::with_capacity(tmp.len());
        for i in 0..tmp.len() {
            let (inp, addr, out_cat, lb) = tmp[i];
            let ub = if i + 1 < tmp.len() {
                tmp[i + 1].3.saturating_sub(1)
            } else {
                shard_ub
            };
            with_bounds.push((inp, addr, out_cat, lb, ub));
        }

        // 2d. Sort this shard's transitions back by inp so we maintain input order
        with_bounds.sort_unstable_by_key(|&(inp, _, _, _, _)| inp);

        // 2e. Dump into the global buckets
        for (inp, addr, out_cat, _lb, ub) in with_bounds {
            let bidx = match byte_to_bucket_idx[inp as usize] {
                Some(idx) => idx,
                None => {
                    let idx = buckets.len();
                    buckets.push((inp, SmallVec::new()));
                    byte_to_bucket_idx[inp as usize] = Some(idx);
                    idx
                }
            };
            buckets[bidx].1.push((s, addr, out_cat, ub));
        }
    }

    // 3. The buckets are already grouped by byte, just need to be sorted
    buckets.sort_unstable_by_key(|&(byte, _)| byte);
    frame.trans = buckets;
    frame.trans_idx = 0;
}

/// Create a new frame and populate it with states for the given transition references.
/// Uses precomputed bounds from transition refs for O(1) operation.
pub fn create_frame_with_transitions<'a, V, C>(
    infos: &[ShardInfo<'a, V, C>],
    frame_pool: Option<&mut Vec<FrameMulti<'a>>>,
    shards_len: usize,
    prefix_len: usize,
    _frame: &FrameMulti<'a>,
    refs: &[TransitionRef]
) -> FrameMulti<'a>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
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
            ub,
        });
    }
    
    new_frame
}

/// Count descendant keys under transition references with precomputed bounds (O(1) per call).
pub fn count_children_for_transition_refs<V, C>(
    refs: &[TransitionRef],
    shard_infos: &[ShardInfo<V, C>],
) -> Option<usize>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    let mut total = 0usize;
    let use_bitmaps = shard_infos.iter().any(|info| info.bitmap.is_some());
    
    for &(shard_idx, _addr, output, ub) in refs {
        let should_count = if use_bitmaps {
            shard_infos[shard_idx].bitmap.is_some()
        } else {
            true
        };
        
        if should_count {
            let lb = output.value();
            
            if ub >= lb {
                let count = if use_bitmaps {
                    if let Some(ref bm) = shard_infos[shard_idx].bitmap {
                        bm.range(lb as u32..=ub as u32).count()
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
    
    Some(total)
}