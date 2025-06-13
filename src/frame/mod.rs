//! Frame-based FST traversal infrastructure for efficient multi-shard operations

mod frame;
mod frame_ops;

pub use frame::{FrameMulti, FrameState};
pub use frame_ops::{
    ShardInfo, 
    build_frame_states, 
    build_frame_transitions_and_bounds,
    create_frame_with_transitions,
    count_children_for_transition_refs
};