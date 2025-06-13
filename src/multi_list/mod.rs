//! Modular multi-shard listing with lazy bitmap-based filtering.

mod bitmap_filter;
mod streamer;
mod lazy_filter;
mod lazy_tag_filter;

pub use bitmap_filter::ShardBitmapFilter;
pub use streamer::MultiShardListStreamer;
pub use lazy_filter::{LazyShardFilter, ComposedFilter, IdentityFilter, FilterComposition};
pub use lazy_tag_filter::{LazyTagFilter, TagFilterConfig};

// Re-export LazyTagFilter as TagFilter for backward compatibility
pub use lazy_tag_filter::LazyTagFilter as TagFilter;