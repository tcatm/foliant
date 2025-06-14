//! Modular multi-shard listing with lazy bitmap-based filtering.

mod streamer;
mod lazy_filter;
mod lazy_tag_filter;
mod lazy_search_filter;

pub use streamer::MultiShardListStreamer;
pub use lazy_filter::{LazyShardFilter, ComposedFilter, IdentityFilter, FilterComposition};
pub use lazy_tag_filter::{LazyTagFilter, TagFilterConfig};
pub use lazy_search_filter::LazySearchFilter;

// Re-export LazyTagFilter as TagFilter for backward compatibility
pub use lazy_tag_filter::LazyTagFilter as TagFilter;