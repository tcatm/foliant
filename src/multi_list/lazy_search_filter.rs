//! Lazy search filter implementation that delays search computation until needed.

use super::lazy_filter::LazyShardFilter;
use crate::Shard;
use crate::payload_store::PayloadCodec;
use roaring::RoaringBitmap;
use serde::de::DeserializeOwned;
use std::error::Error;

/// Lazy search filter that performs substring search on demand for each shard.
/// 
/// Unlike eager filtering, this filter doesn't perform any searches
/// until explicitly asked for a specific shard's bitmap.
pub struct LazySearchFilter {
    query: String,
}

impl LazySearchFilter {
    /// Create a new lazy search filter with the given query.
    pub fn new(query: String) -> Self {
        Self { query }
    }
}

impl<V, C> LazyShardFilter<V, C> for LazySearchFilter
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    fn compute_bitmap(&self, shard: &Shard<V, C>) -> Result<Option<RoaringBitmap>, Box<dyn Error>> {
        // Early return if no search index exists
        if let Some(search_idx) = &shard.search {
            // Get candidate IDs from search index
            let candidate_ids = search_idx.search(&self.query)?;
            
            if candidate_ids.is_empty() {
                return Ok(None);
            }
            
            // Build bitmap from candidate IDs
            let mut bitmap = RoaringBitmap::new();
            
            // We need to verify each candidate by actually checking the key
            // This handles false positives from n-gram matching
            for id in candidate_ids {
                if let Ok(Some(entry)) = shard.get_entry(id) {
                    if let crate::Entry::Key(key, _, _) = entry {
                        // Verify the key actually contains the query (case-insensitive)
                        if key.to_lowercase().contains(&self.query.to_lowercase()) {
                            bitmap.insert(id as u32);
                        }
                    }
                }
            }
            
            if bitmap.is_empty() {
                Ok(None)
            } else {
                Ok(Some(bitmap))
            }
        } else {
            // No search index = no results
            Ok(None)
        }
    }
    
    fn debug_name(&self) -> &str {
        "LazySearchFilter"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_lazy_search_filter_creation() {
        let filter = LazySearchFilter::new("test".to_string());
        assert_eq!(filter.query, "test");
    }
}