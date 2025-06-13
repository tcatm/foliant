//! Lazy tag filter implementation that delays bitmap computation until needed.

use super::lazy_filter::LazyShardFilter;
use crate::{Shard, TagMode};
use crate::payload_store::PayloadCodec;
use roaring::RoaringBitmap;
use serde::de::DeserializeOwned;
use std::error::Error;

/// Configuration for tag filtering.
#[derive(Debug, Clone)]
pub struct TagFilterConfig {
    pub include_tags: Vec<String>,
    pub exclude_tags: Vec<String>,
    pub mode: TagMode,
}

/// Lazy tag filter that computes bitmaps on demand for each shard.
/// 
/// Unlike the eager TagFilterBitmap, this filter doesn't compute any bitmaps
/// until explicitly asked for a specific shard's bitmap.
pub struct LazyTagFilter {
    // Pre-computed lowercase versions to avoid repeated allocations
    include_tags_lower: Vec<String>,
    exclude_tags_lower: Vec<String>,
    mode: TagMode,
}

impl LazyTagFilter {
    /// Create a new lazy tag filter with the given configuration.
    pub fn new(include_tags: Vec<String>, exclude_tags: Vec<String>, mode: TagMode) -> Self {
        // Pre-compute lowercase versions once
        let include_tags_lower = include_tags.iter().map(|t| t.to_lowercase()).collect();
        let exclude_tags_lower = exclude_tags.iter().map(|t| t.to_lowercase()).collect();
        
        Self {
            include_tags_lower,
            exclude_tags_lower,
            mode,
        }
    }
    
    /// Create from a TagFilterConfig
    pub fn from_config(config: &TagFilterConfig) -> Self {
        Self::new(
            config.include_tags.clone(),
            config.exclude_tags.clone(),
            config.mode,
        )
    }
}

impl<V, C> LazyShardFilter<V, C> for LazyTagFilter
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    fn compute_bitmap(&self, shard: &Shard<V, C>) -> Result<Option<RoaringBitmap>, Box<dyn Error>> {
        // Early return if no tag index exists and we have include filters
        if !self.include_tags_lower.is_empty() && shard.tags.is_none() {
            return Ok(None);
        }
        
        // Build the include set, or the full range if empty
        let mut bm = if !self.include_tags_lower.is_empty() {
            let mut acc: Option<RoaringBitmap> = None;
            if let Some(idx) = &shard.tags {
                for tag in &self.include_tags_lower {
                    if let Some(sub) = idx.get(tag)? {
                        // Check if sub bitmap is empty - avoid cloning if so
                        if sub.is_empty() && self.mode == TagMode::And {
                            // Empty bitmap in AND mode = result will be empty
                            return Ok(None);
                        }
                        
                        let new_acc = match acc {
                            Some(prev) if self.mode == TagMode::And => {
                                let intersection = prev & &sub;
                                // Early return if intersection becomes empty
                                if intersection.is_empty() {
                                    return Ok(None);
                                }
                                intersection
                            },
                            Some(prev) => prev | &sub,
                            None => sub.clone(), // Only clone when we know it's non-empty and needed
                        };
                        acc = Some(new_acc);
                    } else if self.mode == TagMode::And {
                        // Tag not found in AND mode = empty result
                        return Ok(None);
                    }
                }
            }
            
            let result = acc.unwrap_or_default();
            // Early return if result is empty after include processing
            if result.is_empty() {
                return Ok(None);
            }
            result
        } else {
            // No include tags = accept all
            let max_id = shard.fst.len() as u32;
            if max_id == 0 {
                // Empty shard
                return Ok(None);
            }
            let mut full = RoaringBitmap::new();
            full.insert_range(0..max_id);
            full
        };

        // Subtract any exclude sets
        if let Some(idx) = &shard.tags {
            for tag in &self.exclude_tags_lower {
                if let Some(sub) = idx.get(tag)? {
                    bm -= &sub;
                    // Early return if bitmap becomes empty after exclusion
                    if bm.is_empty() {
                        return Ok(None);
                    }
                }
            }
        }
        
        // Final check (should be redundant now, but kept for safety)
        if bm.is_empty() {
            Ok(None)
        } else {
            Ok(Some(bm))
        }
    }
    
    fn debug_name(&self) -> &str {
        "LazyTagFilter"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::TagFilterConfig;
    
    #[test]
    fn test_lazy_tag_filter_creation() {
        let filter = LazyTagFilter::new(
            vec!["Tag1".to_string()],
            vec!["Tag2".to_string()],
            TagMode::Or,
        );
        assert_eq!(filter.include_tags_lower, vec!["tag1"]);
        assert_eq!(filter.exclude_tags_lower, vec!["tag2"]);
        assert_eq!(filter.mode, TagMode::Or);
    }
    
    #[test]
    fn test_from_config() {
        let config = TagFilterConfig {
            include_tags: vec!["Foo".to_string()],
            exclude_tags: vec!["Bar".to_string()],
            mode: TagMode::And,
        };
        let filter = LazyTagFilter::from_config(&config);
        assert_eq!(filter.include_tags_lower, vec!["foo"]);
        assert_eq!(filter.exclude_tags_lower, vec!["bar"]);
        assert_eq!(filter.mode, config.mode);
    }
}