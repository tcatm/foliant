//! Lazy search filter implementation that delays search computation until needed.

use super::lazy_filter::LazyShardFilter;
use crate::{Shard, Entry};
use crate::payload_store::PayloadCodec;
use crate::search_index::{normalize_text_unicode, strip_diacritics};
use roaring::RoaringBitmap;
use serde::de::DeserializeOwned;
use std::error::Error;

/// Lazy search filter that performs substring search on demand for each shard.
/// 
/// Unlike eager filtering, this filter doesn't perform any searches
/// until explicitly asked for a specific shard's bitmap.
/// 
/// Supports multiple included and excluded search terms:
/// - All included terms must be present in a key for it to match
/// - Any excluded term present in a key will exclude it from results
pub struct LazySearchFilter {
    /// Terms that must all be present (AND operation)
    included: Vec<String>,
    /// Terms that must not be present (any match excludes the key)
    excluded: Vec<String>,
}

impl LazySearchFilter {
    /// Create a new lazy search filter with a single query term.
    pub fn new(query: String) -> Self {
        Self { 
            included: vec![query],
            excluded: Vec::new(),
        }
    }
    
    /// Create a new lazy search filter with multiple included and excluded terms.
    /// 
    /// All included terms must be present (AND operation).
    /// Any excluded term will exclude the key from results.
    pub fn with_terms(included: Vec<String>, excluded: Vec<String>) -> Self {
        Self { included, excluded }
    }
    
    /// Builder-style method to add an included term.
    pub fn include(mut self, term: String) -> Self {
        self.included.push(term);
        self
    }
    
    /// Builder-style method to add an excluded term.
    pub fn exclude(mut self, term: String) -> Self {
        self.excluded.push(term);
        self
    }
}

impl<V, C> LazyShardFilter<V, C> for LazySearchFilter
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    fn compute_bitmap(&self, shard: &Shard<V, C>) -> Result<Option<RoaringBitmap>, Box<dyn Error>> {
        // Early return if no search index exists or no included terms
        let search_idx = match &shard.search {
            Some(idx) => idx,
            None => return Ok(None),
        };
        
        if self.included.is_empty() {
            return Ok(None);
        }
        
        // Get candidate bitmaps for all included terms
        let mut included_bitmaps = Vec::new();
        
        for term in &self.included {
            let candidate_ids = search_idx.search(term)?;
            if candidate_ids.is_empty() {
                // If any included term has no matches, result is empty
                return Ok(None);
            }
            
            // Convert to bitmap
            let mut term_bitmap = RoaringBitmap::new();
            for id in candidate_ids {
                term_bitmap.insert(id as u32);
            }
            included_bitmaps.push(term_bitmap);
        }
        
        // If we have no bitmaps at all, no matches
        if included_bitmaps.is_empty() {
            return Ok(None);
        }
        
        // Intersect all included term bitmaps (AND operation)
        let mut result_bitmap = included_bitmaps[0].clone();
        for bitmap in included_bitmaps.iter().skip(1) {
            result_bitmap &= bitmap;
            if result_bitmap.is_empty() {
                return Ok(None);
            }
        }
        
        // Return candidate bitmap without verification
        // Verification will happen in verify_entry()
        if result_bitmap.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result_bitmap))
        }
    }
    
    fn verify_entry(&self, entry: &Entry<V>) -> bool {
        if let Entry::Key(key, _, _) = entry {
            // Normalize and strip diacritics for accent-insensitive matching
            let key_normalized = normalize_text_unicode(key);
            let key_stripped = strip_diacritics(&key_normalized);
            
            // Check all included terms are present
            let all_included = self.included.iter()
                .all(|term| {
                    let term_normalized = normalize_text_unicode(term);
                    let term_stripped = strip_diacritics(&term_normalized);
                    // Check both normalized and stripped versions
                    key_normalized.contains(&term_normalized) || key_stripped.contains(&term_stripped)
                });
            
            // Check no excluded terms are present  
            let none_excluded = self.excluded.iter()
                .all(|term| {
                    let term_normalized = normalize_text_unicode(term);
                    let term_stripped = strip_diacritics(&term_normalized);
                    // Exclude if found in either normalized or stripped version
                    !key_normalized.contains(&term_normalized) && !key_stripped.contains(&term_stripped)
                });
            
            all_included && none_excluded
        } else {
            false
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
        assert_eq!(filter.included, vec!["test"]);
        assert!(filter.excluded.is_empty());
    }
    
    #[test]
    fn test_lazy_search_filter_with_terms() {
        let filter = LazySearchFilter::with_terms(
            vec!["hello".to_string(), "world".to_string()],
            vec!["bad".to_string(), "evil".to_string()]
        );
        assert_eq!(filter.included, vec!["hello", "world"]);
        assert_eq!(filter.excluded, vec!["bad", "evil"]);
    }
    
    #[test]
    fn test_lazy_search_filter_builder() {
        let filter = LazySearchFilter::new("test".to_string())
            .include("another".to_string())
            .exclude("bad".to_string())
            .exclude("worse".to_string());
        
        assert_eq!(filter.included, vec!["test", "another"]);
        assert_eq!(filter.excluded, vec!["bad", "worse"]);
    }
}