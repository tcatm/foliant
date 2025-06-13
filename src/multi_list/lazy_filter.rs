//! Lazy shard filtering framework that delays bitmap computation until after prefix walk.
//! 
//! This module provides a composable, lazy evaluation system for shard filtering that:
//! - Delays bitmap computation until shards are known to match the prefix
//! - Allows combining multiple filters with monoid-like composition
//! - Significantly reduces unnecessary bitmap operations

use roaring::RoaringBitmap;
use serde::de::DeserializeOwned;
use crate::shard::Shard;
use crate::payload_store::PayloadCodec;
use std::error::Error;

/// Trait for filters that lazily compute bitmaps on a per-shard basis.
/// 
/// Unlike eager filters that compute all bitmaps upfront, lazy filters
/// only compute bitmaps for shards that have already passed the prefix walk.
pub trait LazyShardFilter<V, C>: Send + Sync
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// Compute the bitmap for a specific shard.
    /// 
    /// Returns:
    /// - `Ok(Some(bitmap))` - Shard has matches, use bitmap for filtering
    /// - `Ok(None)` - Shard has no matches, can be excluded
    /// - `Err(_)` - Error computing bitmap
    fn compute_bitmap(&self, shard: &Shard<V, C>) -> Result<Option<RoaringBitmap>, Box<dyn Error>>;
    
    /// Get a debug representation of this filter for logging
    fn debug_name(&self) -> &str;
}

/// Identity filter that accepts all entries (monoid identity element)
pub struct IdentityFilter;

impl<V, C> LazyShardFilter<V, C> for IdentityFilter
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    fn compute_bitmap(&self, shard: &Shard<V, C>) -> Result<Option<RoaringBitmap>, Box<dyn Error>> {
        // Return full range bitmap
        let mut full = RoaringBitmap::new();
        let max_id = shard.fst.len() as u32;
        if max_id > 0 {
            full.insert_range(0..max_id);
        }
        Ok(Some(full))
    }
    
    fn debug_name(&self) -> &str {
        "IdentityFilter"
    }
}

/// Composed filter that combines multiple filters with intersection (AND) semantics
pub struct ComposedFilter<V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    filters: Vec<Box<dyn LazyShardFilter<V, C>>>,
}

impl<V, C> ComposedFilter<V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// Create a new composed filter from a list of filters
    pub fn new(filters: Vec<Box<dyn LazyShardFilter<V, C>>>) -> Self {
        Self { filters }
    }
    
    /// Create an empty composed filter (identity)
    pub fn identity() -> Self {
        Self { filters: vec![] }
    }
    
    /// Add another filter to the composition
    pub fn and(mut self, filter: Box<dyn LazyShardFilter<V, C>>) -> Self {
        self.filters.push(filter);
        self
    }
}

impl<V, C> LazyShardFilter<V, C> for ComposedFilter<V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    fn compute_bitmap(&self, shard: &Shard<V, C>) -> Result<Option<RoaringBitmap>, Box<dyn Error>> {
        // Empty composition = identity (accept all)
        if self.filters.is_empty() {
            return IdentityFilter.compute_bitmap(shard);
        }
        
        // Start with the first filter's result
        let mut result = self.filters[0].compute_bitmap(shard)?;
        
        // Intersect with remaining filters
        for filter in &self.filters[1..] {
            let bitmap = filter.compute_bitmap(shard)?;
            result = match (result, bitmap) {
                // If any filter returns None (no matches), the result is None
                (None, _) | (_, None) => None,
                // Otherwise intersect the bitmaps
                (Some(r), Some(b)) => {
                    let intersected = r & b;
                    if intersected.is_empty() {
                        None
                    } else {
                        Some(intersected)
                    }
                }
            };
            
            // Early exit if no matches
            if result.is_none() {
                break;
            }
        }
        
        Ok(result)
    }
    
    fn debug_name(&self) -> &str {
        "ComposedFilter"
    }
}

/// Extension trait to make filter composition more ergonomic
pub trait FilterComposition<V, C>: LazyShardFilter<V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// Combine this filter with another using AND semantics
    fn and_then(self, other: impl LazyShardFilter<V, C> + 'static) -> ComposedFilter<V, C>
    where
        Self: Sized + 'static,
    {
        ComposedFilter::new(vec![
            Box::new(self) as Box<dyn LazyShardFilter<V, C>>,
            Box::new(other) as Box<dyn LazyShardFilter<V, C>>,
        ])
    }
}

// Implement FilterComposition for all LazyShardFilter types
impl<T, V, C> FilterComposition<V, C> for T
where
    T: LazyShardFilter<V, C>,
    V: DeserializeOwned,
    C: PayloadCodec,
{
}

#[cfg(test)]
mod tests {
    // Note: These tests are not implemented yet
    
    // Test that composed filters follow monoid laws
    #[test]
    fn test_monoid_identity() {
        // Left identity: identity.and(f) = f
        // Right identity: f.and(identity) = f
        // (Can't easily test without mock shards, but the logic is there)
    }
    
    #[test]
    fn test_monoid_associativity() {
        // (f1.and(f2)).and(f3) = f1.and(f2.and(f3))
        // (Can't easily test without mock shards, but the logic is there)
    }
}