use std::collections::BTreeMap;
use roaring::RoaringBitmap;

use crate::Shard;
use crate::payload_store::PayloadCodec;
use serde::de::DeserializeOwned;

/// Index that maps common prefixes to shard indices for fast shard filtering.
/// This is used as a pre-filter before the actual FST traversal in build_frame_states.
pub struct PrefixIndex {
    /// BTreeMap from common prefix bytes to bitmap of shard indices
    /// Using BTreeMap for efficient prefix range queries and RoaringBitmap for space efficiency
    prefix_to_shards: BTreeMap<Box<[u8]>, RoaringBitmap>,
    /// Shards with empty common prefix (must always be checked)
    empty_prefix_shards: RoaringBitmap,
}

impl PrefixIndex {
    /// Create a new empty prefix index.
    pub fn new() -> Self {
        PrefixIndex {
            prefix_to_shards: BTreeMap::new(),
            empty_prefix_shards: RoaringBitmap::new(),
        }
    }
    
    /// Add a shard to the index.
    pub fn add_shard<V, C>(&mut self, shard: &Shard<V, C>, shard_idx: usize)
    where
        V: DeserializeOwned,
        C: PayloadCodec,
    {
        let common_prefix = shard.common_prefix();
        if !common_prefix.is_empty() {
            let prefix_bytes = common_prefix.into_bytes().into_boxed_slice();
            self.prefix_to_shards.entry(prefix_bytes)
                .or_insert_with(RoaringBitmap::new)
                .insert(shard_idx as u32);
        } else {
            // Empty common prefix means this shard could contain any prefix
            // We'll need to always check these shards
            self.empty_prefix_shards.insert(shard_idx as u32);
        }
    }
    
    /// Build a prefix index from a slice of shards.
    /// For each shard, extracts its common prefix and maps it to the shard index.
    pub fn build<V, C>(shards: &[Shard<V, C>]) -> Self
    where
        V: DeserializeOwned,
        C: PayloadCodec,
    {
        let mut index = Self::new();
        for (idx, shard) in shards.iter().enumerate() {
            index.add_shard(shard, idx);
        }
        index
    }
    
    /// Find shard indices that might contain the given prefix.
    /// Returns a bitmap of shards whose common prefix is compatible with the query prefix.
    /// 
    /// Optimized implementation using range queries instead of full scan.
    /// Time complexity: O(log #prefixes + result_size) vs O(#prefixes)
    pub fn find_candidate_shards(&self, prefix: &[u8]) -> RoaringBitmap {
        let mut candidates = self.empty_prefix_shards.clone();
        
        if prefix.is_empty() {
            // Empty query matches all shards
            for bitmap in self.prefix_to_shards.values() {
                candidates |= bitmap;
            }
            return candidates;
        }
        
        // 1. Walk the prefix path: "a", "ap", ..., "apple" if query = "apple"
        // These are shards whose common prefix is shorter than or equal to query
        let mut buf = Vec::with_capacity(prefix.len());
        for &byte in prefix {
            buf.push(byte);
            if let Some(bitmap) = self.prefix_to_shards.get(&buf[..]) {
                candidates |= bitmap;
            }
        }
        
        // 2. Lexicographic range: all keys in ["apple", "apq") if query = "apple"
        // These are shards whose common prefix is longer than query but starts with it
        let upper_bound = lexicographic_successor(prefix);
        for (_, bitmap) in self.prefix_to_shards.range(prefix.to_vec().into_boxed_slice()..upper_bound.into_boxed_slice()) {
            candidates |= bitmap;
        }
        
        candidates
    }
    
    /// Find shard indices that might contain the given prefix, returning as Vec.
    /// Convenience method that converts the bitmap to a vector.
    pub fn find_candidate_shards_vec(&self, prefix: &[u8]) -> Vec<usize> {
        self.find_candidate_shards(prefix)
            .iter()
            .map(|idx| idx as usize)
            .collect()
    }
    
    /// Get the number of unique prefixes in the index
    pub fn prefix_count(&self) -> usize {
        self.prefix_to_shards.len()
    }
    
    /// Get the total number of shards in the index
    pub fn shard_count(&self) -> usize {
        let prefix_shards: usize = self.prefix_to_shards.values().map(|v| v.len() as usize).sum();
        let empty_shards = self.empty_prefix_shards.len() as usize;
        prefix_shards + empty_shards
    }
}

/// Compute the lexicographic successor of a byte slice.
/// 
/// For input "apple", returns "applf" (last byte incremented).
/// For input ending in 0xFF like "app\xFF", returns "aq".
/// For input that's all 0xFF, returns original + 0x00.
fn lexicographic_successor(bytes: &[u8]) -> Vec<u8> {
    let mut result = bytes.to_vec();
    
    // Find the rightmost byte that can be incremented (not 0xFF)
    if let Some(pos) = (0..result.len()).rev().find(|&i| result[i] != 0xFF) {
        result[pos] += 1;
        result.truncate(pos + 1);  // Remove everything after the incremented byte
    } else {
        // All bytes are 0xFF, append a 0x00
        result.push(0x00);
    }
    
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helper to create a prefix index with given prefix-to-shard mappings
    fn create_test_index(entries: &[(&[u8], usize)]) -> PrefixIndex {
        let mut index = PrefixIndex::new();
        for &(prefix, shard_idx) in entries {
            if prefix.is_empty() {
                index.empty_prefix_shards.insert(shard_idx as u32);
            } else {
                index.prefix_to_shards.entry(prefix.to_vec().into_boxed_slice())
                    .or_insert_with(RoaringBitmap::new)
                    .insert(shard_idx as u32);
            }
        }
        index
    }


    #[test]
    fn test_exact_match() {
        // Case 1: Exact match - ["app"] → shard 0, query "app" → {0}
        let index = create_test_index(&[(b"app", 0)]);
        let result = index.find_candidate_shards_vec(b"app");
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_shard_shorter() {
        // Case 2: Shard shorter - ["app"] → shard 0, query "apple" → {0}
        let index = create_test_index(&[(b"app", 0)]);
        let result = index.find_candidate_shards_vec(b"apple");
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_shard_longer() {
        // Case 3: Shard longer - ["apple"] → shard 0, query "app" → {0}
        let index = create_test_index(&[(b"apple", 0)]);
        let result = index.find_candidate_shards_vec(b"app");
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_mixed_prefixes() {
        // Case 4: Mixed - ["app"(0), "banana"(1), "apple"(2)], query "app" → {0,2}
        let index = create_test_index(&[
            (b"app", 0),
            (b"banana", 1),
            (b"apple", 2),
        ]);
        let mut result = index.find_candidate_shards_vec(b"app");
        result.sort();
        assert_eq!(result, vec![0, 2]);
    }

    #[test]
    fn test_empty_prefix_shard() {
        // Case 5: Empty prefix shard - [""(0), "dog"(1)], query "cat" → {0}
        let index = create_test_index(&[
            (b"", 0),
            (b"dog", 1),
        ]);
        let result = index.find_candidate_shards_vec(b"cat");
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_upper_bound_edge() {
        // Case 6: Upper-bound edge - ["\xFF"] → shard 0, query "" → {0}
        let index = create_test_index(&[(b"\xFF", 0)]);
        let result = index.find_candidate_shards_vec(b"");
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_all_match_query() {
        // Case 8: All-match query - multiple shards, query "" (empty) → all shards
        let entries: Vec<_> = (0..10).map(|i| {
            let prefix = format!("prefix{}", i);
            (prefix.as_bytes().to_vec(), i)
        }).collect();
        
        let mut index = PrefixIndex::new();
        for (prefix_vec, shard_idx) in &entries {
            index.prefix_to_shards.entry(prefix_vec.clone().into_boxed_slice())
                .or_insert_with(RoaringBitmap::new)
                .insert(*shard_idx as u32);
        }
        
        let result = index.find_candidate_shards_vec(b"");
        let mut expected: Vec<usize> = (0..10).collect();
        expected.sort();
        let mut actual = result;
        actual.sort();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_lexicographic_successor() {
        // Test the helper function
        assert_eq!(lexicographic_successor(b"apple"), b"applf");
        assert_eq!(lexicographic_successor(b"app\xFF"), b"apq");  // Fixed: should be "apq" not "aq"
        assert_eq!(lexicographic_successor(b"\xFF\xFF"), b"\xFF\xFF\x00");
        assert_eq!(lexicographic_successor(b""), b"\x00");
        assert_eq!(lexicographic_successor(b"a"), b"b");
        assert_eq!(lexicographic_successor(b"ap\xFF"), b"aq");  // This would be "aq"
    }

    #[test]
    fn test_many_prefixes_performance() {
        // Case 7: Many prefixes - create 1000 synthetic prefixes and test performance
        let mut entries = Vec::new();
        
        // Generate diverse prefixes of varying lengths
        for i in 0..1000 {
            let prefix = match i % 4 {
                0 => format!("a{:03}", i),
                1 => format!("b{:03}", i),
                2 => format!("prefix/deep/path{:03}", i),
                _ => format!("x{:03}/y{:03}", i, i + 1000),
            };
            entries.push((prefix.into_bytes(), i));
        }
        
        let mut index = PrefixIndex::new();
        for (prefix_vec, shard_idx) in &entries {
            index.prefix_to_shards.entry(prefix_vec.clone().into_boxed_slice())
                .or_insert_with(RoaringBitmap::new)
                .insert(*shard_idx as u32);
        }
        
        // Test with a query that should match some but not all prefixes
        let query = b"prefix/deep/";
        let result = index.find_candidate_shards_vec(query);
        
        // Should find shards whose prefixes start with "prefix/deep/"
        assert!(!result.is_empty());
        assert!(result.len() < 1000); // Should be much smaller than total
        
        // Verify correctness: all returned shards should be relevant
        for &shard_idx in &result {
            let (prefix_vec, _) = &entries[shard_idx];
            let prefix = prefix_vec.as_slice();
            
            // Either the query starts with this prefix, or this prefix starts with query
            assert!(
                query.starts_with(prefix) || prefix.starts_with(query),
                "Shard {} with prefix {:?} should not match query {:?}",
                shard_idx, std::str::from_utf8(prefix), std::str::from_utf8(query)
            );
        }
    }

    #[test]
    fn test_complex_prefix_scenarios() {
        // Test various complex scenarios
        let index = create_test_index(&[
            (b"", 0),           // Empty prefix shard
            (b"a", 1),          // Single byte
            (b"ab", 2),         // Two bytes
            (b"abc", 3),        // Three bytes
            (b"abd", 4),        // Different third byte
            (b"b", 5),          // Different first byte
            (b"prefix/", 6),    // With delimiter
            (b"prefix/deep", 7), // Longer with same start
        ]);
        
        // Query for "ab" should match shards: 0 (empty), 1 (a), 2 (ab), 3 (abc), 4 (abd)
        let mut result = index.find_candidate_shards_vec(b"ab");
        result.sort();
        assert_eq!(result, vec![0, 1, 2, 3, 4]);
        
        // Query for "prefix/" should match: 0 (empty), 6 (prefix/), 7 (prefix/deep)
        let mut result = index.find_candidate_shards_vec(b"prefix/");
        result.sort();
        assert_eq!(result, vec![0, 6, 7]);
        
        // Query for "xyz" should only match: 0 (empty)
        let result = index.find_candidate_shards_vec(b"xyz");
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_range_boundary_conditions() {
        // Test edge cases around range boundaries
        let index = create_test_index(&[
            (b"aa", 0),
            (b"ab", 1),
            (b"aba", 2),
            (b"abb", 3),
            (b"ac", 4),
            (b"b", 5),
        ]);
        
        // Query "ab" should find: 1 (ab), 2 (aba), 3 (abb)
        let mut result = index.find_candidate_shards_vec(b"ab");
        result.sort();
        assert_eq!(result, vec![1, 2, 3]);
        
        // Query "aba" should find: 1 (ab), 2 (aba)
        let mut result = index.find_candidate_shards_vec(b"aba");
        result.sort();
        assert_eq!(result, vec![1, 2]);
    }

    #[test]
    fn test_query_longer_than_index_prefixes() {
        // Test case where query prefix is longer than any prefix in the index
        // Index has prefixes: "a", "b", "c"
        // Query: "aa" should return shard with prefix "a" since "aa" starts with "a"
        let index = create_test_index(&[
            (b"a", 0),
            (b"b", 1), 
            (b"c", 2),
        ]);
        
        // Query "aa" should find shard 0 (prefix "a") since "aa" starts with "a"
        let result = index.find_candidate_shards_vec(b"aa");
        assert_eq!(result, vec![0]);
        
        // Query "bb" should find shard 1 (prefix "b") since "bb" starts with "b"
        let result = index.find_candidate_shards_vec(b"bb");
        assert_eq!(result, vec![1]);
        
        // Query "xyz" should find no shards since it doesn't start with any prefix
        let result = index.find_candidate_shards_vec(b"xyz");
        assert_eq!(result, Vec::<usize>::new());
        
        // Add an empty prefix shard to test that case too
        let index = create_test_index(&[
            (b"", 0),   // Empty prefix shard
            (b"a", 1),
            (b"b", 2), 
            (b"c", 3),
        ]);
        
        // Query "xyz" should now find shard 0 (empty prefix) since empty prefix matches anything
        let result = index.find_candidate_shards_vec(b"xyz");
        assert_eq!(result, vec![0]);
        
        // Query "aa" should find both shard 0 (empty prefix) and shard 1 (prefix "a")
        let mut result = index.find_candidate_shards_vec(b"aa");
        result.sort();
        assert_eq!(result, vec![0, 1]);
    }
}