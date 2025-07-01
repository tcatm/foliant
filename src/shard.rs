use memmap2::Mmap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Shared memory map cloneable via reference counting.
#[derive(Clone)]
pub struct SharedMmap(Arc<Mmap>);

impl From<Mmap> for SharedMmap {
    fn from(m: Mmap) -> Self {
        SharedMmap(Arc::new(m))
    }
}

impl AsRef<[u8]> for SharedMmap {
    fn as_ref(&self) -> &[u8] {
        (&*self.0).as_ref()
    }
}
use fst::Map;
use serde::de::DeserializeOwned;

use crate::error::{IndexError, Result};
use crate::lookup_table_store::LookupTableStore;
use crate::payload_store::{CborPayloadCodec, PayloadCodec, PayloadStore};
use crate::tag_index::TagIndex;
use crate::search_index::{SearchIndex, normalize_text_unicode, strip_diacritics, GRAM_SIZE};
use unicode_segmentation::UnicodeSegmentation;
use fst::automaton::Str;
use fst::raw::Fst as RawFst;
use fst::Automaton;
use fst::IntoStreamer;
use fst::Streamer as FstStreamer;
/// One shard of a database: a memory-mapped FST map and its payload store.
pub struct Shard<V, C: PayloadCodec = CborPayloadCodec>
where
    V: DeserializeOwned,
{
    /// Path to the index file
    pub(crate) idx_path: PathBuf,
    /// Memory-mapped raw index data (shared)
    pub(crate) idx_mmap: SharedMmap,
    /// Parsed FST map for lookups (backed by a shared memory map)
    pub(crate) fst: Map<SharedMmap>,
    /// Read-only lookup table for payload indirection
    pub(crate) lookup: LookupTableStore,
    /// Associated payload store
    pub(crate) payload: PayloadStore<V, C>,
    /// Optional tag index for this shard (variant-C .tags file)
    pub(crate) tags: Option<TagIndex>,
    /// Optional search index for this shard (.search/ folder)
    pub(crate) search: Option<SearchIndex>,
}

impl<V, C> Shard<V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// Open a shard from the given index path.
    pub fn open(idx_path: &Path) -> Result<Self> {
        let payload_path = idx_path.with_extension("payload");
        let idx_file = File::open(&idx_path).map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to open index file {:?}: {}", idx_path, e),
            ))
        })?;
        // Memory-map the index file (Mmap implements Clone cheaply)
        let idx_mmap = unsafe { Mmap::map(&idx_file) }.map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to mmap index file {:?}: {}", idx_path, e),
            ))
        })?;
        let idx_mmap = SharedMmap::from(idx_mmap);
        // Build FST from the memory map
        let fst = Map::new(idx_mmap.clone()).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("fst map error: {}", e),
            ))
        })?;
        let lookup_path = idx_path.with_extension("lookup");
        let lookup = LookupTableStore::open(&lookup_path).map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to open lookup file {:?}: {}", lookup_path, e),
            ))
        })?;
        let payload = PayloadStore::<V, C>::open(&payload_path).map_err(|e| {
            IndexError::Io(io::Error::new(
                e.kind(),
                format!("failed to open payload file {:?}: {}", payload_path, e),
            ))
        })?;
        // Try loading a variant-C tags index if present
        let tags = match TagIndex::open(idx_path) {
            Ok(idx) => Some(idx),
            Err(IndexError::Io(_)) | Err(IndexError::InvalidFormat(_)) => None,
        };
        // Try loading a search index if present
        let search = match SearchIndex::open(idx_path) {
            Ok(idx) => Some(idx),
            Err(IndexError::Io(_)) | Err(IndexError::InvalidFormat(_)) => None,
        };
        Ok(Shard {
            idx_path: idx_path.to_path_buf(),
            idx_mmap,
            fst,
            lookup,
            payload,
            tags,
            search,
        })
    }

    /// Return the path to the index file for this shard.
    pub fn idx_path(&self) -> &Path {
        &self.idx_path
    }

    /// Return the longest common prefix of all indexed keys in this shard.
    ///
    /// Walk the underlying FST from the root, following the unique outgoing
    /// transition at each node until a branching point or leaf is reached.
    /// The bytes traversed form the longest common prefix.
    pub fn common_prefix(&self) -> String {
        let raw = self.fst.as_fst();
        let mut node = raw.root();
        let mut prefix_bytes = Vec::new();
        while node.len() == 1 {
            let tr = node.transition(0);
            prefix_bytes.push(tr.inp);
            node = raw.node(tr.addr);
        }
        String::from_utf8_lossy(&prefix_bytes)
            .trim_end_matches('\u{FFFD}')
            .to_string()
    }

    /// Number of keys in this shard.
    pub fn len(&self) -> usize {
        self.fst.len()
    }

    /// Does this shard contain any key starting with `prefix`?
    pub fn has_prefix(&self, prefix: &str) -> bool {
        let starts = Str::new(prefix).starts_with();
        self.fst.search(starts).into_stream().next().is_some()
    }

    /// Try to retrieve the value for `key` from this shard.
    pub fn get_value(&self, key: &str) -> Result<Option<V>> {
        if let Some(weight) = self.fst.get(key) {
            let lut_entry = self.lookup.get(weight as u32)?;
            let v = self.payload.get(lut_entry.payload_ptr).map_err(|e| {
                IndexError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("payload error: {}", e),
                ))
            })?;
            Ok(v)
        } else {
            Ok(None)
        }
    }

    /// Reverse lookup by raw pointer in this shard: retrieve the entry if present.
    pub fn get_entry(&self, ptr: u64) -> Result<Option<crate::Entry<V>>> {
        let raw_fst = RawFst::new(self.idx_mmap.clone()).map_err(IndexError::from)?;
        if let Some(key_bytes) = raw_fst.get_key(ptr) {
            let key = String::from_utf8(key_bytes.to_vec()).map_err(|e| {
                IndexError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid utf8 in key: {}", e),
                ))
            })?;
            let lut_entry = self.lookup.get(ptr as u32)?;
            let value = self.payload.get(lut_entry.payload_ptr)?;
            Ok(Some(crate::Entry::Key(key, ptr, value)))
        } else {
            Ok(None)
        }
    }


    /// Get access to the underlying FST for debugging purposes
    pub fn get_fst(&self) -> &Map<SharedMmap> {
        &self.fst
    }

    /// Get a reference to the tag index if it exists.
    pub fn tag_index(&self) -> Option<&TagIndex> {
        self.tags.as_ref()
    }
    
    /// Check if this shard has a search index.
    pub fn has_search_index(&self) -> bool {
        self.search.is_some()
    }
    
    /// Stream entries matching the given substring query via the shard's search index,
    /// optionally restricted to those whose key starts with `prefix`.
    pub fn stream_by_search<'a>(
        &'a self,
        query: &str,
        prefix: Option<&'a str>,
    ) -> Result<Box<dyn crate::Streamer<Item = crate::Entry<V>, Cursor = Vec<u8>> + 'a>> {
        if let Some(idx) = &self.search {
            let candidate_ids = idx.search(query)?;
            let normalized_query = normalize_text_unicode(query);
            let cluster_count = normalized_query.graphemes(true).count();
            if candidate_ids.is_empty() && !normalized_query.is_empty() && cluster_count < GRAM_SIZE {
                struct RawSearchStreamer<'a, V, C>
                where
                    V: DeserializeOwned,
                    C: PayloadCodec,
                {
                    shard: &'a Shard<V, C>,
                    prefix: Option<&'a str>,
                    query: String,
                    stream: fst::map::Stream<'a>,
                }

                impl<'a, V, C> crate::Streamer for RawSearchStreamer<'a, V, C>
                where
                    V: DeserializeOwned,
                    C: PayloadCodec,
                {
                    type Item = crate::Entry<V>;
                    type Cursor = Vec<u8>;

                    fn cursor(&self) -> Self::Cursor {
                        Vec::new()
                    }

                    fn seek(&mut self, _cursor: Self::Cursor) {}

                    fn next(&mut self) -> Option<Self::Item> {
                        while let Some((_key_bytes, doc_id)) = self.stream.next() {
                            if let Ok(Some(entry)) = self.shard.get_entry(doc_id) {
                                if let crate::Entry::Key(ref k, _, _) = entry {
                                    if let Some(pref) = self.prefix {
                                        if !k.starts_with(pref) {
                                            continue;
                                        }
                                    }
                                    let key_norm = strip_diacritics(&normalize_text_unicode(k));
                                    let query_norm = strip_diacritics(&normalize_text_unicode(&self.query));
                                    if key_norm.contains(&query_norm) {
                                        return Some(entry);
                                    }
                                }
                            }
                        }
                        None
                    }
                }

                return Ok(Box::new(RawSearchStreamer {
                    shard: self,
                    prefix,
                    query: query.to_string(),
                    stream: self.fst.stream(),
                }));
            }
            
            struct SearchStreamer<'a, V, C>
            where
                V: DeserializeOwned,
                C: PayloadCodec,
            {
                shard: &'a Shard<V, C>,
                candidate_ids: Vec<u64>,
                prefix: Option<&'a str>,
                query: String,
                idx: usize,
            }
            
            impl<'a, V, C> crate::Streamer for SearchStreamer<'a, V, C>
            where
                V: DeserializeOwned,
                C: PayloadCodec,
            {
                type Item = crate::Entry<V>;
                type Cursor = Vec<u8>;

                fn cursor(&self) -> Self::Cursor {
                    // Simple cursor: current index as bytes
                    (self.idx as u64).to_le_bytes().to_vec()
                }

                fn seek(&mut self, cursor: Self::Cursor) {
                    if cursor.len() >= 8 {
                        let idx_bytes: [u8; 8] = cursor[..8].try_into().unwrap_or([0; 8]);
                        self.idx = u64::from_le_bytes(idx_bytes) as usize;
                    }
                }

                fn next(&mut self) -> Option<Self::Item> {
                    while self.idx < self.candidate_ids.len() {
                        let id = self.candidate_ids[self.idx];
                        self.idx += 1;
                        
                        if let Ok(Some(entry)) = self.shard.get_entry(id) {
                            if let crate::Entry::Key(ref k, _, _) = entry {
                                // Apply prefix filter if specified
                                if let Some(pref) = self.prefix {
                                    if !k.starts_with(pref) {
                                        continue;
                                    }
                                }
                                let key_norm = strip_diacritics(&normalize_text_unicode(k));
                                let query_norm = strip_diacritics(&normalize_text_unicode(&self.query));
                                if key_norm.contains(&query_norm) {
                                    return Some(entry);
                                }
                            }
                        }
                    }
                    None
                }
            }
            
            Ok(Box::new(SearchStreamer {
                shard: self,
                candidate_ids,
                prefix,
                query: query.to_string(),
                idx: 0,
            }))
        } else {
            // No search index: return empty stream
            struct EmptyStreamer<V, C>(std::marker::PhantomData<(V, C)>);
            impl<V, C> crate::Streamer for EmptyStreamer<V, C>
            where
                V: DeserializeOwned,
                C: PayloadCodec,
            {
                type Item = crate::Entry<V>;
                type Cursor = Vec<u8>;

                fn cursor(&self) -> Self::Cursor {
                    Vec::new()
                }

                fn seek(&mut self, _cursor: Self::Cursor) {}

                fn next(&mut self) -> Option<Self::Item> {
                    None
                }
            }
            Ok(Box::new(EmptyStreamer::<V, C>(std::marker::PhantomData)))
        }
    }
    
    /// Stream entries by a list of document IDs.
    pub fn stream_by_ids<'a>(
        &'a self,
        doc_ids: Vec<u64>,
    ) -> Result<Box<dyn crate::Streamer<Item = crate::Entry<V>, Cursor = Vec<u8>> + 'a>> {
        struct IdStreamer<'a, V, C>
        where
            V: DeserializeOwned,
            C: PayloadCodec,
        {
            shard: &'a Shard<V, C>,
            doc_ids: Vec<u64>,
            idx: usize,
        }

        impl<'a, V, C> crate::Streamer for IdStreamer<'a, V, C>
        where
            V: DeserializeOwned,
            C: PayloadCodec,
        {
            type Item = crate::Entry<V>;
            type Cursor = Vec<u8>;

            fn cursor(&self) -> Self::Cursor {
                self.idx.to_le_bytes().to_vec()
            }

            fn seek(&mut self, cursor: Self::Cursor) {
                if cursor.len() >= 8 {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&cursor[..8]);
                    self.idx = usize::from_le_bytes(bytes);
                }
            }

            fn next(&mut self) -> Option<Self::Item> {
                while self.idx < self.doc_ids.len() {
                    let doc_id = self.doc_ids[self.idx];
                    self.idx += 1;
                    
                    if let Ok(Some(entry)) = self.shard.get_entry(doc_id) {
                        return Some(entry);
                    }
                }
                None
            }
        }
        
        Ok(Box::new(IdStreamer {
            shard: self,
            doc_ids,
            idx: 0,
        }))
    }
}
