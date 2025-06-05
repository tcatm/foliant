use fst::Streamer;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;

use tantivy::collector::TopDocs;
use tantivy::merge_policy::LogMergePolicy;
use tantivy::query::QueryParser;
use tantivy::schema::{
    Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, FAST, STORED,
};
use tantivy::{Document, Index, IndexReader, IndexWriter, ReloadPolicy};

use crate::error::{IndexError, Result};
use crate::shard::Shard;
use serde_json::Value;
use std::sync::mpsc::{channel, Sender};
use std::thread;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::{Score, SegmentReader};

const SEARCH_DIR_EXT: &str = "search";
const TEXT_FIELD: &str = "key";
const ID_FIELD: &str = "id";

/// Read-only handle to a Tantivy search index per shard, indexing keys.
pub struct TantivyIndex {
    index: Index,
    reader: IndexReader,
    id_field: Field,
    text_field: Field,
}

impl TantivyIndex {
    /// Open a shard-local Tantivy index in the `<idx_path>.search/` directory.
    pub fn open<P: AsRef<Path>>(idx_path: P) -> Result<Self> {
        let dir = idx_path.as_ref().with_extension(SEARCH_DIR_EXT);
        let index = Index::open_in_dir(&dir).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to open search index {:?}: {}", dir, e),
            ))
        })?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()
            .map_err(|e| {
                IndexError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("failed to create search index reader: {}", e),
                ))
            })?;
        let schema = index.schema();
        let id_field = schema
            .get_field(ID_FIELD)
            .ok_or_else(|| IndexError::InvalidFormat("missing id field"))?;
        let text_field = schema
            .get_field(TEXT_FIELD)
            .ok_or_else(|| IndexError::InvalidFormat("missing key field"))?;
        Ok(TantivyIndex {
            index,
            reader,
            id_field,
            text_field,
        })
    }

    /// Execute a search for `query`, returning up to `limit` matching document IDs.
    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<u64>> {
        let searcher = self.reader.searcher();
        let parser = QueryParser::for_index(&self.index, vec![self.text_field]);
        let q = parser.parse_query(query).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("query parse error: {}", e),
            ))
        })?;
        let top_docs = searcher
            .search(&q, &TopDocs::with_limit(limit))
            .map_err(|e| {
                IndexError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("search error: {}", e),
                ))
            })?;
        let mut results = Vec::with_capacity(top_docs.len());
        for (_score, addr) in top_docs {
            let doc = searcher.doc(addr).map_err(|e| {
                IndexError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("failed to fetch doc: {}", e),
                ))
            })?;
            if let Some(val) = doc.get_first(self.id_field) {
                if let Some(id) = val.as_u64() {
                    results.push(id);
                }
            }
        }
        Ok(results)
    }

    /// Execute a streaming search for `query`, yielding up to `limit` matching document IDs as they are found.
    pub fn search_stream<'a>(
        &'a self,
        query: &str,
        limit: usize,
    ) -> Result<impl Iterator<Item = u64> + 'a> {
        let (tx, rx) = channel();
        let reader = self.reader.clone();
        let index = self.index.clone();
        let id_field = self.id_field;
        let text_field = self.text_field;
        let parser = QueryParser::for_index(&index, vec![text_field]);
        let query = parser.parse_query(query).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("query parse error: {}", e),
            ))
        })?;
        thread::spawn(move || {
            let searcher = reader.searcher();
            let collector = StreamingCollector {
                limit,
                sender: tx,
                id_field,
            };
            let _ = searcher.search(&query, &collector);
        });
        Ok(rx.into_iter())
    }
}

struct StreamingCollector {
    limit: usize,
    sender: Sender<u64>,
    id_field: Field,
}

struct StreamingSegmentCollector {
    limit: usize,
    count: usize,
    sender: Sender<u64>,
    segment_reader: SegmentReader,
    id_field: Field,
}

impl Collector for StreamingCollector {
    type Fruit = ();
    type Child = StreamingSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<StreamingSegmentCollector> {
        Ok(StreamingSegmentCollector {
            limit: self.limit,
            count: 0,
            sender: self.sender.clone(),
            segment_reader: segment_reader.clone(),
            id_field: self.id_field,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, _fruits: Vec<()>) -> tantivy::Result<()> {
        Ok(())
    }
}

impl SegmentCollector for StreamingSegmentCollector {
    type Fruit = ();

    fn collect(&mut self, doc: u32, _score: Score) {
        if self.count >= self.limit {
            return;
        }
        if let Ok(store_reader) = self.segment_reader.get_store_reader(0) {
            if let Ok(document) = store_reader.get(doc) {
                if let Some(val) = document.get_first(self.id_field) {
                    if let Some(id) = val.as_u64() {
                        let _ = self.sender.send(id);
                        self.count += 1;
                    }
                }
            }
        }
    }

    fn harvest(self) -> () {}
}

/// Builder for creating a Tantivy search index for shard keys.
pub struct TantivyIndexBuilder {
    writer: IndexWriter,
}

impl TantivyIndexBuilder {
    /// Create a new builder for writing `<idx_path>.search/`.
    pub fn new<P: AsRef<Path>>(idx_path: P) -> Result<Self> {
        let dir = idx_path.as_ref().with_extension(SEARCH_DIR_EXT);
        fs::create_dir_all(&dir).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to create search directory {:?}: {}", dir, e),
            ))
        })?;
        let mut schema_builder = Schema::builder();
        // Configure text indexing using the default tokenizer
        let text_indexing = TextFieldIndexing::default()
            .set_tokenizer("default")
            .set_index_option(IndexRecordOption::Basic);
        let text_options = TextOptions::default().set_indexing_options(text_indexing);
        let _text_field = schema_builder.add_text_field(TEXT_FIELD, text_options);
        let _id_field = schema_builder.add_u64_field(ID_FIELD, FAST | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_dir(&dir, schema.clone()).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to open/create search index {:?}: {}", dir, e),
            ))
        })?;
        let writer = index.writer(50_000_000).map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to create search index writer: {}", e),
            ))
        })?;

        let mut merge_policy = LogMergePolicy::default();
        merge_policy.set_min_num_segments(1);
        merge_policy.set_max_docs_before_merge(u32::MAX as usize);
        merge_policy.set_min_layer_size(0);
        writer.set_merge_policy(Box::new(merge_policy));

        Ok(TantivyIndexBuilder { writer })
    }

    /// Insert a key and its document ID into the search index.
    pub fn insert_key(&mut self, id: u64, key: &str) {
        let schema = self.writer.index().schema();
        let id_field = schema.get_field(ID_FIELD).unwrap();
        let text_field = schema.get_field(TEXT_FIELD).unwrap();
        let mut doc = Document::default();
        doc.add_u64(id_field, id);
        doc.add_text(text_field, key);
        let _ = self.writer.add_document(doc);
    }

    /// Commit and finalize the index files.
    pub fn finish(mut self) -> Result<()> {
        self.writer.commit().map_err(|e| {
            IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to commit search index: {}", e),
            ))
        })?;
        Ok(())
    }

    /// Build a search index for a single shard's keys, invoking an optional progress hook.
    fn build_shard(shard: &Shard<Value>, on_progress: Option<Arc<dyn Fn(u64)>>) -> Result<()> {
        let mut builder = TantivyIndexBuilder::new(shard.idx_path())?;
        if let Some(cb) = on_progress.clone() {
            cb(0);
        }
        let mut count = 0u64;
        let mut stream = shard.fst.stream();
        while let Some((bytes, weight)) = stream.next() {
            let id = weight as u64;
            let key = String::from_utf8_lossy(bytes).into_owned();
            builder.insert_key(id, &key);
            count += 1;
            if let Some(cb) = on_progress.clone() {
                cb(count);
            }
        }
        builder.finish()?;
        Ok(())
    }

    /// Build or rebuild the search index files for all shards and attach them to the database.
    pub fn build_index(
        db: &mut crate::Database<Value>,
        on_progress: Option<Arc<dyn Fn(u64)>>,
    ) -> Result<()> {
        for shard in db.shards_mut() {
            TantivyIndexBuilder::build_shard(shard, on_progress.clone())?;
            shard.search = Some(TantivyIndex::open(shard.idx_path())?);
        }
        Ok(())
    }
}
