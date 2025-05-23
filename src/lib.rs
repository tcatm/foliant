use std::io::{self};
use std::fs::{read_dir, File};
use std::path::Path;
use memmap2::Mmap;
use std::io::BufWriter;
use fst::MapBuilder;
use fst::IntoStreamer;
use fst::Streamer as FstStreamer;
use fst::Automaton;
use fst::automaton::Str;
use fst::map::{OpBuilder, Union};
use regex_automata::sparse::SparseDFA;
use std::fmt;
use serde::Serialize;
mod payload_store;
mod multi_list;
use payload_store::{PayloadStoreBuilder, PayloadStore};
use serde::de::DeserializeOwned;
use std::path::PathBuf;
use serde_cbor::Value as Value;
pub type Result<T> = std::result::Result<T, IndexError>;

// Create a buffered writer for the index file
const CHUNK_SIZE: usize = 128 * 1024;

/// Error type for the index library.
#[derive(Debug)]
pub enum IndexError {
    /// I/O error
    Io(io::Error),
    /// Data is not in the expected indexed format
    InvalidFormat(&'static str),
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexError::Io(e) => write!(f, "I/O error: {}", e),
            IndexError::InvalidFormat(msg) => write!(f, "Invalid index format: {}", msg),
        }
    }

}

impl std::error::Error for IndexError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            IndexError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for IndexError {
    fn from(err: io::Error) -> IndexError {
        IndexError::Io(err)
    }
}

impl From<fst::Error> for IndexError {
    fn from(err: fst::Error) -> IndexError {
        IndexError::Io(io::Error::new(io::ErrorKind::Other, format!("fst error: {}", err)))
    }
}
/// One shard of a database: a memory-mapped FST map and its payload store.
pub struct Shard<V>
where V: DeserializeOwned,
{
    fst: fst::Map<Mmap>,
    payload: PayloadStore<V>,
}

impl<V> Shard<V>
where V: DeserializeOwned,
{
    /// Open a shard from `<base>.idx` and `<base>.payload` files.
    pub fn open<P: AsRef<Path>>(base: P) -> Result<Self> {
        let base = base.as_ref();
        let idx_path = base.with_extension("idx");
        let payload_path = base.with_extension("payload");
        let idx_file = File::open(&idx_path)
            .map_err(|e| IndexError::Io(io::Error::new(e.kind(), format!("failed to open index file {:?}: {}", idx_path, e))))?;
        let idx_mmap = unsafe { Mmap::map(&idx_file) }
            .map_err(|e| IndexError::Io(io::Error::new(e.kind(), format!("failed to mmap index file {:?}: {}", idx_path, e))))?;
        let fst = fst::Map::new(idx_mmap)
            .map_err(|e| IndexError::Io(io::Error::new(io::ErrorKind::Other, format!("fst map error: {}", e))))?;
        let payload = PayloadStore::open(&payload_path)
            .map_err(|e| IndexError::Io(io::Error::new(e.kind(), format!("failed to open payload file {:?}: {}", payload_path, e))))?;
        Ok(Shard { fst, payload })
    }
}

/// Read-only database: union of one or more shards (FST maps + payload stores)
pub struct Database<V = Value>
where
    V: DeserializeOwned,
{
    shards: Vec<Shard<V>>,
}

/// Trait for streaming items, similar to `Iterator`.
pub trait Streamer {
    /// The type of item yielded by the streamer.
    type Item;
    /// Return the next item in the stream, or None if finished.
    fn next(&mut self) -> Option<Self::Item>;
    /// Consume the streamer and collect all remaining items into a Vec.
    fn collect(mut self) -> Vec<Self::Item>
    where Self: Sized,
    {
        let mut v = Vec::new();
        while let Some(item) = self.next() {
            v.push(item);
        }
        v
    }
}
// Blanket impl so that Box<dyn Streamer> itself implements Streamer
impl<S> Streamer for Box<S>
where
    S: Streamer + ?Sized,
{
    type Item = S::Item;
    fn next(&mut self) -> Option<Self::Item> {
        (**self).next()
    }
}

/// Builder for creating a new on-disk database with values of type V.
/// Insert keys with `insert()`, then call `close()` or `into_database()`.
pub struct DatabaseBuilder<V = Value>
where
    V: Serialize,
{
    /// Base path for .idx (fst) and .payload files
    base: PathBuf,
    /// FST index builder (writes to <base>.idx)
    fst_builder: MapBuilder<BufWriter<File>>,
    /// Payload store builder (writes to <base>.payload)
    payload_store: PayloadStoreBuilder<V>,
}

impl<V: Serialize> DatabaseBuilder<V> {
    /// Create a new database builder writing to <base>.idx and <base>.payload (truncating existing).
    pub fn new<P: AsRef<Path>>(base: P) -> Result<Self> {
        let base = base.as_ref().to_path_buf();
        let idx_path = base.with_extension("idx");
        let payload_path = base.with_extension("payload");

        let fst_file = File::create(&idx_path)
            .map_err(|e| IndexError::Io(io::Error::new(e.kind(), format!("failed to create index file: {}", e))))?;

        let fst_writer = BufWriter::with_capacity(CHUNK_SIZE, fst_file);
        let fst_builder = MapBuilder::new(fst_writer)
            .map_err(|e| IndexError::Io(io::Error::new(io::ErrorKind::Other, format!("failed to create index builder: {}", e))))?;

        // Create payload store for writing
        let payload_store = PayloadStoreBuilder::<V>::open(&payload_path)?;
        Ok(DatabaseBuilder {
            base,
            fst_builder,
            payload_store,
        })
    }

    /// Insert a key with an optional value `V` into the database.
    pub fn insert(&mut self, key: &str, value: Option<V>) {
        // Append payload via PayloadStoreBuilder; returns offset+1 (0 means no payload)
        let ptr = self.payload_store.append(value)
            .expect("payload append failed");
        // Insert key into the FST builder, with a pointer to the payload
        self.fst_builder.insert(key, ptr)
            .expect("FST insert failed");
    }

    /// Finalize and write index and payload files to disk, batching writes via a buffer.
    pub fn close(self) -> Result<()> {
        // Finalize the FST builder
        self.fst_builder
            .finish()
            .map_err(|e| IndexError::Io(io::Error::new(io::ErrorKind::Other, format!("failed to finalize index builder: {}", e))))?;

        // Flush payload store
        self.payload_store.close()?;
        Ok(())
    }

    /// Consume the builder, write files, and open a read-only Database<V> via mmap.
    pub fn into_database(self) -> Result<Database<V>>
    where
        V: DeserializeOwned,
    {
        let base = self.base.clone();
        self.close()?;
        Database::<V>::open(base)
    }

}

// The multi-shard Database implementation lives in the following `impl Database<V>`.
impl<V> Database<V>
where V: DeserializeOwned,
{
    /// Create an empty database (no shards).
    pub fn new() -> Self {
        Database { shards: Vec::new() }
    }
    /// Add one shard (opened from `<base>.idx` and `<base>.payload`).
    pub fn add_shard<P: AsRef<Path>>(&mut self, base: P) -> Result<()> {
        let shard = Shard::open(base)?;
        self.shards.push(shard);
        Ok(())
    }
    /// Open a database from either a single shard (file) or a directory of shards.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let base = path.as_ref();
        let mut db = Database::new();
        if base.is_dir() {
            for entry in read_dir(base)? {
                let ent = entry?;
                let p = ent.path();
                if p.extension().and_then(|s| s.to_str()) != Some("idx") {
                    continue;
                }
                let stem = p.file_stem()
                    .and_then(|s| s.to_str())
                    .ok_or(IndexError::InvalidFormat("invalid shard file name"))?;
                let b = base.join(stem);
                if !b.with_extension("payload").exists() {
                    continue;
                }

                if let Err(e) = db.add_shard(&b) {
                    eprintln!("warning: failed to add shard {:?}: {}", b, e);
                    continue;
                }
            }
        } else {
            db.add_shard(base)?;
        }
        Ok(db)
    }

    /// List entries under `prefix`, grouping by `delimiter` if provided.
    ///
    /// Returns a stream of `Entry<V>` (keys or common prefixes), or an error if
    /// listing cannot be constructed.
    /// List entries under `prefix`, grouping by `delimiter` if provided.
    /// Returns a boxed streamer yielding `Entry<V>` (keys or common prefixes).
    pub fn list<'a>(&'a self, prefix: &'a str, delimiter: Option<char>) -> Result<Box<dyn Streamer<Item = Entry<V>> + 'a>> {
        let delim_u8 = delimiter.map(|c| c as u8);
        let prefix_buf = prefix.as_bytes().to_vec();
        let ms = multi_list::MultiShardListStreamer::new(&self.shards, prefix_buf, delim_u8);
        Ok(Box::new(ms))
    }

    /// Search for keys matching a regular expression, optionally restricted to a prefix.
    ///
    /// `prefix`: if `Some(s)`, only keys starting with `s` are considered.
    /// `re`: a regex string; matches are anchored to the full key.
    ///
    /// Returns a stream of `Entry<V>` (keys matching the regex).
    pub fn grep<'a>(&'a self, prefix: Option<&'a str>, re: &str) -> Result<PrefixStream<'a, V>> {
        // Compile the regex into a sparse-DFA transducer.
        let dfa = SparseDFA::new(re)
            .map_err(|e| IndexError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("regex error: {}", e),
            )))?;
        // Split prefix for filtering; use empty string if None
        let prefix_str = prefix.unwrap_or("");
        let start = Str::new(prefix_str).starts_with();
        // Filter shards: only those that contain any key under the prefix
        let mut relevant_shards: Vec<&Shard<V>> = Vec::new();
        if prefix_str.is_empty() {
            for shard in &self.shards {
                relevant_shards.push(shard);
            }
        } else {
            for shard in &self.shards {
                let mut s = shard.fst.search(start.clone()).into_stream();
                if s.next().is_some() {
                    relevant_shards.push(shard);
                }
            }
        }
        // Build combined regex and prefix automaton
        let automaton = dfa.intersection(start);
        let mut op = OpBuilder::new();
        for shard in &relevant_shards {
            op = op.add(shard.fst.search(automaton.clone()));
        }
        let stream = op.union().into_stream();
        Ok(PrefixStream { stream, shards: relevant_shards })
    }

    /// Retrieve the payload for `key`, if any.
    pub fn get_value(&self, key: &str) -> Result<Option<V>> {
        for shard in &self.shards {
            if let Some(ptr) = shard.fst.get(key) {
                let v = shard.payload.get(ptr)
                    .map_err(|e| IndexError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        format!("payload error: {}", e),
                    )))?;
                return Ok(v);
            }
        }
        Ok(None)
    }

    /// Total number of keys across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.fst.len()).sum()
    }
}


/// Streamer for direct prefix listing when no delimiter grouping is needed
pub struct PrefixStream<'a, V>
where
    V: DeserializeOwned,
{
    stream: Union<'a>,
    shards: Vec<&'a Shard<V>>,
}

impl<V> Streamer for PrefixStream<'_, V>
where
    V: DeserializeOwned,
{
    type Item = Entry<V>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key_bytes, ivs)) = self.stream.next() {
            let key = String::from_utf8(key_bytes.to_vec())
                .expect("invalid utf8 in key");

            // Get the first shard and pointer from the iterator
            // TODO error on multiple IndexValues?
            let iv = ivs.into_iter().next()
                .expect("failed to get first shard in grep");
            let value = self.shards[iv.index].payload.get(iv.value)
                .expect("failed to get payload in grep");
            Some(Entry::Key(key, value))
        } else {
            None
        }
    }
}
/// Combined stream type for `list()`: direct prefix, DFS (single-shard), or multi-shard grouping

/// A listing entry returned by `Database::list`: either a key or a grouped prefix.
#[derive(Debug, Clone)]
pub enum Entry<V: DeserializeOwned = Value> {
    /// A complete key (an inserted string).
    Key(String, Option<V>),
    /// A common prefix up through the delimiter.
    CommonPrefix(String),
}

impl<V: DeserializeOwned> Entry<V> {
    pub fn as_str(&self) -> &str {
        match self {
            Entry::Key(s, _) | Entry::CommonPrefix(s) => s,
        }
    }
    pub fn kind(&self) -> &'static str {
        match self {
            Entry::Key(_, _) => "Key",
            Entry::CommonPrefix(_) => "CommonPrefix",
        }
    }
}

impl<V: DeserializeOwned> PartialEq for Entry<V> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<V: DeserializeOwned> Eq for Entry<V> {}

impl<V: DeserializeOwned> std::cmp::PartialOrd for Entry<V> {
    fn partial_cmp(&self, other: &Entry<V>) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<V: DeserializeOwned> std::cmp::Ord for Entry<V> {
    fn cmp(&self, other: &Entry<V>) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}