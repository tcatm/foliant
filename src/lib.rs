use std::io::{self};
use std::fs::File;
use std::path::Path;
use fst::raw::Output;
use memmap2::Mmap;
use std::io::BufWriter;
use fst::MapBuilder;
use fst::IntoStreamer;
use fst::Streamer as FstStreamer;
use fst::Automaton;
use fst::map::Stream as MapStream;
use fst::automaton::{Str, StartsWith};
use std::fmt;
use serde::Serialize;
mod payload_store;
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

/// Opaque handle for navigating the on-disk trie
#[derive(Clone, Copy, Debug)]
pub enum Handle {
    /// Memory-mapped index offset
    Mmap(usize),
}

/// Read-only database: mmap-backed index and payload store
pub struct Database<V = Value>
where
    V: DeserializeOwned,
{
    fst_idx: fst::Map<Mmap>,
    payload: PayloadStore<V>,
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

impl<V> Database<V>
where
    V: DeserializeOwned,
{
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let base = path.as_ref();
        let idx_path = base.with_extension("idx");
        let payload_path = base.with_extension("payload");
        
        // Open the index file
        let idx_file = File::open(&idx_path)
            .map_err(|e| IndexError::Io(io::Error::new(e.kind(), format!("failed to open index file: {}", e))))?;
        let idx_mmap = unsafe { Mmap::map(&idx_file) }
            .map_err(|e| IndexError::Io(io::Error::new(e.kind(), format!("failed to mmap index file: {}", e))))?;
  
        // Create the payload store from the memory-mapped file 
        let payload_store = PayloadStore::<V>::open(payload_path)?;
        // Create the FST index from the memory-mapped file
        let fst_idx = fst::Map::new(idx_mmap)
            .map_err(|e| IndexError::Io(io::Error::new(io::ErrorKind::Other, format!("failed to create index map: {}", e))))?;
        // Return the database
        Ok(Database {
            fst_idx,
            payload: payload_store,
        })
    }
    
    pub fn list<'a>(&'a self, prefix: &'a str, delimiter: Option<char>) -> ListStream<'a, V> {
        // Fast path for no delimiter: use FST prefix automaton
        if delimiter.is_none() {
            let automaton = Str::new(prefix).starts_with();
            let stream = self.fst_idx.search(automaton).into_stream();
            return ListStream::Prefix(PrefixStreamer { stream, payload: &self.payload });
        }
        let raw_fst = self.fst_idx.as_fst();
        let delim = delimiter.map(|c| c as u8);
        // Navigate to the node corresponding to `prefix`, accumulate outputs
        let mut node = raw_fst.root();
        let mut output: Output = node.final_output();
        for &b in prefix.as_bytes() {
            if let Some(idx) = node.find_input(b) {
                let tr = node.transition(idx);
                output = output.cat(tr.out);
                node = raw_fst.node(tr.addr);
            } else {
                // Prefix not present: return empty DFS streamer
                return ListStream::DFS(ListStreamer {
                    fst: raw_fst,
                    payload: &self.payload,
                    delim,
                    prefix: Vec::new(),
                    stack: Vec::new(),
                });
            }
        }
        // Set up shared prefix buffer and initial frame
        let mut prefix_buf = Vec::with_capacity(prefix.len() + 8);
        prefix_buf.extend_from_slice(prefix.as_bytes());
        let stack = vec![Frame {
            node,
            output,
            transition_idx: 0,
            yielded_final: false,
            prefix_len: prefix_buf.len(),
        }];
        ListStream::DFS(ListStreamer {
            fst: raw_fst,
            payload: &self.payload,
            delim,
            prefix: prefix_buf,
            stack,
        })
    }

    /// Get the deserialized payload of type V stored under `key`, if any.
    pub fn get_value(&self, key: &str) -> Result<Option<V>> {
        // Lookup the key in the FST index
        let handle = self.fst_idx.get(key);
        // If found, retrieve the payload from the payload store
        if let Some(ptr) = handle {
            self.payload.get(ptr)
                .map_err(|e| IndexError::Io(io::Error::new(io::ErrorKind::Other, format!("failed to get payload: {}", e))))
        } else {
            Ok(None)
        }
    }
} // end impl Trie

/// Streamer for direct prefix listing when no delimiter grouping is needed
pub struct PrefixStreamer<'a, V>
where
    V: DeserializeOwned,
{
    stream: MapStream<'a, StartsWith<Str<'a>>>,
    payload: &'a PayloadStore<V>,
}

impl<V> Streamer for PrefixStreamer<'_, V>
where
    V: DeserializeOwned,
{
    type Item = Entry<V>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key_bytes, ptr)) = self.stream.next() {
            let key = String::from_utf8(key_bytes.to_vec())
                .expect("invalid utf8 in key");
            let value = self.payload.get(ptr)
                .expect("failed to get payload in list");
            Some(Entry::Key(key, value))
        } else {
            None
        }
    }
}
/// Combined stream type for `list()`, either direct prefix or DFS listing
pub enum ListStream<'a, V>
where
    V: DeserializeOwned,
{
    /// Fast path: prefix-only streaming
    Prefix(PrefixStreamer<'a, V>),
    /// DFS-based listing with optional grouping
    DFS(ListStreamer<'a, V>),
}
impl<V> Streamer for ListStream<'_, V>
where
    V: DeserializeOwned,
{
    type Item = Entry<V>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ListStream::Prefix(s) => s.next(),
            ListStream::DFS(s) => s.next(),
        }
    }
}
// Frame for tracking traversal state in ListStreamer
struct Frame<'a> {
    node: fst::raw::Node<'a>,
    /// accumulated output through this node
    output: Output,
    /// next transition index to visit
    transition_idx: usize,
    /// whether we've yielded the final key at this node
    yielded_final: bool,
    /// prefix buffer length when this frame was pushed
    prefix_len: usize,
}

/// Streamer for lazily listing entries under a prefix
pub struct ListStreamer<'a, V>
where
    V: DeserializeOwned,
{
    fst: &'a fst::raw::Fst<Mmap>,
    payload: &'a PayloadStore<V>,
    delim: Option<u8>,
    /// shared buffer of current key prefix bytes
    prefix: Vec<u8>,
    /// DFS stack of frames
    stack: Vec<Frame<'a>>,
}

impl<V> Streamer for ListStreamer<'_, V>
where
    V: DeserializeOwned,
{
    type Item = Entry<V>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(frame) = self.stack.last_mut() {
            // First, yield the complete key at this node if not yet done
            if !frame.yielded_final {
                frame.yielded_final = true;
                if frame.node.is_final() {
                    let ptr = frame.output.value();
                    let value = self.payload.get(ptr)
                        .expect("failed to get payload in list");
                    // current prefix buffer holds this key
                    let key = String::from_utf8(self.prefix.clone())
                        .expect("invalid utf8 in key");
                    return Some(Entry::Key(key, value));
                }
            }
            // Next, explore transitions
            if frame.transition_idx < frame.node.len() {
                // take next transition
                let tr = frame.node.transition(frame.transition_idx);
                frame.transition_idx += 1;
                let b = tr.inp;
                let out = frame.output.cat(tr.out);
                if Some(b) == self.delim {
                    // group at delimiter: yield prefix+delim
                    let mut buf = self.prefix.clone();
                    buf.push(b);
                    let s = String::from_utf8(buf)
                        .expect("invalid utf8 in prefix");
                    return Some(Entry::CommonPrefix(s));
                } else {
                    // descend into child
                    self.prefix.push(b);
                    let new_node = self.fst.node(tr.addr);
                    // if only one child, flatten in-place (reuse frame)
                    if frame.node.len() == 1 {
                        frame.node = new_node;
                        frame.output = out;
                        frame.transition_idx = 0;
                        frame.yielded_final = false;
                        frame.prefix_len = self.prefix.len();
                        continue;
                    }
                    // else, push new frame for branching
                    let prefix_len = self.prefix.len();
                    self.stack.push(Frame {
                        node: new_node,
                        output: out,
                        transition_idx: 0,
                        yielded_final: false,
                        prefix_len,
                    });
                    continue;
                }
            }
            // Done with this node: backtrack to parent prefix length
            self.stack.pop();
            let new_len = self.stack.last().map(|f| f.prefix_len).unwrap_or(0);
            self.prefix.truncate(new_len);
        }
        None
    }
}

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