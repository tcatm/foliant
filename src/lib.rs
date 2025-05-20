use std::collections::HashSet;
use std::io::{self, Write, Seek, SeekFrom};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use memmap2::Mmap;
use std::convert::TryInto;
use std::fmt;
use serde::Serialize;
pub use serde_cbor::Value as Value;

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

/// Result type for index operations.
pub type Result<T> = std::result::Result<T, IndexError>;
mod storage;
mod payload_store;
use storage::{NodeStorage, generic_find_prefix, generic_children};
use payload_store::{PayloadStoreBuilder, PayloadStore};
use serde::de::DeserializeOwned;
use std::path::PathBuf;

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
    idx: Arc<Mmap>,
    payload: PayloadStore<V>,
}
// Manually implement Clone since derived bounds are too strict
impl<V> Clone for Database<V>
where
    V: DeserializeOwned,
{
    fn clone(&self) -> Self {
        Database { idx: self.idx.clone(), payload: self.payload.clone() }
    }
}

// Magic header for the new indexed format: 4 bytes, 'I','D','X','1'
// Followed by 8-byte payload section start offset (u64 LE)
const MAGIC: [u8; 4] = *b"IDX1";
// Named length constants for header parsing
const HEADER_LEN: usize = MAGIC.len() + 8;         // 4 + payload_offset (8)
// Node header: is_end (1) + child_count (2) + payload pointer (8)
const NODE_HEADER_LEN: usize = 1 + 2 + 8;
const INDEX_ENTRY_LEN: usize = 1 + 8;             // first_byte (u8) + child_offset (u64)
const LABEL_LEN_LEN: usize = 2;                   // u16 label length

/// A node in the trie.
#[derive(Default, Debug, Clone)]
struct TrieNode {
    /// Optional pointer (offset+1) into the payload file; 0 means no payload.
    payload_ptr: Option<u64>,
    /// Each edge is labeled by a (possibly multi-character) string
    children: Vec<(String, Box<TrieNode>)>,
    is_end: bool,
}

// --- Generic trie backend abstraction to unify both in-memory and mmap implementations ---
/// Result of finding a prefix in a trie backend.
enum GenericFindResult<H> {
    Node(H),
    EdgeMid(H, String),
}

/// Trait for a trie backend that can find prefixes, check terminal nodes, and list children.
/// Backend abstraction for generic trie traversal and grouping.
trait TrieBackend: Clone {
    type Handle: Clone;
    fn find_prefix(&self, prefix: &str) -> Option<GenericFindResult<Self::Handle>>;
    fn is_end(&self, handle: &Self::Handle) -> bool;
    fn children(&self, handle: &Self::Handle) -> Vec<(String, Self::Handle)>;

    /// Streaming iterator over entries under `prefix`, grouping at the first `delimiter`.
    fn list_iter(&self, prefix: &str, delimiter: Option<char>) -> GenericTrieIter<Self>
    where Self: Sized
    {
        let pref = prefix.to_string();
        match self.find_prefix(prefix) {
            Some(GenericFindResult::Node(h)) =>
                GenericTrieIter::new(self.clone(), pref.clone(), delimiter, h),
            Some(GenericFindResult::EdgeMid(h, tail)) => {
                let mut init = pref.clone(); init.push_str(&tail);
                GenericTrieIter::with_init(self.clone(), pref.clone(), delimiter,
                    vec![(h, init)])
            }
            None =>
                GenericTrieIter::empty(self.clone(), pref.clone(), delimiter),
        }
    }

}


/// A generic grouped-iterator over a trie backend.
#[derive(Clone)]
struct GenericTrieIter<B: TrieBackend> {
    backend: B,
    stack: Vec<(B::Handle, String)>,
    prefix: String,
    delimiter: Option<char>,
    seen: HashSet<String>,
}
impl<B: TrieBackend> GenericTrieIter<B> {
    fn new(backend: B, prefix: String, delimiter: Option<char>, handle: B::Handle) -> Self {
        GenericTrieIter {
            backend,
            stack: vec![(handle, prefix.clone())],
            prefix,
            delimiter,
            seen: HashSet::new(),
        }
    }
    fn with_init(
        backend: B,
        prefix: String,
        delimiter: Option<char>,
        init: Vec<(B::Handle, String)>,
    ) -> Self {
        GenericTrieIter {
            backend,
            stack: init,
            prefix,
            delimiter,
            seen: HashSet::new(),
        }
    }
    fn empty(backend: B, prefix: String, delimiter: Option<char>) -> Self {
        GenericTrieIter {
            backend,
            stack: Vec::new(),
            prefix,
            delimiter,
            seen: HashSet::new(),
        }
    }
}
impl<B: TrieBackend> Iterator for GenericTrieIter<B> {
    type Item = Entry;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some((h, path)) = self.stack.pop() {
            if let Some(d) = self.delimiter {
                if path.starts_with(&self.prefix) {
                    let suffix = &path[self.prefix.len()..];
                    if let Some(i) = suffix.find(d) {
                        let group = path[..self.prefix.len() + i + 1].to_string();
                        if self.seen.insert(group.clone()) {
                            return Some(Entry::CommonPrefix(group));
                        }
                        continue;
                    }
                    if !suffix.is_empty() && path.ends_with(d)
                        && self.seen.insert(path.clone())
                    {
                        return Some(Entry::CommonPrefix(path.clone()));
                    }
                }
            }
            let is_term = self.backend.is_end(&h);
            // Children are pre-sorted by NodeStorage implementations
            for (lbl, child) in self.backend.children(&h) {
                let mut np = path.clone();
                np.push_str(&lbl);
                self.stack.push((child, np));
            }
            if is_term && path.starts_with(&self.prefix) {
                if let Some(d) = self.delimiter {
                    if path.ends_with(d) {
                        return Some(Entry::CommonPrefix(path.clone()));
                    }
                }
                return Some(Entry::Key(path.clone()));
            }
        }
        None
    }
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

impl<B> Streamer for GenericTrieIter<B>
where
    B: TrieBackend,
{
    type Item = Entry;
    fn next(&mut self) -> Option<Self::Item> {
        Iterator::next(self)
    }
}

/// Builder for creating a new on-disk database. Use `new(path)` to begin,
/// call `insert(key, value)` as needed, then `close()` to write index and payload files.
/// Builder for creating a new on-disk database with values of type V.
/// Insert keys and optional V payloads, then call `close()` to write index and payload files.
/// Builder for creating a new on-disk database with values of type V.
/// Insert keys with `insert()`, then call `close()` or `into_database()`.
pub struct DatabaseBuilder<V = Value>
where
    V: Serialize,
{
    /// Base path (without extension) for .idx and .payload files
    base: PathBuf,
    root: TrieNode,
    idx_file: File,
    payload_store: PayloadStoreBuilder<V>,
}

impl<V: Serialize> DatabaseBuilder<V> {
    /// Create a new database builder writing to <base>.idx and <base>.payload (truncating existing).
    pub fn new<P: AsRef<Path>>(base: P) -> Result<Self> {
        let base = base.as_ref().to_path_buf();
        let idx_path = base.with_extension("idx");
        let payload_path = base.with_extension("payload");
        // Truncate or create files
        let idx_file = File::create(&idx_path)?;
        let payload_store = PayloadStoreBuilder::<V>::open(&payload_path)?;
        Ok(DatabaseBuilder {
            base,
            root: TrieNode::default(),
            idx_file,
            payload_store,
        })
    }


    /// Mutable lookup of a node by exact key.
    fn find_node_mut<'a>(mut node: &'a mut TrieNode, mut rem: &str) -> Option<&'a mut TrieNode> {
        if rem.is_empty() {
            return Some(node);
        }
        loop {
            let mut matched = false;
            // Iterate by index to avoid borrowing entire children vector
            for i in 0..node.children.len() {
                let label = &node.children[i].0;
                if rem.starts_with(label.as_str()) {
                    rem = &rem[label.len()..];
                    let child = &mut node.children[i].1;
                    node = child.as_mut();
                    matched = true;
                    break;
                }
            }
            if !matched {
                return None;
            }
            if rem.is_empty() {
                return Some(node);
            }
        }
    }


    /// Compute the byte-length of the common prefix of `a` and `b`.
    fn common_prefix_len(a: &str, b: &str) -> usize {
        let mut prefix_len = 0;
        let mut b_chars = b.chars();
        for (i, ac) in a.char_indices() {
            if let Some(bc) = b_chars.next() {
                if ac == bc {
                    prefix_len = i + ac.len_utf8();
                    continue;
                }
            }
            break;
        }
        prefix_len
    }

    /// Write a trie node, recording its payload in payload_buf and writing
    /// a pointer to that payload.
    fn write_node_with_pointers<W: Write + Seek>(
        node: &TrieNode,
        w: &mut W,
        payload_store: &mut crate::payload_store::PayloadStoreBuilder<V>,
    ) -> io::Result<()> {
        // header: is_end (1 byte) + child_count (2 bytes LE)
        w.write_all(&[node.is_end as u8])?;
        let count = node.children.len() as usize;
        w.write_all(&(count as u16).to_le_bytes())?;
        // payload pointer (u64 LE): offset into payload file (0 if none)
        let ptr = node.payload_ptr.unwrap_or(0);
        w.write_all(&ptr.to_le_bytes())?;
        // Reserve space for the index table (count entries × INDEX_ENTRY_LEN bytes)
        let index_pos = w.stream_position()?;
        let zero_table = vec![0u8; count * INDEX_ENTRY_LEN];
        w.write_all(&zero_table)?;
        // sort children by first byte for binary-searchable index
        let mut children: Vec<_> = node.children.iter().collect();
        children.sort_by_key(|(label, _)| label.as_bytes().first().cloned().unwrap_or(0));
        // write each child blob and record its offset
        let mut entries: Vec<(u8, u64)> = Vec::with_capacity(count);
        for (label, child) in children {
            let first_byte = label.as_bytes().first().cloned().unwrap_or(0);
            let child_offset = w.stream_position()?;
            // write label
            let len = label.len() as u16;
            w.write_all(&len.to_le_bytes())?;
            w.write_all(label.as_bytes())?;
            // write subtree recursively
            Self::write_node_with_pointers(child, w, payload_store)?;
            entries.push((first_byte, child_offset));
        }
        // Fill in the index table in one buffered write
        let after_pos = w.stream_position()?;
        w.seek(SeekFrom::Start(index_pos))?;
        let mut table_buf = Vec::with_capacity(entries.len() * INDEX_ENTRY_LEN);
        for (first_byte, offset) in &entries {
            table_buf.push(*first_byte);
            table_buf.extend_from_slice(&offset.to_le_bytes());
        }
        w.write_all(&table_buf)?;
        w.seek(SeekFrom::Start(after_pos))?;
        Ok(())
    }


    /// Insert a key with an optional value `V`, storing the payload and splitting edges on partial matches.
    /// The value is serialized into the payload store and referenced by the node.
    pub fn insert(&mut self, key: &str, value: Option<V>) {
        // Append payload via PayloadStoreBuilder; returns offset+1 (0 means no payload)
        let ptr = self.payload_store.append(value)
            .expect("payload append failed");
  
        // raw pointer to root for payload attachment
        let root_ptr: *mut TrieNode = &mut self.root as *mut TrieNode;
        let mut node: &mut TrieNode = &mut self.root;
        let mut suffix = key;
        loop {
            let mut matched = false;
            for i in 0..node.children.len() {
                let (ref label, _) = node.children[i];
                let lcp = Self::common_prefix_len(label, suffix);
                if lcp == 0 {
                    continue;
                }
                let (orig_label, orig_child) = node.children.remove(i);
                if lcp < orig_label.len() {
                    // Split edge into intermediate node
                    let mut intermediate = TrieNode::default();
                    let label_rem = &orig_label[lcp..];
                    intermediate.children.push((label_rem.to_string(), orig_child));
                    let key_rem = &suffix[lcp..];
                    if key_rem.is_empty() {
                        intermediate.is_end = true;
                    } else {
                        let mut leaf = TrieNode::default();
                        leaf.is_end = true;
                        intermediate.children.push((key_rem.to_string(), Box::new(leaf)));
                    }
                    let prefix_label = &orig_label[..lcp];
                    node.children.insert(i, (prefix_label.to_string(), Box::new(intermediate)));
                    // attach payload pointer to this node
                    unsafe {
                        if let Some(n) = Self::find_node_mut(&mut *root_ptr, key) {
                            n.payload_ptr = Some(ptr);
                        }
                    }
                    return;
                } else {
                    // Consume full edge label
                    suffix = &suffix[lcp..];
                    node.children.insert(i, (orig_label, orig_child));
                    if suffix.is_empty() {
                        node.children[i].1.is_end = true;
                        // attach payload pointer to this node
                        unsafe {
                            if let Some(n) = Self::find_node_mut(&mut *root_ptr, key) {
                                n.payload_ptr = Some(ptr);
                            }
                        }
                        return;
                    }
                    node = &mut node.children[i].1;
                    matched = true;
                    break;
                }
            }
            if !matched {
                // No matching edge; append new leaf
                let mut leaf = TrieNode::default();
                leaf.is_end = true;
                node.children.push((suffix.to_string(), Box::new(leaf)));
                // attach payload pointer to this node
                unsafe {
                    if let Some(n) = Self::find_node_mut(&mut *root_ptr, key) {
                        n.payload_ptr = Some(ptr);
                    }
                }
                return;
            }
        }
    }

    /// Finalize and write index and payload files to disk, batching writes via a buffer.
    pub fn close(mut self) -> Result<()> {
        use std::io::BufWriter;
        const CHUNK_SIZE: usize = 128 * 1024;
        // Wrap the index file in a buffered writer (128KiB) to batch writes
        let mut w = BufWriter::with_capacity(CHUNK_SIZE, self.idx_file);
        // Write magic header and placeholder payload offset
        w.write_all(&MAGIC)?;
        w.write_all(&0u64.to_le_bytes())?;
        // Write trie nodes and payload pointers via our generic writer
        Self::write_node_with_pointers(&self.root, &mut w, &mut self.payload_store)?;
        // Flush buffered writes to disk
        w.flush().map_err(IndexError::Io)?;
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

// NodeStorage implementation for on-disk, mmap-backed Database<V>
impl<V> NodeStorage for Database<V>
where
    V: DeserializeOwned,
{
    type Handle = Handle;
    fn root_handle(&self) -> Self::Handle {
        // root node begins immediately after the 4-byte magic + 8-byte header
        Handle::Mmap(HEADER_LEN)
    }

    fn is_terminal(&self, handle: &Self::Handle) -> io::Result<bool> {
        // Only mmap-backed handles are supported
        let Handle::Mmap(offset) = *handle;
        Ok(self.idx.get(offset).copied().unwrap_or(0) != 0)
    }

    fn read_children(&self, handle: &Self::Handle) -> io::Result<Vec<(String, Self::Handle)>> {
        // Only mmap-backed handles are supported
        let Handle::Mmap(mut pos) = *handle;
        let buf = &*self.idx;
        // ensure we can read node header
        if pos + NODE_HEADER_LEN > buf.len() {
            return Ok(Vec::new());
        }
        // read child count (u16 LE) at byte 1..3
        let count = u16::from_le_bytes([buf[pos + 1], buf[pos + 2]]) as usize;
        // skip header (is_end + child_count + payload ptr)
        pos += NODE_HEADER_LEN;
        // read index entries: each is [first_byte (1), child_offset (8)]
        let mut offsets = Vec::with_capacity(count);
        for i in 0..count {
            let ent = pos + i * INDEX_ENTRY_LEN;
            if ent + INDEX_ENTRY_LEN > buf.len() {
                break;
            }
            let off = u64::from_le_bytes(
                buf[ent + 1..ent + 9]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "truncated index entry"))?
            ) as usize;
            offsets.push(off);
        }
        // collect children: read label and sub-node offset
        let mut out = Vec::with_capacity(offsets.len());
        for off in offsets {
            let mut p = off;
            if p + LABEL_LEN_LEN > buf.len() {
                continue;
            }
            let l = u16::from_le_bytes([buf[p], buf[p + 1]]) as usize;
            p += LABEL_LEN_LEN;
            if p + l > buf.len() {
                continue;
            }
            let label = std::str::from_utf8(&buf[p..p + l])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                .to_string();
            // next byte offset marks the start of this child's subtree
            out.push((label, Handle::Mmap(p + l)));
        }
        out.sort_by(|a, b| b.0.cmp(&a.0));
        Ok(out)
    }
}

// Implement the generic TrieBackend trait for the mmap-backed Database<V>
impl<V> TrieBackend for Database<V>
where
    V: DeserializeOwned,
{
    type Handle = Handle;
    fn find_prefix(&self, prefix: &str) -> Option<GenericFindResult<Self::Handle>> {
        generic_find_prefix(self, prefix).ok().flatten()
    }
    fn is_end(&self, handle: &Self::Handle) -> bool {
        NodeStorage::is_terminal(self, handle).unwrap_or(false)
    }
    fn children(&self, handle: &Self::Handle) -> Vec<(String, Self::Handle)> {
        generic_children(self, handle).unwrap_or_default()
    }
}

// Inherent implementation of Index methods
impl<V> Database<V>
where
    V: DeserializeOwned,
{
    /// Open an indexed radix trie via mmap.
    /// If separate index (.idx) and payload (.payload) files are present under the given base path, they will be opened.
    /// Otherwise, a combined index file is expected at the given path, beginning with the 4-byte MAGIC header "IDX1",
    /// followed by an 8-byte payload section start offset (u64 LE).
    /// Open a read-only database via mmap. Expects separate index (.idx) and payload (.payload) files.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let base = path.as_ref();
        let idx_path = base.with_extension("idx");
        let payload_path = base.with_extension("payload");
        // Open and map index file
        let idx_file = File::open(&idx_path)?;
        let idx_mmap = unsafe { Mmap::map(&idx_file)? };
        // Check magic header in index file
        if idx_mmap.len() < MAGIC.len() || &idx_mmap[..MAGIC.len()] != &MAGIC {
            return Err(IndexError::InvalidFormat("missing or corrupt magic header"));
        }
        // Open payload store for reading
        let payload_store = PayloadStore::open(&payload_path)?;
        Ok(Database { idx: Arc::new(idx_mmap), payload: payload_store })
    }
    
    /// Streaming interface over entries under `prefix`, grouping at the first `delimiter`.
    pub fn list(&self, prefix: &str, delimiter: Option<char>) -> impl Streamer<Item = Entry> {
        <Self as TrieBackend>::list_iter(self, prefix, delimiter)
    }

    /// Get the deserialized payload of type V stored under `key`, if any.
    pub fn get_value(&self, key: &str) -> Result<Option<V>> {
        if let Some(GenericFindResult::Node(handle)) = <Self as TrieBackend>::find_prefix(self, key) {
            // Locate pointer in node header: is_end (1) + child_count (2)
            // payload pointer is at off + 1 + 2
            let Handle::Mmap(off) = handle;
            let ptr_pos = off + 1 + 2;
            if ptr_pos + 8 > self.idx.len() {
                return Err(IndexError::InvalidFormat("truncated payload pointer"));
            }
            let ptr_bytes: [u8; 8] = self.idx[ptr_pos..ptr_pos + 8]
                .try_into()
                .map_err(|_| IndexError::InvalidFormat("invalid payload pointer"))?;
            let ptr_val = u64::from_le_bytes(ptr_bytes);
            // Retrieve payload data via payload store
            let v_opt = self.payload.get(ptr_val)
                .map_err(IndexError::Io)?;
            Ok(v_opt)
        } else {
            Ok(None)
        }
    }
} // end impl Trie

/// A listing entry returned by `Database::list`: either a key or a grouped prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    /// A complete key (an inserted string).
    Key(String),
    /// A common prefix up through the delimiter.
    CommonPrefix(String),
}

impl Entry {
    pub fn as_str(&self) -> &str {
        match self {
            Entry::Key(s) | Entry::CommonPrefix(s) => s,
        }
    }
    pub fn kind(&self) -> &'static str {
        match self {
            Entry::Key(_) => "Key",
            Entry::CommonPrefix(_) => "CommonPrefix",
        }
    }
}

impl std::cmp::PartialOrd for Entry {
    fn partial_cmp(&self, other: &Entry) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for Entry {
    fn cmp(&self, other: &Entry) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}