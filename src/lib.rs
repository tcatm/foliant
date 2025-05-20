#![allow(private_interfaces)]
use std::collections::HashSet;
use std::io::{self, Write, Seek, SeekFrom, Cursor};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use memmap2::Mmap;
use std::convert::TryInto;
use std::fmt;
use serde_cbor;
pub use serde_cbor::Value as Value;

/// Error type for the index library.
#[derive(Debug)]
pub enum IndexError {
    /// I/O error
    Io(io::Error),
    /// Data is not in the expected indexed format
    InvalidFormat(&'static str),
}
// Internal handle for unified NodeStorage across in-memory and mmap variants
#[derive(Clone)]
enum Handle {
    Mem(*const TrieNode),
    Mmap(usize),
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
    /// Optional CBOR value (raw bytes) stored at this node if it is a key.
    value: Option<Vec<u8>>,
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

    /// Index structure for strings, backed by a radix trie.
///
/// Supports both in-memory construction and on-disk memory-mapped queries.
#[derive(Clone)]
/// Database structure for strings, backed by a radix trie.
/// Supports both in-memory construction and on-disk memory-mapped queries.
pub enum Database {
    /// In-memory database built via `new()` and `insert()`.
    InMemory { root: TrieNode },
    /// Read-only database loaded via `open()`, mmapping .idx and reading .payload via PayloadStore.
    Mmap { idx: Arc<Mmap>, payload: PayloadStore },
}

/// Builder for creating a new on-disk database. Use `new(path)` to begin,
/// call `insert(key, value)` as needed, then `close()` to write index and payload files.
/// Builder for creating a new on-disk database. Use `new(path)` to begin,
/// call `insert(key, value)` as needed, then `close()` to write index and payload files.
pub struct DatabaseBuilder {
    root: TrieNode,
    idx_file: File,
    payload_store: PayloadStoreBuilder,
}

impl DatabaseBuilder {
    /// Create a new database builder writing to <base>.idx and <base>.payload (truncating existing).
    pub fn new<P: AsRef<Path>>(base: P) -> Result<Self> {
        let base = base.as_ref();
        let idx_path = base.with_extension("idx");
        let payload_path = base.with_extension("payload");
        // Truncate or create files
        let idx_file = File::create(&idx_path)?;
        let payload_store = PayloadStoreBuilder::open(&payload_path)?;
        Ok(DatabaseBuilder {
            root: TrieNode::default(),
            idx_file,
            payload_store,
        })
    }

    /// Insert a key with optional CBOR payload into the trie.
    pub fn insert(&mut self, key: &str, value: Option<Vec<u8>>) {
        // In-memory insert logic on root
        let mut builder = Database::InMemory { root: self.root.clone() };
        builder.insert(key, value);
        // Extract updated trie
        if let Database::InMemory { root } = builder {
            self.root = root;
        }
    }

    /// Finalize and write index and payload files to disk.
    pub fn close(mut self) -> Result<()> {
        // Write magic and placeholder payload offset
        self.idx_file.write_all(&MAGIC)?;
        self.idx_file.write_all(&0u64.to_le_bytes())?;
        // Write trie nodes and payload pointers
        Database::write_node_with_pointers(&self.root, &mut self.idx_file, &mut self.payload_store)?;
        // Ensure payload file is flushed
        self.payload_store.close()?;
        Ok(())
    }
}

// Unified NodeStorage implementation for both in-memory and memory-mapped Database
impl NodeStorage for Database {
    type Handle = Handle;
    fn root_handle(&self) -> Self::Handle {
        match self {
            Database::InMemory { root } => Handle::Mem(root as *const TrieNode),
            Database::Mmap { .. } => Handle::Mmap(HEADER_LEN),
        }
    }
    fn is_terminal(&self, handle: &Self::Handle) -> io::Result<bool> {
        match (self, handle) {
            (Database::InMemory { .. }, Handle::Mem(ptr)) => {
                let node = unsafe { &**ptr };
                Ok(node.is_end)
            }
            (Database::Mmap { idx, .. }, Handle::Mmap(offset)) => {
                Ok(idx.get(*offset).copied().unwrap_or(0) != 0)
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid handle")),
        }
    }
    fn read_children(&self, handle: &Self::Handle) -> io::Result<Vec<(String, Self::Handle)>> {
        match (self, handle) {
            (Database::InMemory { .. }, Handle::Mem(ptr)) => {
                let node = unsafe { &**ptr };
                let mut out = Vec::with_capacity(node.children.len());
                for (label, child) in &node.children {
                    let child_ptr: *const TrieNode = child.as_ref();
                    out.push((label.clone(), Handle::Mem(child_ptr)));
                }
                out.sort_by(|a, b| b.0.cmp(&a.0));
                Ok(out)
            }
            (Database::Mmap { idx, .. }, Handle::Mmap(mut pos)) => {
                // ensure we can read node header
                if pos + NODE_HEADER_LEN > idx.len() {
                    return Ok(Vec::new());
                }
                // read child count
                let count = u16::from_le_bytes([idx[pos + 1], idx[pos + 2]]) as usize;
                // skip header (is_end + child_count + payload ptr)
                pos += NODE_HEADER_LEN;
                // read index entries: each is [first_byte (1), child_offset (8)]
                let mut offsets = Vec::with_capacity(count);
                for i in 0..count {
                    let ent = pos + i * INDEX_ENTRY_LEN;
                    if ent + INDEX_ENTRY_LEN > idx.len() {
                        break;
                    }
                    let off = u64::from_le_bytes(
                        idx[ent + 1..ent + 9]
                            .try_into()
                            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "truncated index entry"))?
                    ) as usize;
                    offsets.push(off);
                }
                let mut out = Vec::with_capacity(offsets.len());
                for off in offsets {
                    let mut p = off;
                    if p + LABEL_LEN_LEN > idx.len() {
                        continue;
                    }
                    let l = u16::from_le_bytes([idx[p], idx[p + 1]]) as usize;
                    p += LABEL_LEN_LEN;
                    if p + l > idx.len() {
                        continue;
                    }
                    let label = std::str::from_utf8(&idx[p..p + l])
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                        .to_string();
                    out.push((label, Handle::Mmap(p + l)));
                }
                out.sort_by(|a, b| b.0.cmp(&a.0));
                Ok(out)
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid handle")),
        }
    }
}
// Implement the generic TrieBackend trait for the unified Database
impl TrieBackend for Database {
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
impl Database {
    /// Create a new, empty in-memory Trie.
    pub fn new() -> Self {
        Database::InMemory { root: TrieNode::default() }
    }
    
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
        Ok(Database::Mmap { idx: Arc::new(idx_mmap), payload: payload_store })
    }
    
    /// Streaming interface over entries under `prefix`, grouping at the first `delimiter`.
    pub fn list(&self, prefix: &str, delimiter: Option<char>) -> impl Streamer<Item = Entry> {
        <Self as TrieBackend>::list_iter(self, prefix, delimiter)
    }

    /// Insert a key with an optional CBOR payload into the radix trie, splitting edges on partial matches.
    pub fn insert(&mut self, key: &str, value: Option<Vec<u8>>) {
        // Prepare payload and insertion pointers
        let mut payload = value;
        match self {
            Database::InMemory { root } => {
                // raw pointer to root for payload attachment
                let root_ptr: *mut TrieNode = root;
                let mut node = root;
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
                            // attach payload if provided
                            if let Some(val) = payload.take() {
                                unsafe {
                                    if let Some(n) = Self::find_node_mut(&mut *root_ptr, key) {
                                        n.value = Some(val);
                                    }
                                }
                            }
                            return;
                        } else {
                            // Consume full edge label
                            suffix = &suffix[lcp..];
                            node.children.insert(i, (orig_label, orig_child));
                            if suffix.is_empty() {
                                node.children[i].1.is_end = true;
                                // attach payload if provided
                                if let Some(val) = payload.take() {
                                    unsafe {
                                        if let Some(n) = Self::find_node_mut(&mut *root_ptr, key) {
                                            n.value = Some(val);
                                        }
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
                        // attach payload if provided
                        if let Some(val) = payload.take() {
                            unsafe {
                                if let Some(n) = Self::find_node_mut(&mut *root_ptr, key) {
                                    n.value = Some(val);
                                }
                            }
                        }
                        return;
                    }
                }
            }
            Database::Mmap { .. } => panic!("cannot insert into a memory-mapped database"),
        }
    }
    /// Get the CBOR-decoded Value stored under `key`, if any.
    pub fn get_value(&self, key: &str) -> Result<Option<Value>> {
        match self {
            Database::InMemory { root } => {
                if let Some(node) = Self::find_node(root, key) {
                    if let Some(bytes) = node.value.as_deref() {
                        let v: Value = serde_cbor::from_slice(bytes)
                            .map_err(|_| IndexError::InvalidFormat("invalid CBOR payload"))?;
                        Ok(Some(v))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            Database::Mmap { idx, payload } => {
                if let Some(GenericFindResult::Node(handle)) = <Self as TrieBackend>::find_prefix(self, key) {
                    // Locate pointer in node header: is_end (1) + child_count (2)
                    let off = if let Handle::Mmap(o) = handle { o } else { return Ok(None) };
                    let ptr_pos = off + 1 + 2;
                    if ptr_pos + 8 > idx.len() {
                        return Err(IndexError::InvalidFormat("truncated payload pointer"));
                    }
                    let ptr_bytes: [u8; 8] = idx[ptr_pos..ptr_pos + 8]
                        .try_into()
                        .map_err(|_| IndexError::InvalidFormat("invalid payload pointer"))?;
                    let ptr_val = u64::from_le_bytes(ptr_bytes);
                    // Retrieve payload data via payload store
                    if let Some(bytes) = payload.clone().get(ptr_val)? {
                        let v: Value = serde_cbor::from_slice(&bytes)
                            .map_err(|_| IndexError::InvalidFormat("invalid CBOR payload"))?;
                        Ok(Some(v))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }
    /// Immutable lookup of a node by exact key.
    fn find_node<'a>(mut node: &'a TrieNode, mut rem: &str) -> Option<&'a TrieNode> {
        if rem.is_empty() {
            return Some(node);
        }
        while !rem.is_empty() {
            let mut matched = false;
            for (label, child) in &node.children {
                if rem.starts_with(label.as_str()) {
                    rem = &rem[label.len()..];
                    node = child.as_ref();
                    matched = true;
                    break;
                }
            }
            if !matched {
                return None;
            }
        }
        Some(node)
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
        payload_store: &mut crate::payload_store::PayloadStoreBuilder,
    ) -> io::Result<()> {
        // header: is_end (1 byte) + child_count (2 bytes LE)
        w.write_all(&[node.is_end as u8])?;
        let count = node.children.len() as usize;
        w.write_all(&(count as u16).to_le_bytes())?;
        // payload pointer (u64 LE): offset into payload_buf, or 0 if none
        // Store payload in payload_buf; encode pointer as offset+1, 0 means no payload
        // Append payload and get pointer id (u64)
        let ptr = if let Some(ref data) = node.value {
            // append returns (offset+1)
            payload_store.append(data)?
        } else {
            0u64
        };
        w.write_all(&ptr.to_le_bytes())?;
        // reserve space for index table (count × (1 byte + 8 bytes))
        let index_pos = w.stream_position()?;
        for _ in 0..count {
            w.write_all(&[0u8])?;
            w.write_all(&0u64.to_le_bytes())?;
        }
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
        // go back and fill in the index table
        let after_pos = w.stream_position()?;
        w.seek(SeekFrom::Start(index_pos))?;
        for (first_byte, offset) in entries {
            w.write_all(&[first_byte])?;
            w.write_all(&offset.to_le_bytes())?;
        }
        w.seek(SeekFrom::Start(after_pos))?;
        Ok(())
    }
    /// Save an in-memory database, writing separate index (.idx) and payload (.payload) files.
    /// For a read-only memory-mapped database, returns an error.
    pub fn save<P: AsRef<Path>>(&self, base: P) -> Result<()> {
        match self {
            Database::InMemory { root } => {
                // Prepare paths
                let idx_path = base.as_ref().with_extension("idx");
                let payload_path = base.as_ref().with_extension("payload");
                // Open index file for writing
                let mut idx_file = File::create(&idx_path)?;
                // Write header: MAGIC + placeholder payload offset (unused for separate files)
                idx_file.write_all(&MAGIC)?;
                idx_file.write_all(&0u64.to_le_bytes())?;
                // Open payload store for writing payload entries
                let mut ps = PayloadStoreBuilder::open(&payload_path)?;
                // Write trie nodes to index file, recording payload pointers to payload store
                Self::write_node_with_pointers(root, &mut idx_file, &mut ps)?;
                // Flush payload store to disk
                ps.close()?;
                Ok(())
            }
            Database::Mmap { .. } => Err(IndexError::InvalidFormat(
                "cannot save a read-only database",
            )),
        }
    }


} // end impl Trie

/// A listing entry returned by `Index::list`: either a full key or a grouped prefix.
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