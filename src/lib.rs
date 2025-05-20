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
use storage::{NodeStorage, generic_find_prefix, generic_children};

// Magic header for the new indexed format: 4 bytes, 'I','D','X','1'
const MAGIC: [u8; 4] = *b"IDX1";
// Named length constants for header parsing
const HEADER_LEN: usize = MAGIC.len();            // 4
const NODE_HEADER_LEN: usize = 1 + 2;             // is_end + child_count (u16)
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

    /// Collect all entries under `prefix` into a Vec, grouping at `delimiter`.
    fn list(&self, prefix: &str, delimiter: Option<char>) -> Vec<Entry>
    where Self: Sized
    {
        self.list_iter(prefix, delimiter).collect()
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
    /// Index structure for strings, backed by a radix trie.
///
/// Supports both in-memory construction and on-disk memory-mapped queries.
#[derive(Clone)]
pub enum Index {
    /// In-memory index built via `new()`, `insert()`, etc.
    InMemory { root: TrieNode },
    /// Read-only memory-mapped index loaded via `open()`, zero-copy listing.
    Mmap { buf: Arc<Mmap> },
}

// Unified NodeStorage implementation for both in-memory and memory-mapped Index
impl NodeStorage for Index {
    type Handle = Handle;
    fn root_handle(&self) -> Self::Handle {
        match self {
            Index::InMemory { root } => Handle::Mem(root as *const TrieNode),
            Index::Mmap { .. } => Handle::Mmap(HEADER_LEN),
        }
    }
    fn is_terminal(&self, handle: &Self::Handle) -> io::Result<bool> {
        match (self, handle) {
            (Index::InMemory { .. }, Handle::Mem(ptr)) => {
                let node = unsafe { &**ptr };
                Ok(node.is_end)
            }
            (Index::Mmap { buf }, Handle::Mmap(offset)) => {
                Ok(buf.get(*offset).copied().unwrap_or(0) != 0)
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid handle")),
        }
    }
    fn read_children(&self, handle: &Self::Handle) -> io::Result<Vec<(String, Self::Handle)>> {
        match (self, handle) {
            (Index::InMemory { .. }, Handle::Mem(ptr)) => {
                let node = unsafe { &**ptr };
                let mut out = Vec::with_capacity(node.children.len());
                for (label, child) in &node.children {
                    let child_ptr: *const TrieNode = child.as_ref();
                    out.push((label.clone(), Handle::Mem(child_ptr)));
                }
                out.sort_by(|a, b| b.0.cmp(&a.0));
                Ok(out)
            }
            (Index::Mmap { buf }, Handle::Mmap(mut pos)) => {
                // ensure we can read node header
                if pos + NODE_HEADER_LEN > buf.len() {
                    return Ok(Vec::new());
                }
                let count = u16::from_le_bytes([buf[pos + 1], buf[pos + 2]]) as usize;
                pos += NODE_HEADER_LEN;
                // skip TLV payload: tag + length + data
                if pos + 5 <= buf.len() {
                    let _tag = buf[pos]; pos += 1;
                    let len_bytes: [u8; 4] = buf[pos..pos + 4]
                        .try_into()
                        .unwrap_or([0, 0, 0, 0]);
                    let val_len = u32::from_le_bytes(len_bytes) as usize;
                    pos += 4;
                    pos = pos.saturating_add(val_len);
                }
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
                    out.push((label, Handle::Mmap(p + l)));
                }
                out.sort_by(|a, b| b.0.cmp(&a.0));
                Ok(out)
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid handle")),
        }
    }
}
// Implement the generic TrieBackend trait for the unified Index
impl TrieBackend for Index {
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
impl Index {
    /// Create a new, empty in-memory Trie.
    pub fn new() -> Self {
        Index::InMemory { root: TrieNode::default() }
    }
    
    /// Open a serialized indexed radix trie from disk via mmap.
    /// Expects the file to begin with the 4-byte MAGIC header "IDX1".
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        // SAFETY: file is not modified
        let mmap = unsafe { Mmap::map(&file)? };
        if mmap.len() < HEADER_LEN || &mmap[..HEADER_LEN] != &MAGIC {
            return Err(IndexError::InvalidFormat("missing or corrupt magic header"));
        }
        Ok(Index::Mmap { buf: Arc::new(mmap) })
    }
    
    /// Streaming iterator over entries under `prefix`, grouping at the first `delimiter`.
    pub fn list_iter<'a>(&'a self, prefix: &str, delimiter: Option<char>) -> impl Iterator<Item = Entry> + 'a {
        <Self as TrieBackend>::list_iter(self, prefix, delimiter)
    }
    /// Collect all entries under `prefix` into a Vec, grouping at `delimiter`.
    pub fn list(&self, prefix: &str, delimiter: Option<char>) -> Vec<Entry> {
        <Self as TrieBackend>::list(self, prefix, delimiter)
    }

    /// Insert a key with an optional CBOR payload into the radix trie, splitting edges on partial matches.
    pub fn insert(&mut self, key: &str, value: Option<Vec<u8>>) {
        // Prepare payload and insertion pointers
        let mut payload = value;
        match self {
            Index::InMemory { root } => {
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
            Index::Mmap { .. } => panic!("cannot insert into a memory-mapped trie"),
        }
    }
    /// Get the CBOR-decoded Value stored under `key`, if any.
    pub fn get_value(&self, key: &str) -> Result<Option<Value>> {
        match self {
            Index::InMemory { root } => {
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
            Index::Mmap { buf } => {
                match <Self as TrieBackend>::find_prefix(self, key) {
                    Some(GenericFindResult::Node(handle)) => {
                        let mut pos = if let Handle::Mmap(off) = handle { off } else { return Ok(None) };
                        pos += NODE_HEADER_LEN;
                        // Read TLV tag and length
                        if pos + 5 > buf.len() {
                            return Err(IndexError::InvalidFormat("truncated payload TLV"));
                        }
                        let tag = buf[pos]; pos += 1;
                        let len_bytes: [u8; 4] = buf[pos..pos+4]
                            .try_into()
                            .map_err(|_| IndexError::InvalidFormat("truncated payload length"))?;
                        let val_len = u32::from_le_bytes(len_bytes) as usize;
                        pos += 4;
                        if tag == 1 {
                            if pos + val_len > buf.len() {
                                return Err(IndexError::InvalidFormat("truncated payload data"));
                            }
                            let bytes = &buf[pos..pos+val_len];
                            let v: Value = serde_cbor::from_slice(bytes)
                                .map_err(|_| IndexError::InvalidFormat("invalid CBOR payload"))?;
                            Ok(Some(v))
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Ok(None),
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

    /// Write this trie into the binary index format ("IDX1"), using pre-order encoding:
    /// - 4-byte MAGIC header ("IDX1")
    /// - per-node header: is_end (1 byte), child_count (u16 LE)
    /// - per-node CBOR payload TLV
    /// - fixed-size index table (child_count × [first_byte (1 byte), child_offset (u64 LE)])
    /// - child blobs: label_len (u16 LE) + label bytes + subtree
    pub fn write_index<W: Write>(&self, w: &mut W) -> Result<()> {
        match self {
            Index::InMemory { root } => {
                // write magic header + node data into a cursor, then dump to w
                let mut buf = Cursor::new(Vec::new());
                buf.write_all(&MAGIC)?;
                Self::write_node(root, &mut buf)?;
                w.write_all(&buf.into_inner())?;
                Ok(())
            }
            Index::Mmap { .. } => panic!("cannot write a memory-mapped trie"),
        }
    }

    fn write_node<W: Write + Seek>(node: &TrieNode, w: &mut W) -> io::Result<()> {
        // header: is_end (1 byte) + child_count (2 bytes LE)
        w.write_all(&[node.is_end as u8])?;
        let count = node.children.len() as usize;
        w.write_all(&(count as u16).to_le_bytes())?;
        // payload TLV: tag (1 byte), length (4 bytes LE), [CBOR payload]
        if let Some(ref data) = node.value {
            // tag = 1 indicates CBOR payload
            w.write_all(&[1u8])?;
            let len = data.len() as u32;
            w.write_all(&len.to_le_bytes())?;
            w.write_all(data)?;
        } else {
            // tag = 0 indicates no value
            w.write_all(&[0u8])?;
            w.write_all(&0u32.to_le_bytes())?;
        }
        // reserve space for index table (count × (1 byte + 8 bytes))
        let index_pos = w.stream_position()?;
        for _ in 0..count {
            // first_byte placeholder + child_offset placeholder
            w.write_all(&[0u8])?;
            w.write_all(&0u64.to_le_bytes())?;
        }
        // sort children by first byte for binary-searchable index
        let mut children: Vec<_> = node.children.iter().collect();
        // sort by first byte (empty label => 0) for binary-searchable index
        children.sort_by_key(|(label, _)| label.as_bytes().first().cloned().unwrap_or(0));
        // write each child blob and record its offset
        let mut entries: Vec<(u8, u64)> = Vec::with_capacity(count);
        for (label, child) in children {
            // determine first_byte (empty label => 0)
            let first_byte = label.as_bytes().first().cloned().unwrap_or(0);
            let child_offset = w.stream_position()?;
            // write label
            let len = label.len() as u16;
            w.write_all(&len.to_le_bytes())?;
            w.write_all(label.as_bytes())?;
            // write subtree
            Self::write_node(&*child, w)?;
            entries.push((first_byte, child_offset));
        }
        // go back and fill in the index table
        let after_pos = w.stream_position()?;
        w.seek(SeekFrom::Start(index_pos))?;
        for (first_byte, offset) in entries {
            w.write_all(&[first_byte])?;
            w.write_all(&offset.to_le_bytes())?;
        }
        // rewind to after child blobs
        w.seek(SeekFrom::Start(after_pos))?;
        Ok(())
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