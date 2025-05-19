use std::collections::HashSet;
use std::io::{self, Read, Write, Seek, SeekFrom, Cursor};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use memmap2::Mmap;
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
impl<B: TrieBackend> PartialEq<Vec<Entry>> for GenericTrieIter<B> {
    fn eq(&self, other: &Vec<Entry>) -> bool {
        let got = self.clone().collect::<Vec<_>>();
        let want = other.clone();
        got == want
    }
}

/// A prefix trie for indexing strings.
#[derive(Clone)]
pub struct Trie {
    root: TrieNode,
}

/// A lazily-loaded radix trie over a memory-mapped index file.
#[derive(Clone)]
pub struct MmapTrie {
    buf: Arc<Mmap>,
}

impl MmapTrie {
    /// Load a serialized indexed radix trie from disk via mmap.
    /// Expects the file to begin with the 4-byte MAGIC header "IDX1".
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        // SAFETY: we do not modify the file
        let mmap = unsafe { Mmap::map(&file)? };
        // verify magic header
        if mmap.len() < HEADER_LEN || &mmap[..HEADER_LEN] != &MAGIC {
            return Err(IndexError::InvalidFormat("missing or corrupt magic header"));
        }
        let buf = Arc::new(mmap);
        Ok(MmapTrie { buf })
    }

    /// Streaming iterator over entries under `prefix`, grouping at the first `delimiter`.
    pub fn list_iter<'a>(&'a self, prefix: &str, delimiter: Option<char>) -> impl Iterator<Item = Entry> + 'a {
        <Self as TrieBackend>::list_iter(self, prefix, delimiter)
    }
    /// Collect all entries under `prefix` into a Vec, grouping at `delimiter`.
    pub fn list(&self, prefix: &str, delimiter: Option<char>) -> Vec<Entry> {
        <Self as TrieBackend>::list(self, prefix, delimiter)
    }
    /// Get the raw CBOR value bytes for an exact `key`, if present.
    /// Get the raw CBOR-encoded bytes stored under `key`, if any.
    fn get_value_raw(&self, key: &str) -> Result<Option<&[u8]>> {
        // Find the node handle for this key
        match <Self as TrieBackend>::find_prefix(self, key) {
            Some(GenericFindResult::Node(handle)) => {
                let mut pos = handle + NODE_HEADER_LEN;
                // Read TLV tag and length
                if pos + 5 > self.buf.len() {
                    return Err(IndexError::InvalidFormat("truncated payload TLV"));
                }
                let tag = self.buf[pos]; pos += 1;
                let len_bytes: [u8; 4] = self.buf[pos..pos+4]
                    .try_into()
                    .map_err(|_| IndexError::InvalidFormat("truncated payload length"))?;
                let val_len = u32::from_le_bytes(len_bytes) as usize;
                pos += 4;
                if tag == 1 {
                    if pos + val_len > self.buf.len() {
                        return Err(IndexError::InvalidFormat("truncated payload data"));
                    }
                    Ok(Some(&self.buf[pos..pos+val_len]))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
    /// Get the CBOR-decoded Value stored under `key`, if any.
    pub fn get_value(&self, key: &str) -> Result<Option<Value>> {
        if let Some(bytes) = self.get_value_raw(key)? {
            let v: Value = serde_cbor::from_slice(bytes)
                .map_err(|_| IndexError::InvalidFormat("invalid CBOR payload"))?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }
}

// NodeStorage implementations for Trie and MmapTrie
impl NodeStorage for Trie {
    /// Handle is a raw pointer to a TrieNode; heap-allocated Boxes ensure stability
    type Handle = *const TrieNode;
    fn root_handle(&self) -> Self::Handle {
        &self.root as *const TrieNode
    }
    fn is_terminal(&self, handle: &Self::Handle) -> io::Result<bool> {
        // SAFETY: handle is a valid pointer from root_handle or read_children
        let node = unsafe { &**handle };
        Ok(node.is_end)
    }
    fn read_children(&self, handle: &Self::Handle) -> io::Result<Vec<(String, Self::Handle)>> {
        // SAFETY: handle is a valid pointer to TrieNode
        let node = unsafe { &**handle };
        let mut out = Vec::with_capacity(node.children.len());
        for (label, child) in &node.children {
            let child_ptr: *const TrieNode = child.as_ref();
            out.push((label.clone(), child_ptr));
        }
        // Ensure children are sorted descending by label for correct traversal order
        out.sort_by(|a, b| b.0.cmp(&a.0));
        Ok(out)
    }
}

impl NodeStorage for MmapTrie {
    type Handle = usize;
    fn root_handle(&self) -> Self::Handle {
        // skip magic header
        HEADER_LEN
    }
    fn is_terminal(&self, handle: &Self::Handle) -> io::Result<bool> {
        Ok(self.buf.get(*handle).copied().unwrap_or(0) != 0)
    }
    fn read_children(&self, handle: &Self::Handle) -> io::Result<Vec<(String, Self::Handle)>> {
        let mut pos = *handle;
        // ensure we can read node header
        if pos + NODE_HEADER_LEN > self.buf.len() {
            return Ok(Vec::new());
        }
        // parse node header
        let count = u16::from_le_bytes([self.buf[pos + 1], self.buf[pos + 2]]) as usize;
        pos += NODE_HEADER_LEN;
        // skip TLV payload: tag + length + data
        if pos + 5 <= self.buf.len() {
            let _tag = self.buf[pos];
            pos += 1;
            // read length (4 bytes LE)
            let len_bytes: [u8; 4] = self.buf[pos..pos + 4]
                .try_into()
                .unwrap_or([0,0,0,0]);
            let val_len = u32::from_le_bytes(len_bytes) as usize;
            pos += 4;
            // skip the payload data itself
            pos = pos.saturating_add(val_len);
        }
        let mut offsets = Vec::with_capacity(count);
        for i in 0..count {
            let ent = pos + i * INDEX_ENTRY_LEN;
            if ent + INDEX_ENTRY_LEN > self.buf.len() {
                break;
            }
            let off = u64::from_le_bytes(
                self.buf[ent + 1..ent + 9]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "truncated index entry"))?
            ) as usize;
            offsets.push(off);
        }
        let mut out = Vec::with_capacity(offsets.len());
        for off in offsets {
            let mut p = off;
            if p + LABEL_LEN_LEN > self.buf.len() {
                continue;
            }
            let l = u16::from_le_bytes([self.buf[p], self.buf[p + 1]]) as usize;
            p += LABEL_LEN_LEN;
            if p + l > self.buf.len() {
                continue;
            }
            let label = std::str::from_utf8(&self.buf[p..p + l])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                .to_string();
            let child_handle = p + l;
            out.push((label, child_handle));
        }
        // Ensure children are sorted descending by label for traversal
        out.sort_by(|a, b| b.0.cmp(&a.0));
        Ok(out)
    }
}

// Implement the generic TrieBackend trait for the in-memory Trie
impl TrieBackend for Trie {
    type Handle = *const TrieNode;
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

// Implement the generic TrieBackend trait for the memory-mapped Trie
impl TrieBackend for MmapTrie {
    type Handle = usize;
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

// buffer-based prefix lookup logic removed; use generic_find_prefix instead


/// Statistics collected during trie traversal.
#[derive(Default, Debug, Clone)]
pub struct TraverseStats {
    /// Number of trie nodes visited during traversal.
    pub nodes_visited: usize,
    /// Number of edges (child links) traversed.
    pub edges_traversed: usize,
    /// Number of keys (terminal nodes) collected.
    pub keys_collected: usize,
}
// Inherent implementation of Trie methods (without find_node_extra)
impl Trie {


    /// Create a new, empty Trie.
    pub fn new() -> Self {
        Trie { root: TrieNode::default() }
    }
    
    /// Streaming iterator over entries under `prefix`, grouping at the first `delimiter`.
    pub fn list_iter<'a>(&'a self, prefix: &str, delimiter: Option<char>) -> impl Iterator<Item = Entry> + 'a {
        <Self as TrieBackend>::list_iter(self, prefix, delimiter)
    }
    /// Collect all entries under `prefix` into a Vec, grouping at `delimiter`.
    pub fn list(&self, prefix: &str, delimiter: Option<char>) -> Vec<Entry> {
        <Self as TrieBackend>::list(self, prefix, delimiter)
    }

    /// Insert a string into the radix trie, splitting edges on partial matches.
    pub fn insert(&mut self, key: &str) {
        let mut node = &mut self.root;
        let mut suffix = key;
        loop {
            let mut matched = false;
            for i in 0..node.children.len() {
                let (ref label, _) = node.children[i];
                let lcp = Trie::common_prefix_len(label, suffix);
                if lcp == 0 {
                    continue;
                }
                let (orig_label, orig_child) = node.children.remove(i);
                if lcp < orig_label.len() {
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
                    return;
                } else {
                    suffix = &suffix[lcp..];
                    node.children.insert(i, (orig_label, orig_child));
                    if suffix.is_empty() {
                        node.children[i].1.is_end = true;
                        return;
                    }
                    node = &mut node.children[i].1;
                    matched = true;
                    break;
                }
            }
            if !matched {
                let mut leaf = TrieNode::default();
                leaf.is_end = true;
                node.children.push((suffix.to_string(), Box::new(leaf)));
                return;
            }
        }
    }
    /// Insert a key with an attached CBOR value (raw bytes).
    pub fn insert_with_value(&mut self, key: &str, value: Vec<u8>) {
        // Standard insert (marks is_end and potentially splits edges)
        self.insert(key);
        // Locate the leaf node and set its payload
        if let Some(node) = Self::find_node_mut(&mut self.root, key) {
            node.value = Some(value);
        }
    }
    /// Get a reference to the raw value bytes for `key`, if any.
    /// Get the raw CBOR-encoded bytes stored under `key`, if any.
    fn get_value_raw(&self, key: &str) -> Option<&[u8]> {
        Self::find_node(&self.root, key).and_then(|node| node.value.as_deref())
    }
    /// Get the CBOR-decoded Value stored under `key`, if any.
    pub fn get_value(&self, key: &str) -> Result<Option<Value>> {
        if let Some(bytes) = self.get_value_raw(key) {
            let v: Value = serde_cbor::from_slice(bytes)
                .map_err(|_| IndexError::InvalidFormat("invalid CBOR payload"))?;
            Ok(Some(v))
        } else {
            Ok(None)
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

    /// Serialize the compressed radix trie in the new indexed on-disk format:
    /// - 4-byte MAGIC header ("IDX1")
    /// - pre-order nodes with 1-byte is_end, 2-byte LE child_count
    /// - fixed-size index table (count × [1-byte first_byte, 8-byte offset])
    /// - child blobs: 2-byte label_len + label + subtree
    pub fn write_radix<W: Write>(&self, w: &mut W) -> Result<()> {
        // write magic header + node data into a cursor, then dump to w
        let mut buf = Cursor::new(Vec::new());
        buf.write_all(&MAGIC)?;
        Self::write_node(&self.root, &mut buf)?;
        w.write_all(&buf.into_inner())?;
        Ok(())
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


    /// Deserialize the indexed on-disk format: expects a 4-byte MAGIC header,
    /// then nodes with index tables and child blobs.
    pub fn read_radix<R: Read>(r: &mut R) -> Result<Self> {
        // read entire stream into memory buffer
        let mut buf = Vec::new();
        r.read_to_end(&mut buf)?;
        // require and skip magic header
        if buf.len() < HEADER_LEN || &buf[..HEADER_LEN] != &MAGIC {
            return Err(IndexError::InvalidFormat("missing or corrupt magic header"));
        }
        let mut pos = HEADER_LEN;
        let root = Self::read_node_from_buf(&buf, &mut pos)?;
        Ok(Trie { root })
    }


    fn read_node_from_buf(buf: &[u8], pos: &mut usize) -> io::Result<TrieNode> {
        // header: is_end (1 byte) + child_count (2 bytes LE)
        if *pos + 3 > buf.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated header"));
        }
        let is_end = buf[*pos] != 0;
        *pos += 1;
        let count = u16::from_le_bytes([buf[*pos], buf[*pos + 1]]) as usize;
        *pos += 2;
        // read payload TLV: tag (1 byte) + length (4 bytes LE) + payload bytes
        if *pos + 5 > buf.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated payload TLV"));
        }
        let tag = buf[*pos];
        *pos += 1;
        let len_bytes: [u8; 4] = buf[*pos..*pos + 4]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated payload length"))?;
        let val_len = u32::from_le_bytes(len_bytes) as usize;
        *pos += 4;
        let value = if tag == 1 {
            if *pos + val_len > buf.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated payload data"));
            }
            let data = buf[*pos..*pos + val_len].to_vec();
            *pos += val_len;
            Some(data)
        } else {
            None
        };
        // read index table entries
        let mut index = Vec::with_capacity(count);
        for _ in 0..count {
            if *pos + 9 > buf.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated index entry"));
            }
            let first_byte = buf[*pos];
            let child_offset = u64::from_le_bytes(
                buf[*pos + 1..*pos + 9]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "truncated index entry"))?
            ) as usize;
            *pos += 9;
            index.push((first_byte, child_offset));
        }
        // deserialize each child from its blob
        let mut children = Vec::with_capacity(count);
        for &(_fb, child_pos) in &index {
            let mut cpos = child_pos;
            // label_len + label bytes
            if cpos + 2 > buf.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated label len"));
            }
            let label_len = u16::from_le_bytes([buf[cpos], buf[cpos + 1]]) as usize;
            cpos += 2;
            if cpos + label_len > buf.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated label"));
            }
            let label = std::str::from_utf8(&buf[cpos..cpos + label_len])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                .to_string();
            cpos += label_len;
            // recurse into child node
            let child_node = Self::read_node_from_buf(buf, &mut cpos)?;
            children.push((label, Box::new(child_node)));
        }
        Ok(TrieNode { value, children, is_end })
    }
} // end impl Trie

/// A listing entry returned by `Trie::list`: either a full key or a grouped prefix.
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
