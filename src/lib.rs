use std::collections::HashSet;
use std::io::{self, Read, Write, Seek, SeekFrom, Cursor};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use memmap2::Mmap;

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
    // Each edge is labeled by a (possibly multi-character) string
    children: Vec<(String, Box<TrieNode>)>,
    is_end: bool,
} // end fn find_node_extra_buf

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
            let mut children = self.backend.children(&h);
            children.sort_by(|a, b| b.0.cmp(&a.0));
            for (lbl, child) in children {
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
    pub fn load<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        // SAFETY: we do not modify the file
        let mmap = unsafe { Mmap::map(&file)? };
        // verify magic header
        if mmap.len() < HEADER_LEN || &mmap[..HEADER_LEN] != &MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid index format"));
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
}

// Implement the generic TrieBackend trait for the in-memory Trie
impl TrieBackend for Trie {
    type Handle = usize;
    fn find_prefix(&self, prefix: &str) -> Option<GenericFindResult<Self::Handle>> {
        match self.find_node_extra(prefix) {
            Some(FindResult::Node(node)) => {
                let handle = node as *const TrieNode as usize;
                Some(GenericFindResult::Node(handle))
            }
            Some(FindResult::EdgeMid(child, tail)) => {
                let handle = child as *const TrieNode as usize;
                Some(GenericFindResult::EdgeMid(handle, tail.to_string()))
            }
            None => None,
        }
    }
    fn is_end(&self, handle: &Self::Handle) -> bool {
        let addr = *handle;
        let ptr = addr as *const TrieNode;
        unsafe { (*ptr).is_end }
    }
    fn children(&self, handle: &Self::Handle) -> Vec<(String, Self::Handle)> {
        let addr = *handle;
        let ptr = addr as *const TrieNode;
        let node = unsafe { &*ptr };
        node.children.iter()
            .map(|(l, c)| {
                let child_handle = c.as_ref() as *const TrieNode as usize;
                (l.clone(), child_handle)
            })
            .collect()
    }
}

// Implement the generic TrieBackend trait for the memory-mapped Trie
impl TrieBackend for MmapTrie {
    type Handle = usize;
    fn find_prefix(&self, prefix: &str) -> Option<GenericFindResult<Self::Handle>> {
        match find_node_extra_buf(&self.buf, prefix) {
            Some(FindResultBuf::Node(pos)) => Some(GenericFindResult::Node(pos)),
            Some(FindResultBuf::EdgeMid(label_pos, tail)) => {
                let len = u16::from_le_bytes([self.buf[label_pos], self.buf[label_pos+1]]) as usize;
                let subtree = label_pos + 2 + len;
                Some(GenericFindResult::EdgeMid(subtree, tail))
            }
            None => None,
        }
    }
    fn is_end(&self, handle: &Self::Handle) -> bool {
        self.buf.get(*handle).copied().unwrap_or(0) != 0
    }
    fn children(&self, handle: &Self::Handle) -> Vec<(String, Self::Handle)> {
        let mut pos = *handle;
        if pos + NODE_HEADER_LEN > self.buf.len() {
            return Vec::new();
        }
        let count = u16::from_le_bytes([self.buf[pos+1], self.buf[pos+2]]) as usize;
        pos += NODE_HEADER_LEN;
        let mut offsets = Vec::with_capacity(count);
        for i in 0..count {
            let ent = pos + i * INDEX_ENTRY_LEN;
            if ent + INDEX_ENTRY_LEN > self.buf.len() { break; }
            let off = u64::from_le_bytes(self.buf[ent+1..ent+9].try_into().unwrap()) as usize;
            offsets.push(off);
        }
        let mut out = Vec::with_capacity(count);
        for &lp in &offsets {
            let mut p = lp;
            if p + LABEL_LEN_LEN > self.buf.len() { continue; }
            let l = u16::from_le_bytes([self.buf[p], self.buf[p+1]]) as usize;
            p += LABEL_LEN_LEN;
            if p + l > self.buf.len() { continue; }
            if let Ok(lbl) = std::str::from_utf8(&self.buf[p..p+l]) {
                let child_pos = p + l;
                out.push((lbl.to_string(), child_pos));
            }
        }
        out
    }
}

// Internal helper for buffer-based prefix lookup
enum FindResultBuf {
    Node(usize),
    EdgeMid(usize, String),
}


// Find the node or mid-edge position for `prefix` in the mapped buffer
fn find_node_extra_buf(buf: &[u8], prefix: &str) -> Option<FindResultBuf> {
    // require and skip magic header
    if buf.len() < HEADER_LEN || &buf[..HEADER_LEN] != &MAGIC {
        return None;
    }
    let mut pos = HEADER_LEN;
    let mut rem = prefix;
    if rem.is_empty() {
        return Some(FindResultBuf::Node(pos));
    }
    loop {
        // read node header: is_end (1 byte) + child_count (2 bytes)
        if pos + NODE_HEADER_LEN > buf.len() {
            return None;
        }
        let _is_end = buf[pos];
        let count = u16::from_le_bytes([
            buf[pos + 1], buf[pos + 2]
        ]) as usize;
        pos += NODE_HEADER_LEN;
        // read index table: (first_byte, child_offset)
        let mut table = Vec::with_capacity(count);
        // read index table entries
        for _ in 0..count {
            if pos + INDEX_ENTRY_LEN > buf.len() {
                return None;
            }
            let first_byte = buf[pos];
            let child_offset = u64::from_le_bytes(
                buf[pos + 1..pos + INDEX_ENTRY_LEN].try_into().unwrap()
            ) as usize;
            table.push((first_byte, child_offset));
            pos += INDEX_ENTRY_LEN;
        }
        // match on the first byte of the remaining prefix
        let b0 = rem.as_bytes()[0];
        match table.binary_search_by_key(&b0, |(fb, _)| *fb) {
            Ok(i) => {
                let (_fb, child_pos) = table[i];
                // read the edge label at child_pos
                let mut cpos = child_pos;
                // read edge label length
                if cpos + LABEL_LEN_LEN > buf.len() {
                    return None;
                }
                let label_len = u16::from_le_bytes([
                    buf[cpos], buf[cpos + 1]
                ]) as usize;
                cpos += LABEL_LEN_LEN;
                if cpos + label_len > buf.len() {
                    return None;
                }
                // read label
                let label = match std::str::from_utf8(&buf[cpos..cpos + label_len]) {
                    Ok(s) => s,
                    Err(_) => return None,
                };
                // full-edge match?
                if rem.starts_with(label) {
                    rem = &rem[label_len..];
                    pos = cpos + label_len;
                    if rem.is_empty() {
                        return Some(FindResultBuf::Node(pos));
                    }
                    continue;
                }
                // mid-edge match?
                if label.starts_with(rem) {
                    let tail = label[rem.len()..].to_string();
                    return Some(FindResultBuf::EdgeMid(child_pos, tail));
                }
                return None;
            }
            Err(_) => return None,
        }
    }
}

/// Internal result of prefix lookup: exact node or mid-edge child + label remainder
enum FindResult<'a> {
    /// `prefix` ended exactly at this node
    Node(&'a TrieNode),
    /// `prefix` fell in the middle of an edge: child node and the rest of its label
    EdgeMid(&'a TrieNode, &'a str),
}

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


impl Trie {
    // The in-memory find_node helper is unused; use find_node_extra for lookups.
    /// Extended prefix lookup: if `prefix` ends inside an edge label, return child and remainder.
    fn find_node_extra<'a>(&'a self, prefix: &str) -> Option<FindResult<'a>> {
        let mut node = &self.root;
        let mut rem = prefix;
        if rem.is_empty() {
            return Some(FindResult::Node(node));
        }
        while !rem.is_empty() {
            let mut matched = false;
            for (label, child) in &node.children {
                if rem.starts_with(label) {
                    // consume full edge label and descend
                    rem = &rem[label.len()..];
                    node = child.as_ref();
                    matched = true;
                    break;
                } else if label.starts_with(rem) {
                    // prefix ends mid-edge; prepare child partial match
                    let tail = &label[rem.len()..];
                    return Some(FindResult::EdgeMid(child.as_ref(), tail));
                }
            }
            if !matched {
                return None;
            }
        }
        Some(FindResult::Node(node))
    }
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
    pub fn write_radix<W: Write>(&self, w: &mut W) -> io::Result<()> {
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
    pub fn read_radix<R: Read>(r: &mut R) -> io::Result<Self> {
        // read entire stream into memory buffer
        let mut buf = Vec::new();
        r.read_to_end(&mut buf)?;
        // require and skip magic header
        if buf.len() < HEADER_LEN || &buf[..HEADER_LEN] != &MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid index format"));
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
        // read index table entries
        let mut index = Vec::with_capacity(count);
        for _ in 0..count {
            if *pos + 9 > buf.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated index entry"));
            }
            let first_byte = buf[*pos];
            let child_offset = u64::from_le_bytes(
                buf[*pos + 1..*pos + 9].try_into().unwrap()
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
        Ok(TrieNode { children, is_end })
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
