use std::collections::HashSet;
use std::io::{self, Read, Write, Seek, SeekFrom, Cursor};
use std::fs::File;
use std::path::Path;
use memmap2::Mmap;

// Magic header for the new indexed format: 4 bytes, 'I','D','X','1'
const MAGIC: [u8; 4] = *b"IDX1";

/// A node in the trie.
#[derive(Default, Debug)]
struct TrieNode {
    // Each edge is labeled by a (possibly multi-character) string
    children: Vec<(String, Box<TrieNode>)>,
    is_end: bool,
}

/// A prefix trie for indexing strings.
pub struct Trie {
    root: TrieNode,
}

/// A lazily-loaded radix trie over a memory-mapped index file.
pub struct MmapTrie {
    buf: Mmap,
}

impl MmapTrie {
    /// Load a serialized indexed radix trie from disk via mmap.
    /// Expects the file to begin with the 4-byte MAGIC header "IDX1".
    pub fn load<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        // SAFETY: we do not modify the file
        let mmap = unsafe { Mmap::map(&file)? };
        // verify magic header
        if mmap.len() < MAGIC.len() || &mmap[..MAGIC.len()] != &MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid index format"));
        }
        Ok(MmapTrie { buf: mmap })
    }

    /// Iterate over entries under `prefix`, grouping at the first `delimiter`.
    pub fn list_iter<'a>(
        &'a self,
        prefix: &str,
        delimiter: Option<char>
    ) -> MmapTrieGroupIter<'a> {
        let pref = prefix.to_string();
        let buf = &self.buf;
        match find_node_extra_buf(buf, prefix) {
            Some(FindResultBuf::Node(pos)) =>
                MmapTrieGroupIter::new(buf, pref.clone(), delimiter, pos),
            Some(FindResultBuf::EdgeMid(label_pos, tail)) => {
                let label_len = u16::from_le_bytes([
                    buf[label_pos], buf[label_pos + 1]
                ]) as usize;
                let subtree_pos = label_pos + 2 + label_len;
                let mut init = pref.clone(); init.push_str(&tail);
                MmapTrieGroupIter::with_init(buf, pref.clone(), delimiter,
                    vec![(subtree_pos, init)])
            }
            None => MmapTrieGroupIter::empty(pref, delimiter),
        }
    }

    /// Collect all entries under `prefix` into a Vec, grouping at `delimiter`.
    pub fn list(&self, prefix: &str, delimiter: Option<char>) -> Vec<Entry> {
        self.list_iter(prefix, delimiter).collect()
    }
}

// Internal helper for buffer-based prefix lookup
enum FindResultBuf {
    Node(usize),
    EdgeMid(usize, String),
}


// Find the node or mid-edge position for `prefix` in the mapped buffer
fn find_node_extra_buf(buf: &[u8], prefix: &str) -> Option<FindResultBuf> {
    // skip magic header if present
    let mut pos = if buf.len() >= MAGIC.len() && &buf[..MAGIC.len()] == &MAGIC {
        MAGIC.len()
    } else {
        0
    };
    let mut rem = prefix;
    if rem.is_empty() {
        return Some(FindResultBuf::Node(pos));
    }
    loop {
        // read header: is_end + child_count
        if pos + 3 > buf.len() {
            return None;
        }
        let _is_end = buf[pos];
        let count = u16::from_le_bytes([buf[pos + 1], buf[pos + 2]]) as usize;
        pos += 3;
        // read index table: (first_byte, child_offset)
        let mut table = Vec::with_capacity(count);
        for _ in 0..count {
            if pos + 9 > buf.len() {
                return None;
            }
            let first_byte = buf[pos];
            let child_offset = u64::from_le_bytes(buf[pos + 1..pos + 9].try_into().unwrap()) as usize;
            table.push((first_byte, child_offset));
            pos += 9;
        }
        // match on the first byte of the remaining prefix
        let b0 = rem.as_bytes()[0];
        match table.binary_search_by_key(&b0, |(fb, _)| *fb) {
            Ok(i) => {
                let (_fb, child_pos) = table[i];
                // read the edge label at child_pos
                let mut cpos = child_pos;
                if cpos + 2 > buf.len() {
                    return None;
                }
                let label_len = u16::from_le_bytes([buf[cpos], buf[cpos + 1]]) as usize;
                cpos += 2;
                if cpos + label_len > buf.len() {
                    return None;
                }
                let label = std::str::from_utf8(&buf[cpos..cpos + label_len]).unwrap();
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

/// Iterator over a memmapped radix trie without full deserialization.
pub struct MmapTrieGroupIter<'a> {
    buf: &'a [u8],
    stack: Vec<(usize, String)>,
    prefix: String,
    delimiter: Option<char>,
    seen: HashSet<String>,
}

impl<'a> MmapTrieGroupIter<'a> {
    fn empty(prefix: String, delimiter: Option<char>) -> Self {
        MmapTrieGroupIter { buf: &[], stack: Vec::new(), prefix, delimiter, seen: HashSet::new() }
    }
    fn new(buf: &'a [u8], prefix: String, delimiter: Option<char>, pos: usize) -> Self {
        MmapTrieGroupIter { buf, stack: vec![(pos, prefix.clone())], prefix, delimiter, seen: HashSet::new() }
    }
    fn with_init(buf: &'a [u8], prefix: String, delimiter: Option<char>, init: Vec<(usize, String)>) -> Self {
        MmapTrieGroupIter { buf, stack: init, prefix, delimiter, seen: HashSet::new() }
    }
}

impl<'a> Iterator for MmapTrieGroupIter<'a> {
    type Item = Entry;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some((pos, path)) = self.stack.pop() {
            // grouping logic
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
                    if !suffix.is_empty() && path.ends_with(d) && self.seen.insert(path.clone()) {
                        return Some(Entry::CommonPrefix(path.clone()));
                    }
                }
            }
            // parse node at pos (indexed on-disk format)
            let mut cpos = pos;
            let is_end = self.buf[cpos] != 0;
            cpos += 1;
            let count = u16::from_le_bytes([self.buf[cpos], self.buf[cpos + 1]]) as usize;
            cpos += 2;
            // read the index table: count entries of (first_byte, child_offset)
            let mut offsets = Vec::with_capacity(count);
            for _ in 0..count {
                let _fb = self.buf[cpos];
                let off = u64::from_le_bytes(
                    self.buf[cpos + 1..cpos + 9].try_into().unwrap()
                ) as usize;
                offsets.push(off);
                cpos += 9;
            }
            // collect children by parsing each label at its offset
            let mut children = Vec::with_capacity(count);
            for &child_pos in &offsets {
                let mut lpos = child_pos;
                let label_len = u16::from_le_bytes([
                    self.buf[lpos], self.buf[lpos + 1]
                ]) as usize;
                lpos += 2;
                let label = std::str::from_utf8(
                    &self.buf[lpos..lpos + label_len]
                ).unwrap().to_string();
                let desc_pos = lpos + label_len;
                children.push((label, desc_pos));
            }
            // descend children in reverse lex order
            children.sort_by(|a, b| b.0.cmp(&a.0));
            for (label, child_pos) in children {
                let mut new_path = path.clone();
                new_path.push_str(&label);
                self.stack.push((child_pos, new_path));
            }
            // yield key if terminal
            if is_end && path.starts_with(&self.prefix) {
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

/// Iterator over entries in the compressed radix trie with on-the-fly grouping.
#[derive(Clone, Debug)]
pub struct TrieGroupIter<'a> {
    stack: Vec<(&'a TrieNode, String)>,
    prefix: String,
    delimiter: Option<char>,
    seen: HashSet<String>,
}

impl<'a> TrieGroupIter<'a> {
    /// Create an empty iterator (no entries) for nonexistent prefix
    pub(crate) fn empty(prefix: String, delimiter: Option<char>) -> Self {
        TrieGroupIter { stack: Vec::new(), prefix, delimiter, seen: HashSet::new() }
    }

    /// Create a new iterator that groups at the first `delimiter` for each prefix.
    pub(crate) fn new(root: &'a TrieNode, prefix: String, delimiter: Option<char>) -> Self {
        // Initialize stack with the full prefix as starting path
        let init_path = prefix.clone();
        TrieGroupIter { stack: vec![(root, init_path)], prefix, delimiter, seen: HashSet::new() }
    }
}

impl<'a> Iterator for TrieGroupIter<'a> {
    type Item = Entry;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some((node, path)) = self.stack.pop() {
            if let Some(d) = self.delimiter {
                if path.starts_with(&self.prefix) {
                    let suffix = &path[self.prefix.len()..];
                    if let Some(pos) = suffix.find(d) {
                        let group = path[..self.prefix.len() + pos + 1].to_string();
                        if self.seen.insert(group.clone()) {
                            return Some(Entry::CommonPrefix(group));
                        }
                        continue;
                    }
                    // For descendant paths with trailing delimiter, yield grouping
                    if !suffix.is_empty() && path.ends_with(d) && self.seen.insert(path.clone()) {
                        return Some(Entry::CommonPrefix(path.clone()));
                    }
                }
            }
            // descend children in reverse lex order for correct ordering
            {
                let mut children: Vec<_> = node.children.iter().collect();
                children.sort_by(|a, b| b.0.cmp(&a.0));
                for (label, child) in children {
                    let mut new_path = path.clone();
                    new_path.push_str(label);
                    self.stack.push((child, new_path));
                }
            }
            // after queuing children, yield key if terminal
            if node.is_end && path.starts_with(&self.prefix) {
                // If the key ends with the delimiter, present it as a common prefix
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

// Allow comparing iterator directly to Vec<Entry> in tests
impl<'a> PartialEq<Vec<Entry>> for TrieGroupIter<'a> {
    fn eq(&self, other: &Vec<Entry>) -> bool {
        self.clone().collect::<Vec<_>>() == *other
    }
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

    /// Streaming iterator over entries under `prefix`, grouping at the first `delimiter`.
    /// Supports prefixes that fall mid-edge by routing into the single matching subtree.
    pub fn list_iter<'a>(&'a self, prefix: &str, delimiter: Option<char>) -> TrieGroupIter<'a> {
        // Find either an exact node or a mid-edge match
        let pref = prefix.to_string();
        match self.find_node_extra(prefix) {
            Some(FindResult::Node(node)) => {
                TrieGroupIter::new(node, pref, delimiter)
            }
            Some(FindResult::EdgeMid(child, rem_label)) => {
                // Start directly at the partial-match child, seeding full path = prefix + rem_label
                let mut init = pref.clone(); init.push_str(rem_label);
                TrieGroupIter { stack: vec![(child, init)], prefix: pref, delimiter, seen: HashSet::new() }
            }
            None => TrieGroupIter::empty(pref, delimiter),
        }
    }
    /// List entries under `prefix` into a Vec<Entry>, grouping at the first `delimiter`.
    pub fn list(&self, prefix: &str, delimiter: Option<char>) -> Vec<Entry> {
        self.list_iter(prefix, delimiter).collect()
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

    /// Serialize the compressed radix trie to any `Write`, using a binary-searchable index internally.
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


    /// Deserialize using the new indexed format (reads in-memory buffer).
    pub fn read_radix<R: Read>(r: &mut R) -> io::Result<Self> {
        // read entire stream into memory buffer
        let mut buf = Vec::new();
        r.read_to_end(&mut buf)?;
        // skip magic header if present
        let mut pos = if buf.len() >= MAGIC.len() && &buf[..MAGIC.len()] == &MAGIC {
            MAGIC.len()
        } else {
            0
        };
        let root = Self::read_node_from_buf(&buf, &mut pos)?;
        Ok(Trie { root })
    }

    /// Load a trie by memory-mapping the serialized file.
    pub fn load_mmap<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        // SAFETY: we won't modify the file
        let mmap = unsafe { Mmap::map(&file)? };
        let buf = &mmap[..];
        let mut pos = 0;
        let root = Self::read_node_from_buf(buf, &mut pos)?;
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
