use std::collections::BTreeSet;
use std::io::{self, Read, Write, Cursor};
use std::fs::File;
use std::path::Path;
use memmap2::Mmap;
// Removed unused serde derives; index persistence moved to CLI text/binary format

/// A node in the trie.
#[derive(Default)]
struct TrieNode {
    // Each edge is labeled by a (possibly multi-character) string
    children: Vec<(String, Box<TrieNode>)>,
    is_end: bool,
}

/// A prefix trie for indexing strings.
pub struct Trie {
    root: TrieNode,
}

impl Trie {
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
                // Remove the matching child to modify it
                let (orig_label, orig_child) = node.children.remove(i);
                if lcp < orig_label.len() {
                    // Partial match -> split the edge
                    let mut intermediate = TrieNode::default();
                    // Remainder of the original label
                    let label_rem = &orig_label[lcp..];
                    intermediate.children.push((label_rem.to_string(), orig_child));
                    // Remainder of the new key
                    let key_rem = &suffix[lcp..];
                    if key_rem.is_empty() {
                        intermediate.is_end = true;
                    } else {
                        let mut leaf = TrieNode::default();
                        leaf.is_end = true;
                        intermediate.children.push((key_rem.to_string(), Box::new(leaf)));
                    }
                    // Insert the new split edge
                    let prefix_label = &orig_label[..lcp];
                    node.children.insert(i, (prefix_label.to_string(), Box::new(intermediate)));
                    return;
                } else {
                    // Edge fully matches prefix of suffix -> descend
                    suffix = &suffix[lcp..];
                    // Re-insert without modification
                    node.children.insert(i, (orig_label, orig_child));
                    if suffix.is_empty() {
                        // Exact key end: mark this node
                        node.children[i].1.is_end = true;
                        return;
                    }
                    // Descend into child node
                    node = &mut node.children[i].1;
                    matched = true;
                    break;
                }
            }
            if !matched {
                // No matching edge -> add new leaf
                let mut leaf = TrieNode::default();
                leaf.is_end = true;
                node.children.push((suffix.to_string(), Box::new(leaf)));
                return;
            }
        }
    }

    /// List entries under `prefix`. If `delimiter` is None, returns all full keys
    /// starting with `prefix`. If `Some(d)`, groups by the first `d` after the prefix.
    pub fn list(&self, prefix: &str, delimiter: Option<char>) -> Vec<Entry> {
        let mut buckets = BTreeSet::new();
        for key in self.collect_keys() {
            if !key.starts_with(prefix) {
                continue;
            }
            if let Some(d) = delimiter {
                let suffix = &key[prefix.len()..];
                if let Some(pos) = suffix.find(d) {
                    let group = &key[..prefix.len() + pos + 1];
                    buckets.insert(group.to_string());
                    continue;
                }
            }
            buckets.insert(key);
        }
        // Map into Entry enums
        let delim = delimiter;
        buckets.into_iter().map(|s| {
            if let Some(d) = delim {
                if s.ends_with(d) {
                    return Entry::CommonPrefix(s);
                }
            }
            Entry::Key(s)
        }).collect()
    }

    /// Gather all keys in the trie by DFS.
    fn collect_keys(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut path = String::new();
        Trie::collect_all(&self.root, &mut path, &mut out);
        out
    }

    fn collect_all(node: &TrieNode, path: &mut String, out: &mut Vec<String>) {
        if node.is_end {
            out.push(path.clone());
        }
        for (label, child) in &node.children {
            let orig_len = path.len();
            path.push_str(label);
            Trie::collect_all(child, path, out);
            path.truncate(orig_len);
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
    /// Serialize the compressed radix trie (pre-order with edge labels).
    pub fn write_radix<W: Write>(&self, w: &mut W) -> io::Result<()> {
        Self::write_node(&self.root, w)
    }

    fn write_node<W: Write>(node: &TrieNode, w: &mut W) -> io::Result<()> {
        // Write end-of-key flag
        w.write_all(&[node.is_end as u8])?;
        // Write number of children
        let count = node.children.len() as u16;
        w.write_all(&count.to_le_bytes())?;
        // Write each child: [label_len][label_bytes][child_subtree]
        for (label, child) in &node.children {
            let len = label.len() as u16;
            w.write_all(&len.to_le_bytes())?;
            w.write_all(label.as_bytes())?;
            Self::write_node(child, w)?;
        }
        Ok(())
    }

    /// Deserialize a radix trie from a pre-order serialized form.
    pub fn read_radix<R: Read>(r: &mut R) -> io::Result<Self> {
        let root = Self::read_node(r)?;
        Ok(Trie { root })
    }

    /// Load a radix trie from an on-disk serialized file via mmap.
    pub fn load_radix<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let mut cursor = Cursor::new(&mmap[..]);
        Self::read_radix(&mut cursor)
    }

    fn read_node<R: Read>(r: &mut R) -> io::Result<TrieNode> {
        let mut flag = [0u8; 1];
        r.read_exact(&mut flag)?;
        let is_end = flag[0] != 0;
        let mut buf2 = [0u8; 2];
        r.read_exact(&mut buf2)?;
        let child_count = u16::from_le_bytes(buf2) as usize;
        let mut children = Vec::with_capacity(child_count);
        for _ in 0..child_count {
            r.read_exact(&mut buf2)?;
            let label_len = u16::from_le_bytes(buf2) as usize;
            let mut lb = vec![0u8; label_len];
            r.read_exact(&mut lb)?;
            let label = String::from_utf8(lb)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let child = Self::read_node(r)?;
            children.push((label, Box::new(child)));
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
    /// Get the inner string slice.
    pub fn as_str(&self) -> &str {
        match self {
            Entry::Key(s) | Entry::CommonPrefix(s) => s,
        }
    }
    /// Return the entry type as a string: "Key" or "CommonPrefix".
    pub fn kind(&self) -> &'static str {
        match self {
            Entry::Key(_) => "Key",
            Entry::CommonPrefix(_) => "CommonPrefix",
        }
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Entry) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Entry) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}
