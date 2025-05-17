use std::collections::HashSet;
use std::io::{self, Read, Write};
// Removed unused serde derives; index persistence moved to CLI text/binary format

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
}

impl<'a> TrieGroupIter<'a> {
    /// Create a new iterator that groups at the first `delimiter` for each prefix.
    pub(crate) fn new(root: &'a TrieNode, prefix: String, delimiter: Option<char>) -> Self {
        TrieGroupIter { stack: vec![(root, String::new())], prefix, delimiter, seen: HashSet::new() }
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
                        // yield this CommonPrefix and prune subtree
                        return Some(Entry::CommonPrefix(group));
                    }
                    // already yielded, skip this subtree
                    continue;
                }
                    if path.ends_with(d) && self.seen.insert(path.clone()) {
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
    /// Locate the node corresponding to the end of `prefix` in the compressed trie.
    fn find_node<'a>(&'a self, prefix: &str) -> Option<&'a TrieNode> {
        let mut node = &self.root;
        let mut rem = prefix;
        if rem.is_empty() {
            return Some(node);
        }
        while !rem.is_empty() {
            let mut matched = false;
            for (label, child) in &node.children {
                if rem.starts_with(label) {
                    rem = &rem[label.len()..];
                    node = &*child;
                    matched = true;
                    break;
                } else if label.starts_with(rem) {
                    // prefix ends in middle of this edge
                    return Some(node);
                }
            }
            if !matched {
                return None;
            }
        }
        Some(node)
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

    /// Streaming iterator over entries under `prefix`, grouping at the first `delimiter`.
    pub fn list_iter<'a>(&'a self, prefix: &str, delimiter: Option<char>) -> TrieGroupIter<'a> {
        if let Some(node) = self.find_node(prefix) {
            TrieGroupIter::new(node, prefix.to_string(), delimiter)
        } else {
            TrieGroupIter::empty(prefix.to_string(), delimiter)
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
