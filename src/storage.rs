use std::io;
use crate::GenericFindResult;

/// Abstract node‐reader over arbitrary storage.
pub(crate) trait NodeStorage {
    /// A handle into the storage (e.g. a pointer or buffer offset).
    type Handle: Clone;

    /// Return the root node handle.
    fn root_handle(&self) -> Self::Handle;

    /// Is this node terminal (end of a key)?
    fn is_terminal(&self, handle: &Self::Handle) -> io::Result<bool>;

    /// Read all child edges: (edge_label, child_handle).
    fn read_children(&self, handle: &Self::Handle) -> io::Result<Vec<(String, Self::Handle)>>;
}

/// Generic prefix‐lookup over any NodeStorage implementation.
pub(crate) fn generic_find_prefix<S: NodeStorage>(
    store: &S,
    prefix: &str,
) -> io::Result<Option<GenericFindResult<S::Handle>>> {
    let mut rem = prefix;
    let mut handle = store.root_handle();
    if rem.is_empty() {
        return Ok(Some(GenericFindResult::Node(handle)));
    }
    loop {
        let children = store.read_children(&handle)?;
        let mut matched = false;
        // scan children for a matching or mid‐edge label
        for (label, child) in &children {
            if rem.starts_with(label) {
                // consume full edge label
                rem = &rem[label.len()..];
                handle = child.clone();
                if rem.is_empty() {
                    return Ok(Some(GenericFindResult::Node(handle)));
                }
                matched = true;
                break;
            } else if label.starts_with(rem) {
                // prefix ends mid‐edge
                let tail = label[rem.len()..].to_string();
                return Ok(Some(GenericFindResult::EdgeMid(child.clone(), tail)));
            }
        }
        if !matched {
            return Ok(None);
        }
    }
}

/// Generic children enumeration over any NodeStorage implementation.
pub(crate) fn generic_children<S: NodeStorage>(
    store: &S,
    handle: &S::Handle,
) -> io::Result<Vec<(String, S::Handle)>> {
    store.read_children(handle)
}