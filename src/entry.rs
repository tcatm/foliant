use serde::de::DeserializeOwned;
use serde_cbor::Value;

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
