use serde::de::DeserializeOwned;
use fst::map::Union;
use fst::Streamer as FstStreamer;

use crate::shard::Shard;
use crate::entry::Entry;

/// Trait for streaming items, similar to `Iterator`.
pub trait Streamer {
    /// The type of item yielded by the streamer.
    type Item;
    /// Return the next item in the stream, or None if finished.
    fn next(&mut self) -> Option<Self::Item>;
    /// Consume the streamer and collect all remaining items into a Vec.
    fn collect(mut self) -> Vec<Self::Item>
    where
        Self: Sized,
    {
        let mut v = Vec::new();
        while let Some(item) = self.next() {
            v.push(item);
        }
        v
    }
}

// Blanket impl so that Box<dyn Streamer> itself implements Streamer
impl<S> Streamer for Box<S>
where
    S: Streamer + ?Sized,
{
    type Item = S::Item;
    fn next(&mut self) -> Option<Self::Item> {
        (**self).next()
    }
}

/// Streamer for direct prefix listing when no delimiter grouping is needed
pub struct PrefixStream<'a, V>
where
    V: DeserializeOwned,
{
    pub(crate) stream: Union<'a>,
    pub(crate) shards: Vec<&'a Shard<V>>,
}

impl<V> Streamer for PrefixStream<'_, V>
where
    V: DeserializeOwned,
{
    type Item = Entry<V>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key_bytes, ivs)) = self.stream.next() {
            let key = String::from_utf8(key_bytes.to_vec())
                .expect("invalid utf8 in key");

            let iv = ivs.into_iter().next()
                .expect("failed to get first shard in grep");
            let value = self.shards[iv.index].payload.get(iv.value)
                .expect("failed to get payload in grep");
            Some(Entry::Key(key, value))
        } else {
            None
        }
    }
}
