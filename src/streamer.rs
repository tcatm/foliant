use fst::map::Union;
use fst::Streamer as FstStreamer;
use serde::de::DeserializeOwned;

use crate::entry::Entry;
use crate::payload_store::{CborPayloadCodec, PayloadCodec};
use crate::shard::Shard;

/// Trait for streaming items, similar to `Iterator`.
pub trait Streamer {
    /// The type of item yielded by the streamer.
    type Item;

    /// The type of the continuation cursor for this streamer.
    ///
    /// Calling `cursor()` returns a token representing the position *after* the
    /// last returned item. Use `seek()` to resume from that position.
    type Cursor: Clone + Eq + serde::Serialize + serde::de::DeserializeOwned;

    /// Return the next item in the stream, or `None` if finished.
    fn next(&mut self) -> Option<Self::Item>;

    /// Snapshot the stream state so you can resume *after* the last item returned by `next()`.
    fn cursor(&self) -> Self::Cursor;

    /// Rewind or fast‑forward the stream to the given cursor position.
    /// After `seek(c)`, the next call to `next()` returns the item immediately after that cursor.
    fn seek(&mut self, cursor: Self::Cursor);

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
    type Cursor = S::Cursor;

    fn next(&mut self) -> Option<Self::Item> {
        (**self).next()
    }

    fn cursor(&self) -> Self::Cursor {
        (**self).cursor()
    }

    fn seek(&mut self, cursor: Self::Cursor) {
        (**self).seek(cursor)
    }
}

/// Streamer for direct prefix listing when no delimiter grouping is needed
pub struct PrefixStream<'a, V, C: PayloadCodec = CborPayloadCodec>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    pub(crate) stream: Union<'a>,
    pub(crate) shards: Vec<&'a Shard<V, C>>,
}

impl<V, C> Streamer for PrefixStream<'_, V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    type Item = Entry<V>;
    type Cursor = Vec<u8>;

    fn cursor(&self) -> Self::Cursor {
        unimplemented!("cursor not implemented");
    }

    fn seek(&mut self, _cursor: Self::Cursor) {
        unimplemented!("seek not implemented");
    }
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key_bytes, ivs)) = self.stream.next() {
            let key = String::from_utf8(key_bytes.to_vec()).expect("invalid utf8 in key");

            let iv = ivs
                .into_iter()
                .next()
                .expect("failed to get first shard in grep");
            let weight = iv.value;
            let payload_ptr = self.shards[iv.index]
                .lookup
                .get(weight as u32)
                .expect("failed to get lookup in grep")
                .payload_ptr;
            let value = self.shards[iv.index]
                .payload
                .get(payload_ptr)
                .expect("failed to get payload in grep");
            Some(Entry::Key(key, weight, value))
        } else {
            None
        }
    }
}
