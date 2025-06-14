use crate::error::Result;
use crate::streamer::Streamer as DbStreamer;
use crate::tag_index::TagIndex;
use crate::Database;
use crate::payload_store::PayloadCodec;
use crate::tag_list::TagStreamer;

use serde::de::DeserializeOwned;

impl<V, C> Database<V, C>
where
    V: DeserializeOwned,
    C: PayloadCodec,
{
    /// Stream all tags present in the database, optionally restricted to keys
    /// under the given prefix, yielding each tag and its total count
    /// (unioned across all shards' tag indexes).
    pub fn list_tags<'a>(
        &'a self,
        prefix: Option<&'a str>,
    ) -> Result<Box<dyn DbStreamer<Item = (String, usize), Cursor = Vec<u8>> + 'a>> {
        Ok(Box::new(TagStreamer::new(self, prefix)?))
    }

    /// Load on-disk tag indexes (.tags) for each shard, attaching them if present.
    /// Shards without a .tags file are silently skipped.
    pub fn load_tag_index(&mut self) -> Result<()> {
        for shard in &mut self.shards {
            if let Ok(ti) = TagIndex::open(shard.idx_path()) {
                shard.tags = Some(ti);
            }
        }
        Ok(())
    }
}