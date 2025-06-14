mod builder;
mod database;
mod entry;
mod error;
mod frame;
mod lookup_table_store;
pub mod multi_list;
pub mod payload_store;
pub use payload_store::{convert_v2_to_v3_inplace, PayloadStoreBuilderV2, PayloadStoreV2};
mod shard;
pub mod shard_provider;
mod streamer;
mod tag_index;
mod tag_list;
mod search_index;

pub use builder::DatabaseBuilder;
pub use builder::SegmentInfo;
pub use database::Database;
pub use database::TagMode;
pub use entry::Entry;
pub use multi_list::TagFilterConfig;
pub use error::{IndexError, Result};
pub use shard::Shard;
pub use streamer::{PrefixStream, Streamer};
pub use tag_index::{TagIndex, TagIndexBuilder};
pub use tag_list::TagStreamer;
pub use search_index::{SearchIndex, SearchIndexBuilder};

use indicatif::{ProgressBar, ProgressStyle};
use serde::de::DeserializeOwned;
use std::{fs, path::Path};

/// Load a database from either a single shard (.idx) or a directory of shards.
/// When loading a directory, displays a progress bar and optionally limits
/// the number of shards to load.
pub fn load_db<V, P>(path: P, limit: Option<usize>) -> Result<Database<V>>
where
    V: DeserializeOwned + 'static,
    P: AsRef<Path>,
{
    let p = path.as_ref();
    if p.is_dir() {
        let mut db = Database::<V>::new();
        let mut paths = Vec::new();
        for entry in fs::read_dir(p)? {
            let pth = entry?.path();
            if pth.extension().and_then(|s| s.to_str()) == Some("idx")
                && pth.with_extension("payload").exists()
            {
                paths.push(pth);
            }
        }
        let total = match limit {
            Some(n) => n.min(paths.len()),
            None => paths.len(),
        } as u64;
        let pb = ProgressBar::new(total);
        let style = ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] [{eta_precise}] {pos}/{len} ({percent}%) {msg}",
        )
        .unwrap()
        .progress_chars("#>-");
        pb.set_style(style);
        pb.set_message("loading shards");
        for pth in paths.into_iter().take(limit.unwrap_or(usize::MAX)) {
            let name = pth.file_name().and_then(|s| s.to_str()).unwrap_or("");
            pb.set_message(name.to_string());
            if let Err(e) = db.add_shard(&pth) {
                eprintln!("warning: failed to add shard {:?}: {}", pth, e);
            }
            pb.inc(1);
        }
        pb.finish();
        Ok(db)
    } else {
        Database::<V>::open(p)
    }
}
