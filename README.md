# foliant

foliant is a command-line tool for building and querying a prefix index with optional JSON payloads.

Supported features:
- Build a compressed, binary-searchable prefix index from plain text or JSONL input
- Optional CBOR-encoded payloads extracted from JSON lines
- Memory-mapped FST index for zero-copy, zero-allocation prefix listing
- Grouped listings by custom delimiters
- Substring search with n-gram indexing (supports 1-2 character queries, Unicode, case-insensitive)
- Tag-based filtering with AND/OR modes
- CLI tool with `index`, `list`, `shell`, and `server` commands

# Table of Contents
1. [Build & Test](#build--test)
2. [CLI Usage Examples](#cli-usage-examples)
3. [Generating Sample JSONL Input](#generating-sample-jsonl-input)
4. [Developer Guide](#developer-guide)
   - [Key Types](#key-types)
   - [Design Notes](#design-notes)
5. [On-Disk Format](#on-disk-format)

## Build & Test
Requires Rust 1.x. From the repo root:
```bash
cargo build --release
cargo test
```

## CLI Usage Examples

> **Note:** the `-i`/`--index` flag now accepts either a single `.idx` filepath (explicit extension required) or a directory containing one or more `.idx` shards (with matching `.payload` files).

### Build an index from plain text lines
```
foliant index -i data.idx --input input.txt
```

### Build an index from JSON lines
```
foliant index -i data.idx -j path --input sample.jsonl
```

### Build an index with tags (two-pass)
```bash
foliant index -i data.idx --json path --input sample.jsonl --tag-index tags
```
### Build an index with a custom prefix
```bash
foliant index -i data.idx --json path --prefix foo/ --input sample.jsonl
```

### List entries
- List all keys:
  ```bash
  foliant list -i data.idx
  ```
- List under a prefix:
  ```bash
  foliant list -i data.idx prefix
  ```
- Group by delimiter `/`:
  ```bash
  foliant list -i data.idx -d / prefix
  ```

### Interactive shell
```bash
# Single-shard database:
foliant shell -i data.idx

# Multi-shard database directory:
foliant shell -i shards_dir
```

### Search entries in interactive shell
```bash
# Once inside the shell:
> search <query>

# You can also use search filters with the ls command:
> ls -q term1 -q term2    # Find entries containing both term1 AND term2
> ls -q jpg -! png        # Find entries with "jpg" but not "png"
> ls prefix/ -q search    # Combine prefix and search
```

### Generate a tag index for an existing database
```bash
foliant tag-index -i data.idx --tag-field tags
```

### Generate a search index for an existing database
```bash
foliant search-index -i data.idx
```

The search index enables substring search functionality, including support for:
- Single and two-character queries (e.g., searching for "a" or "pg")  
- Unicode support (emojis, Chinese characters, etc.)
- Case-insensitive and accent-insensitive search

### Run the HTTP server
```bash
foliant server -i data.idx --addr 127.0.0.1:3000
```

The server provides a REST API with the following endpoints:
- `GET /keys` - List keys with optional filters
  - `?prefix=foo/` - Filter by prefix
  - `?delim=/` - Group by delimiter
  - `?search=term1,term2` - Search for entries containing all terms
  - `?exclude_search=term3` - Exclude entries containing terms
  - `?include_tags=tag1,tag2` - Filter by tags
  - `?limit=100` - Limit results
  - `?cursor=...` - Pagination cursor
- `GET /tags` - List all tags
- `GET /tags/counts` - List tags with counts

When values are present, they are shown after the key in dim ANSI color, serialized as one-line JSON.

## Generating Sample JSONL Input

Here’s a quick shell snippet to list files in `$HOME` as JSON objects:
```bash
# Using GNU find (Linux) with -printf
find "$HOME" -type f \
  -printf '{"path":"%p","last_modified":%Ts,"size":%s}\n' \
  > sample.jsonl
```

On systems without `find -printf` (e.g. BSD/macOS), use `stat -f`:
```bash
find "$HOME" -type f | while IFS= read -r file; do
  stat -f '{"path":"%N","last_modified":%m,"size":%z}' "$file"
done > sample.jsonl
```

Sample lines in `sample.jsonl`:
```json
{"path":"/home/alice/.bashrc","last_modified":1633072800,"size":4285}
{"path":"/home/alice/docs/report.pdf","last_modified":1633159200,"size":234567}
```  

## Developer Guide

### Key Types
- `DatabaseBuilder<V, C = CborPayloadCodec>`: builder for creating a new on-disk database; insert keys with optional values (`V: Serialize`), then finalize to write the `<idx_path>.idx`, `<idx_path>.lookup`, and `<idx_path>.payload` files.
- `Database<V, C = CborPayloadCodec>`: read-only handle (`V: DeserializeOwned`) for querying the index; supports prefix listing (`list`) and value lookup (`get_value`)
- `Entry`: enum returned by `Database::list`, either `Entry::Key(String)` for full keys or `Entry::CommonPrefix(String)` for grouped prefixes
- `PayloadStoreBuilder<V, C = CborPayloadCodec>` and `PayloadStore<V, C = CborPayloadCodec>`: internal types for writing and reading the `.payload` file using the provided `PayloadCodec` (defaults to CBOR)

- `TagIndex`: read-only handle for querying a tag index file; supports streaming all tags with their memory offsets via `list_tags`, retrieving a tag's bitmap via `get`, or decoding a memory offset into a bitmap via `get_bitmap`
- `TagIndexBuilder`: builder for creating a new tag index file; insert lookup IDs with associated tags, then finalize to write the `.tags` file
- `TantivyIndex`: read-only handle for querying a full-text search index per shard; supports executing raw Tantivy queries against keys.
- `TantivyIndexBuilder`: builder for creating or updating a full-text search index; insert keys and finalize to write the `.search/` directories.

### Streaming API

Below is a summary of the `Database` methods that return a stream, along with their arguments, stream-item types, and a brief description:

| Method         | Arguments                                                              | Stream Item         | Description                        |
| -------------- | ---------------------------------------------------------------------- | ------------------- | ---------------------------------- |
| `list`         | `prefix: &str`, `delimiter: Option<char>`                              | `Entry<V>`          | Prefix/delimiter listing           |
| `grep`         | `prefix: Option<&str>`, `re: &str`                                     | `Entry<V>`          | Full-key regex search              |
| `search`       | `prefix: Option<&str>`, `query: &str`                                  | `Entry<V>`          | Fuzzy full-text (Tantivy) search   |
| `list_by_tags` | `include_tags: &[&str]`, `exclude_tags: &[&str]`, `mode: TagMode`, `prefix: Option<&str>` | `Entry<V>`          | Tag-filtered listing               |
| `list_tag_names` | `prefix: Option<&str>`                                                | `String`            | Tag listing (names only, no counts, fast) |
| `list_tags`      | `prefix: Option<&str>`                                                 | `(String, usize)`   | Tag listing (counts for keys under prefix) |

### Design Notes
- The index uses the `fst` crate's `MapBuilder` to store keys with `u64` lookup identifiers as weights, each pointing into the lookup-FST mapping IDs to payload pointers.
- The lookup table is itself an on-disk FST mapping 32-bit big-endian lookup IDs to 64-bit payload pointers as weights, enabling atomic, checksum-validated writes and zero-copy memory-mapped queries.
- Payloads are encoded using the provided `PayloadCodec` (defaults to CBOR) and stored sequentially with a 2-byte little-endian length prefix
- Memory-mapped I/O enables zero-copy, zero-allocation prefix listing and fast value lookup
- Listings can be grouped by the first occurrence of a custom delimiter

## On-Disk Format
foliant produces up to four files per database (the tag index is optional):

- During index building, intermediate files `<idx_path>.idx.tmp` and `<idx_path>.lookup.tmp` are written in-place and then atomically renamed to `<idx_path>.idx` and `<idx_path>.lookup` once complete.

- `<idx_path>.idx`: an on-disk FST index comprising one or more inlined sub-FST segments, each followed by a 16-byte trailer.  The trailer is a packed little-endian struct:
  ```text
  u32 seg_id | u64 start_off | u32 crc32(seg_id∥start_off)
  ```
  To read the index, `mmap` the file and scan backwards reading trailers from the end, validating each CRC, and extracting each sub-FST.  The segments are then merged in a single pass into the final lookup structure for queries.
- `<idx_path>.lookup`: an on-disk FST mapping 32-bit big-endian lookup IDs to 64-bit payload pointers (encoded as FST weights). To read it, memory-map the file and use the `fst::Map` API (e.g. `map.get(&id.to_be_bytes())`) to fetch the payload file offset (stored as `offset+1` in the `.payload` file).
- `<idx_path>.payload`: a flat file storing CBOR‑encoded payloads. The file begins with a 4-byte magic header (`FPAY`) and a 2-byte little-endian format version (`1` or `2`).
  - Format v1 is uncompressed; each payload record then consists of:
    1. A 2-byte little-endian length (`u16`)
    2. The CBOR‑encoded value bytes
  - Format v2 is compressed in zstd blocks of up to 128 KiB uncompressed; each block consists of:
    1. A 4-byte little-endian uncompressed length (`u32`)
    2. A 4-byte little-endian compressed length (`u32`)
    3. The zstd-compressed block data

- `<idx_path>.tags`: a tag index file with simplified v2 format comprising:
  1. Header: `FTGT` magic + version(2) + FST size
  2. FST section mapping each lowercase tag to a 64-bit memory offset  
  3. Data entries at each offset: `(bitmap_len: u32, bitmap: [u8], cbor: [u8])`
     - The bitmap contains Roaring bitmap data for document IDs with this tag
     - The CBOR contains the original case version of the tag string
     - Readers can efficiently skip CBOR data if only bitmap access is needed

The `.idx` file begins with the magic header and structure defined by the `fst` crate. This format enables fast, memory-mapped prefix queries and efficient payload retrieval with minimal allocations.
