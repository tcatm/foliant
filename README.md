# foliant

foliant is a command-line tool for building and querying a prefix index with optional JSON payloads.

Supported features:
- Build a compressed, binary-searchable prefix index from plain text or JSONL input
- Optional CBOR-encoded payloads extracted from JSON lines
- Memory-mapped FST index for zero-copy, zero-allocation prefix listing
- Grouped listings by custom delimiters
- CLI tool with `index`, `list`, and `shell` commands

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

foliant shell -i data.idx
```

### Generate a tag index for an existing database
```bash
foliant tag-index -i data.idx --tag-field tags
```

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
  stat -f '{"path":"%N","last_modified":%m,"size":%z}\n' "$file"
done > sample.jsonl
```

Sample lines in `sample.jsonl`:
```json
{"path":"/home/alice/.bashrc","last_modified":1633072800,"size":4285}
{"path":"/home/alice/docs/report.pdf","last_modified":1633159200,"size":234567}
```  

## Developer Guide

### Key Types
- `DatabaseBuilder<V, C = CborPayloadCodec>`: builder for creating a new on-disk database; insert keys with optional values (`V: Serialize`), then finalize to write the `.idx`, `.lookup`, and `.payload` files.
- `Database<V, C = CborPayloadCodec>`: read-only handle (`V: DeserializeOwned`) for querying the index; supports prefix listing (`list`) and value lookup (`get_value`)
- `Entry`: enum returned by `Database::list`, either `Entry::Key(String)` for full keys or `Entry::CommonPrefix(String)` for grouped prefixes
- `PayloadStoreBuilder<V, C = CborPayloadCodec>` and `PayloadStore<V, C = CborPayloadCodec>`: internal types for writing and reading the `.payload` file using the provided `PayloadCodec` (defaults to CBOR)

- `TagIndex`: read-only handle for querying a tag index file; supports listing tags (`list_tags`) and retrieving bitmap for a tag (`get`)
- `TagIndexBuilder`: builder for creating a new tag index file; insert lookup IDs with associated tags, then finalize to write the `.tags` file

### Design Notes
- The index uses the `fst` crate's `MapBuilder` to store keys with `u64` lookup identifiers as weights, each pointing into the lookup-FST mapping IDs to payload pointers.
- The lookup table is itself an on-disk FST mapping 32-bit big-endian lookup IDs to 64-bit payload pointers as weights, enabling atomic, checksum-validated writes and zero-copy memory-mapped queries.
- Payloads are encoded using the provided `PayloadCodec` (defaults to CBOR) and stored sequentially with a 2-byte little-endian length prefix
- Memory-mapped I/O enables zero-copy, zero-allocation prefix listing and fast value lookup
- Listings can be grouped by the first occurrence of a custom delimiter

## On-Disk Format
foliant produces up to four files per database (the tag index is optional):

- During index building, intermediate files `<base>.idx.tmp` and `<base>.lookup.tmp` are written in-place and then atomically renamed to `<base>.idx>` and `<base>.lookup>` once complete.

- `<base>.idx`: an on-disk FST index comprising one or more inlined sub-FST segments, each followed by a 16-byte trailer.  The trailer is a packed little-endian struct:
  ```text
  u32 seg_id | u64 start_off | u32 crc32(seg_id∥start_off)
  ```
  To read the index, `mmap` the file and scan backwards reading trailers from the end, validating each CRC, and extracting each sub-FST.  The segments are then merged in a single pass into the final lookup structure for queries.
- `<base>.lookup`: an on-disk FST mapping 32-bit big-endian lookup IDs to 64-bit payload pointers (encoded as FST weights). To read it, memory-map the file and use the `fst::Map` API (e.g. `map.get(&id.to_be_bytes())`) to fetch the payload file offset (stored as `offset+1` in the `.payload` file).
- `<base>.payload`: a flat file storing CBOR‑encoded payloads. The file begins with a 4-byte magic header (`FPAY`) and a 2-byte little-endian format version (`1` or `2`).
  - Format v1 is uncompressed; each payload record then consists of:
    1. A 2-byte little-endian length (`u16`)
    2. The CBOR‑encoded value bytes
  - Format v2 is compressed in zstd blocks of up to 128 KiB uncompressed; each block consists of:
    1. A 4-byte little-endian uncompressed length (`u32`)
    2. A 4-byte little-endian compressed length (`u32`)
    3. The zstd-compressed block data

- `<base>.tags`: (variant C) a monolithic tag index file embedding an FST mapping each tag string to a packed `(offset_in_blob<<32)|length` weight, followed by concatenated Roaring bitmap blobs.

The `.idx` file begins with the magic header and structure defined by the `fst` crate. This format enables fast, memory-mapped prefix queries and efficient payload retrieval with minimal allocations.