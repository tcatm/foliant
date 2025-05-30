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

### Build an index with tags extracted at index time
```bash
foliant index -i data.idx --json path --tag-field tags --input sample.jsonl
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
- `DatabaseBuilder<V>`: builder for creating a new on-disk database; insert keys with optional values (`V: Serialize`), then finalize to write the `.idx`, `.lookup`, and `.payload` files.
- `Database<V>`: read-only handle (`V: DeserializeOwned`) for querying the index; supports prefix listing (`list`) and value lookup (`get_value`)
- `Entry`: enum returned by `Database::list`, either `Entry::Key(String)` for full keys or `Entry::CommonPrefix(String)` for grouped prefixes
- `PayloadStoreBuilder<V>` and `PayloadStore<V>`: internal types for writing and reading the `.payload` file

- `TagIndex`: read-only handle for querying a tag index file; supports listing tags (`list_tags`) and retrieving bitmap for a tag (`get`)
- `TagIndexBuilder`: builder for creating a new tag index file; insert lookup IDs with associated tags, then finalize to write the `.tags` file

### Design Notes
- The index uses the `fst` crate's `MapBuilder` to store keys with `u64` lookup identifiers as weights, each pointing into the flat lookup table.
- Lookup table entries are fixed-size records mapping lookup IDs to payload pointers, allowing the table to be reordered independently of the FST.
- Payloads are encoded with CBOR and stored sequentially with a 2-byte little-endian length prefix
- Memory-mapped I/O enables zero-copy, zero-allocation prefix listing and fast value lookup
- Listings can be grouped by the first occurrence of a custom delimiter

## On-Disk Format
foliant produces up to four files per database (the tag index is optional):

- `<base>.idx`: an FST map file (using the `fst` crate) containing keys mapped to `u64` lookup identifiers. Each lookup ID is `index + 1` into the `.lookup` file.
- `<base>.lookup`: a flat file storing fixed-size lookup table entries (`LookupEntry`), each mapping to a payload pointer. Each entry is a 4-byte little-endian `u32` payload pointer (`offset+1` into the `.payload` file).
- `<base>.payload`: a flat file storing CBOR‑encoded payloads. The file begins with a 4-byte magic header (`FPAY`) and a 2-byte little-endian format version (`1`). Format v1 is uncompressed. Each payload record then consists of:

1. A 2-byte little-endian length (`u16`)
2. The CBOR‑encoded value bytes

- `<base>.tags`: (variant C) a monolithic tag index file embedding an FST mapping each tag string to a packed `(offset_in_blob<<32)|length` weight, followed by concatenated Roaring bitmap blobs.

The `.idx` file begins with the magic header and structure defined by the `fst` crate. This format enables fast, memory-mapped prefix queries and efficient payload retrieval with minimal allocations.