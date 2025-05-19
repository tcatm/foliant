<!--
# foliant

A compressed radix trie with binary-searchable on-disk format and optional memory-mapped reader for fast, zero-allocation prefix searches and CBOR-encoded leaf values.

Supported features:
  - In-memory insertable, compressed radix trie
  - On-disk serialization format with binary-searchable index tables
  - `MmapTrie`: zero-copy, zero-allocation prefix listing via memory-mapped file
  - CBOR-encoded leaf values stored as TLV (tag-length-value) blocks
  - Grouped listings by custom delimiters
  - CLI tool `foliant` with `index` and `list` commands
-->

# Table of Contents
1. [Build & Test](#build--test)
2. [CLI Usage Examples](#cli-usage-examples)
3. [Generating Sample JSONL Input](#generating-sample-jsonl-input)
4. [Developer Guide](#developer-guide)
   - [Repository Layout](#repository-layout)
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
foliant index -i data.idx input.txt
```

### Build an index from JSON lines
```
foliant index -i data.idx -j path sample.jsonl
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

### Repository Layout
- `src/lib.rs`: core `Trie<V>` and `MmapTrie<V>` implementations
- `src/main.rs`: CLI (`foliant`) using `clap`
- `tests/`: unit & integration tests
- `Cargo.toml` / `Cargo.lock`: dependencies and metadata

### Key Types
- `TrieNode<V>`: in-memory node with `children: Vec<(String, Box<TrieNode<V>>)>`, `is_end`, and optional `value: Vec<u8>`
- `Trie<V>`: owns root node, supports `insert`, `insert_with_value`, `list`, `write_index`, `read_radix`
- `MmapTrie<V>`: wraps an `Mmap` buffer, supports `load`, `list`, and `get_value`
- Generic `TrieBackend` + `GenericTrieIter` for unified traversal

### Design Notes
- Edge labels are stored in a compressed (radix) trie, splitting only on partial matches
- On-disk format is binary-searchable by sorting children on first byte
- Mmap-based reader avoids heap allocations and supports large indexes efficiently

## On-Disk Format
All indexes begin with the 4-byte magic header:
```text
[0..4) = "IDX1"
```
Each node is serialized in pre-order:
1. Header (3 bytes):
   - `is_end` (1 byte)
   - `child_count` (u16 LE)
2. TLV CBOR value:
   - `tag` (1 byte): 0=no value, 1=has CBOR
   - `length` (u32 LE)
   - `value_bytes` (CBOR)
3. Index table (`child_count` × 9 bytes):
   - `first_byte` (u8)
   - `child_offset` (u64 LE)
4. Child blobs (sorted by first byte):
   - `label_len` (u16 LE)
   - `label` (UTF-8)
   - subtree nodes...