# Developer Guide

This crate implements a compressed (radix) trie with a binary‐searchable on‐disk format plus an optional memory‐mapped reader for fast, zero‐alloc prefix searches.

## Build & Test
Requires Rust 1.XX+.  From the repo root:
```bash
cargo build --release      # produce `target/release/index`
cargo test                 # run all unit + integration tests
```

## CLI Usage Examples

Build an index from plain text lines:
```bash
index index -i data.idx input.txt
```

Build an index from JSON objects (one per line), using field `id` as the key and storing the remaining object as CBOR values:
```bash
index index -i data.idx -j key records.jsonl
```

List keys (and values) in an existing index, optionally grouping by delimiter:
```bash
index list -i data.idx                # list all keys
index list -i data.idx prefix         # list keys under prefix
index list -i data.idx -d / prefix    # group at first '/'
```

When values are present, they are shown after the key in dim ANSI color, serialized as one-line JSON.

## Repository Layout
- `src/lib.rs`: core in‐memory `Trie` + `MmapTrie` implementations
- `src/main.rs`: CLI wrapper (`index` binary)
- `tests/`: unit and integration tests covering insertion, grouping, serialization, and lazy mmap iteration
- `Cargo.toml` / `Cargo.lock`: crate metadata and dependencies

## On‐Disk Format
All serialized indexes begin with a 4‐byte magic header:
```text
  [0..4) = "IDX1"
```
Each node is then written in pre‐order:
1. Node header (3 bytes):
   - `is_end` (1 byte, 0 or 1)
   - `child_count` (u16 little‐endian)
2. TLV-encoded CBOR value (if any):
   - `tag` (1 byte): `0` = no value, `1` = CBOR payload follows
   - `length` (u32 LE): number of payload bytes (0 if tag=0)
   - `[payload bytes]` (CBOR-encoded data)
3. Index table (`child_count` entries, 9 bytes each):
   - `first_byte` (u8)
   - `child_offset` (u64 LE): absolute offset of the child blob
4. Child blobs, in the same sorted order:
   - `label_len` (u16 LE)
   - `label` (UTF‐8 bytes)
   - recursive subtree for that child

Constants in code:
```rust
const MAGIC: [u8;4] = *b"IDX1";
const HEADER_LEN: usize = 4;
const NODE_HEADER_LEN: usize = 1 + 2;
const INDEX_ENTRY_LEN: usize = 1 + 8;
const LABEL_LEN_LEN: usize = 2;
```

## Key Types
- `TrieNode`: in‐memory node with `Vec<(String,Box<TrieNode>)>` and an `is_end` flag
- `Trie`: owns a `root: TrieNode`; supports `insert`, `list_iter`, `write_radix`, `read_radix`
- `MmapTrie`: owns a single `Mmap` buffer; supports `load`, `list_iter`, `list` via `MmapTrieGroupIter`
- `MmapTrieGroupIter`: depth‐first, zero‐alloc iteration over the mmap buffer, doing binary‐search on the index table at each level