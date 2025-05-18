# Developer Guide

This crate implements a compressed (radix) trie with a binary‐searchable on‐disk format plus an optional memory‐mapped reader for fast, zero‐alloc prefix searches.

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
2. Index table (`child_count` entries, 9 bytes each):
   - `first_byte` (u8)
   - `child_offset` (u64 LE): absolute offset of the child blob
3. Child blobs, in the same sorted order:
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

## Build & Test
Requires Rust 1.XX+.  From the repo root:
```bash
cargo build --release      # produce `target/release/index`
cargo test                 # run all unit + integration tests
cargo fmt -- --check       # ensure formatting
cargo clippy -- -D warnings # catch lints
```

## Design Notes
- **Magic header** avoids brittle heuristics when detecting the indexed format.
- **Binary‐searchable index** in each node gives O(log k) descent per level (versus O(k) scans).
- **Zero‐alloc mmap reader** (`MmapTrie`) lets you handle massive indexes without deserializing the full tree.
- **Grouping iterator** (`TrieGroupIter` / `MmapTrieGroupIter`) yields either full keys or grouped prefixes up to a delimiter.

## Contribution Guidelines
- Fork and branch (e.g. `feature/your-change`) before submitting a PR.
- Add examples/tests for any new feature or bug fix under `tests/`.
- Keep changes minimal and in the style of existing code.

---
This README is intended for maintainers and contributors.  See `--help` in the `index` binary for end‐user CLI documentation.
