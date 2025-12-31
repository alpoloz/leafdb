# LeafDB Design

This document describes the on-disk layout, page formats, and key implementation
choices, along with alternatives considered.

## File Format

LeafDB stores data in a single file of fixed-size pages. The file is memory
mapped for fast access. Pages are addressed by page ID and located at:

```
file_offset = page_id * page_size
```

### Page Types

- Meta page (page 0)
- B+ tree leaf page
- B+ tree branch page
- Bucket header page

### File Layout Diagram

```
| Page 0 | Page 1 | Page 2 | Page 3 | ... |
| Meta   | Root   | Free   | Data   | ... |

Meta page (page 0):
  - magic, page size
  - root page id (top-level bucket index)
  - next page id
  - freelist

Root page (page 1):
  - B+ tree node (leaf/branch) for top-level buckets
  - keys = bucket names, values = bucket header page ids
```

### Meta Page (page 0)

The meta page holds file-level metadata and the freelist.

```
Offset  Size  Field
0       4     Magic "LDBM"
4       4     Page size (uint32, little-endian)
8       8     Root page ID (uint64) for top-level bucket index
16      8     Next page ID (uint64) for allocation
24      4     Freelist count (uint32)
28      8*N   Freelist page IDs (uint64 each)
```

### Bucket Header Page

Each bucket has a header page that points to its key/value tree and its
nested-bucket index tree.

```
Offset  Size  Field
0       1     Page type = 3 (bucket header)
1       8     KV tree root page ID (uint64)
9       8     Bucket index root page ID (uint64)
```

### B+ Tree Pages

All B+ tree pages share a common header layout. The body differs for leaf and
branch pages.

```
Offset  Size  Field
0       1     Page type (1 = leaf, 2 = branch)
1       2     Key count (uint16)
3       8     Next leaf page ID (uint64, leaf only; 0 if none)
11      ...   Body
```

Leaf body layout stores key/value pairs, each with length prefixes:

```
KeyLen (uint16) | Key | ValLen (uint32) | Value
```

Branch body layout stores child pointers first, followed by separator keys:

```
Child[0..N] (uint64 each), then Key[0..N-1] (uint16 + bytes)
```

## Bucket Model

- Top-level buckets are stored in the root B+ tree.
- Each bucket uses two trees:
  - KV tree for key/value pairs.
  - Bucket index tree for nested buckets.
- Keys in bucket index trees map bucket name -> bucket header page ID.

## Transaction Model

- Single writer, multiple readers.
- Writer transactions take an exclusive lock and apply changes to mmap pages
  on commit.
- Read transactions take a shared lock and read directly from mmap.

## Implementation Decisions

- **Memory mapping**: Uses writable mmap for fast random access to pages and
  reliance on the OS page cache instead of maintaining an in-memory tree.
- **Page-based B+ tree**: Node splits allocate new pages; updates rewrite
  affected pages in place during commit.
- **Separate trees per bucket**: Nested buckets are stored in a dedicated
  bucket-index tree rather than mixing bucket and KV keys.
- **Freelist in meta page**: Simplifies allocation and deallocation, but is
  limited by meta page capacity.
- **Cursor iteration**: Implemented by walking leaf pages via the next pointer.

## Alternatives and Tradeoffs

- **Copy-on-write (CoW) pages**: Safer crash consistency and snapshots, but
  requires more complex page management and space reclamation.
- **Write-ahead log (WAL)**: Adds durability guarantees and crash recovery at
  the cost of more I/O and log compaction logic.
- **Append-only B+ tree**: Simplifies writes and recovery but increases file
  size without compaction.
- **Separate files per bucket**: Simplifies bucket isolation but complicates
  transactions and increases file management overhead.
- **Freelist as a tree**: Removes meta page size limits but adds complexity to
  page allocation.
- **In-memory caching layer**: Improves hot reads but increases complexity and
  memory usage.
