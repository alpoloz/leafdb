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

- Meta pages (page 0 and page 1)
- B+ tree leaf page
- B+ tree branch page
- Bucket header page
- Freelist page (overflow free page IDs)
- Overflow page (large values)

### File Layout Diagram

```
| Page 0 | Page 1 | Page 2 | Page 3 | ... |
| Meta0  | Meta1  | Data   | Data   | ... |

Meta pages (page 0 and page 1):
  - magic, page size, txid
  - root page id (top-level bucket index)
  - next page id
  - freelist

Root page:
  - Stored in meta, points to a B+ tree node (leaf/branch) for top-level buckets
  - keys = bucket names, values = bucket header page ids
```

### Meta Pages (page 0 and page 1)

LeafDB stores two meta pages and alternates between them on commit. Readers use
the meta page with the highest transaction ID for snapshot isolation.

```
Offset  Size  Field
0       4     Magic "LDB3"
4       4     Page size (uint32, little-endian)
8       8     TxID (uint64)
16      8     Root page ID (uint64) for top-level bucket index
24      8     Next page ID (uint64) for allocation
32      8     Freelist page ID (uint64) for overflow pages
40      4     Freelist count (uint32)
44      8*N   Freelist page IDs (uint64 each)

Older "LDB2" meta pages omit the freelist page pointer and place the freelist
count at offset 32 with IDs starting at offset 36.
```

### Bucket Header Page

Each bucket has a header page that points to its key/value tree and its
nested-bucket index tree.

```
Offset  Size  Field
0       1     Page type = 3 (bucket header)
1       8     KV tree root page ID (uint64)
9       8     Bucket index root page ID (uint64)
17      8     Bucket sequence (uint64)
```

### B+ Tree Pages

All B+ tree pages share a common header layout. The body differs for leaf and
branch pages.

```
Offset  Size  Field
0       1     Page type (1 = leaf, 2 = branch)
1       2     Key count (uint16)
3       8     Next leaf page ID (uint64, leaf only; 0 if none)
11      2     Reserved (padding)
13      ...   Body
```

Leaf body layout stores key/value pairs, each with length prefixes. If the
high bit of `ValLen` is set, the value is stored in overflow pages and the
inline value is an 8-byte overflow page ID.

```
KeyLen (uint16) | Key | ValLen (uint32) | Value or OverflowPageID (uint64)
```

Branch body layout stores child pointers first, followed by separator keys:

```
Child[0..N] (uint64 each), then Key[0..N-1] (uint16 + bytes)
```

### Freelist Pages

Freelist overflow pages store free page IDs when the inline freelist in the
meta page runs out of space.

```
Offset  Size  Field
0       1     Page type = 4 (freelist)
1       2     Entry count (uint16)
3       8     Next freelist page ID (uint64, 0 if none)
11      8*N   Free page IDs (uint64 each)
```

### Overflow Pages

Overflow pages store large values that do not fit within a single leaf page.

```
Offset  Size  Field
0       1     Page type = 5 (overflow)
1       8     Next overflow page ID (uint64, 0 if none)
9       ...   Raw value bytes
```

## Bucket Model

- Top-level buckets are stored in the root B+ tree.
- Each bucket uses two trees:
  - KV tree for key/value pairs.
  - Bucket index tree for nested buckets.
- Keys in bucket index trees map bucket name -> bucket header page ID.

## Transaction Model

- Single writer, multiple readers with snapshot isolation.
- Writer transactions take an exclusive lock and commit by writing new pages
  and then flipping the meta page (meta0/meta1).
- Read transactions pin the mmap during the transaction and use the meta
  snapshot chosen at Begin time.
- Freed pages are reusable only when no active reader can see them; pending
  frees are promoted to the freelist when their TxID is older than the oldest
  active reader.

## Implementation Decisions

- **Memory mapping**: Uses writable mmap for fast random access to pages and
  reliance on the OS page cache instead of maintaining an in-memory tree.
- **Copy-on-write pages**: Updates allocate new pages and never overwrite
  existing pages, enabling snapshot reads.
- **Separate trees per bucket**: Nested buckets are stored in a dedicated
  bucket-index tree rather than mixing bucket and KV keys.
- **Freelist in meta page**: Reused only when safe under MVCC by tracking
  active reader TxIDs and pending frees; overflow IDs are stored in freelist
  pages linked from the meta page.
- **Cursor iteration**: Implemented by walking branch paths to avoid reliance
  on mutable leaf links.
