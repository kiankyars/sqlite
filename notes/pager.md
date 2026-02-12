# Pager Notes

## File Layout

- **Page 0**: Contains the 100-byte `FileHeader` at the start, followed by unused space to fill `page_size`.
- Pages are addressed by `PageNum` (u32, 0-based).
- All integers in the header are big-endian.

## API Summary

```rust
Pager::open(path)                // Open or create a database file
Pager::open_with_pool_size(path, n) // Open with custom buffer pool capacity
pager.read_page(page_num)        // -> &[u8]
pager.write_page(page_num)       // -> &mut [u8], marks dirty
pager.allocate_page()            // -> PageNum (reuses freelist or extends file)
pager.free_page(page_num)        // -> Add page to freelist for reuse
pager.pin(page_num) / unpin()    // Prevent/allow LRU eviction
pager.flush_all()                // Write all dirty pages + header to disk
pager.header() / header_mut()    // Access the FileHeader
```

## Design Decisions

- **Buffer pool**: Uses `HashMap<PageNum, Frame>` with LRU eviction (monotonic counter).
- **Dirty pages**: Flushed on eviction or explicit `flush_all()`.
- **Page allocation**:
  - Reuses a page from the freelist when `header.freelist_head != 0`.
  - The freelist is a linked list where each free page's first 4 bytes contain the next free page number (big-endian `u32`).
  - Reused pages are zeroed before being returned.
  - Falls back to extending the file when the freelist is empty.
- **Page reclamation**:
  - `free_page(page_num)` adds a page to the front of the freelist.
  - Rejects invalid (page 0, out-of-range) or duplicate frees (traverses chain).
  - Zeroes page payload before relinking.
- **Header persistence**: `flush_header()` updates the header both in the pool (if cached) and on disk.

## Implementation Details

- **Freelist format**:
  - `header.freelist_head` (u32): First page in the freelist.
  - `header.freelist_count` (u32): Total number of free pages.
  - Free page: bytes `0..4` = next free page number (or `0` for end of list).
- **WAL Integration**: Changes to pages and the header are marked dirty, allowing WAL-backed atomic commits.
