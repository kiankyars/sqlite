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
pager.allocate_page()            // -> PageNum (extends file)
pager.pin(page_num) / unpin()    // Prevent/allow LRU eviction
pager.flush_all()                // Write all dirty pages + header to disk
pager.header() / header_mut()    // Access the FileHeader
```

## Design Decisions

- Buffer pool uses `HashMap<PageNum, Frame>` with LRU eviction (monotonic counter).
- Dirty pages are flushed on eviction or explicit `flush_all()`.
- Page allocation always extends the file; freelist reuse is deferred.
- `flush_header()` updates the header both in the pool (if cached) and on disk.

## What's Next

- **Freelist**: `allocate_page` should check `header.freelist_head` before extending.
- **B+tree**: Will use `read_page`/`write_page` for node I/O. Interior and leaf page formats are defined in DESIGN.md.
- **Schema table**: Root page number stored in `header.schema_root`.
