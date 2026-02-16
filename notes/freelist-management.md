# Pager Handoff: Freelist Management (`free_page`)

## What was implemented

- Added `Pager::free_page(page_num)` in `crates/storage/src/pager.rs`.
- `free_page` now:
  - rejects invalid frees (`page_num == 0` and out-of-range page ids),
  - rejects duplicate frees by traversing the freelist chain,
  - links the freed page at freelist head (`page[0..4] = old_head`),
  - zeroes the freed page payload before relinking,
  - updates `header.freelist_head` / `header.freelist_count`, and
  - marks header/page state dirty for WAL-backed commit/flush.

## Tests

Added/updated pager unit tests:
- `free_page_adds_to_freelist_and_allocate_reuses_it`
- `free_page_rejects_invalid_and_duplicate_pages`
- `free_page_persists_across_reopen`

## Notes / limitations

- This task provides the pager-level free-page primitive only.
- Higher layers (B+tree/schema/index lifecycle) still need to call `free_page` when they physically drop or recycle pages.
