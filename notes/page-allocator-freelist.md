# Pager Handoff: Page Allocator Freelist Stub

## What was implemented

- Updated `Pager::allocate_page()` in `crates/storage/src/pager.rs` to:
  - Reuse a page from the freelist when `header.freelist_head != 0`.
  - Read the freelist next pointer from bytes `0..4` of the freelist head page (big-endian `u32`).
  - Validate that `freelist_head` and the next pointer are in range (`0` or `< page_count`).
  - Zero the reused page before returning it.
  - Decrement `freelist_count` with saturating behavior.
  - Fall back to extending the file when freelist is empty.

## Tests added

- `allocate_reuses_freelist_before_extension`
- `allocate_zeroes_reused_freelist_page`

## Behavior notes

- This is a freelist **stub**: there is still no public `free_page()` API yet.
- The freelist chain format used by the allocator is currently simple and internal:
  - free page first 4 bytes = next freelist page number (or `0` if end).
