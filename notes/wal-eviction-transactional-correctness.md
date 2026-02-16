# WAL Eviction Transactional Correctness Handoff

## Scope completed

Implemented transactional dirty-page eviction isolation in `crates/storage/src/pager.rs`:

- Added pager spill state: `spilled_dirty: HashMap<PageNum, Vec<u8>>`.
- Updated eviction (`maybe_evict`) so dirty LRU victims are moved into spill state instead of being written to the DB file.
- Updated page load (`ensure_loaded`) so spilled dirty pages are reloaded from memory before disk reads.
- Updated commit path (`flush_all`) so both in-pool dirty pages and spilled dirty pages are written to WAL + applied to DB atomically.

## Why this change

Before this change, dirty-page eviction could write uncommitted bytes directly to the DB file, which broke explicit transaction rollback guarantees when the buffer pool was under pressure.

After this change, uncommitted dirty bytes remain in memory until `commit()`/`flush_all()` and are not durable if the connection closes without commit.

## Tests added

In `crates/storage/src/pager.rs`:

- `dirty_evicted_page_remains_visible_before_commit`
  - Verifies an evicted dirty page is still visible within the same pager/session.
- `dirty_eviction_is_not_durable_without_commit`
  - Verifies evicted uncommitted bytes are not persisted across reopen when no commit occurs.

## Behavior notes / limitations

- Spill state is in-memory only, so very write-heavy long-running transactions can increase memory usage by accumulating evicted dirty pages.
