# WAL Write Path + Commit Handoff

## Scope completed

Implemented task #15 in `crates/storage` with integration in `crates/ralph-sqlite`:

- Added new WAL module: `crates/storage/src/wal.rs`
- Added WAL sidecar file management (`<db-path>-wal`) on pager open
- Added WAL record format with checksums:
  - WAL header: `magic`, `version`, `page_size`
  - Page frame: `frame_type=1`, `txn_id`, `page_num`, `payload_len`, `checksum`, `payload`
  - Commit frame: `frame_type=2`, `txn_id`, `frame_count`, `checksum`
- Updated `Pager::flush_all()` to do write-ahead commit flow:
  1. Stage dirty header page (page 0) if in-memory header changed
  2. Append dirty page frames to WAL
  3. Append commit frame and `fsync` WAL
  4. Apply those pages to DB file and `fsync` DB
- Added `Pager::commit()` as an explicit alias for commit semantics.
- Updated SQL write paths to use `pager.commit()`:
  - `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE` in `crates/ralph-sqlite/src/lib.rs`

## Tests added

In `crates/storage/src/pager.rs`:

- `flush_writes_wal_page_frames_and_commit_record`
  - Parses WAL bytes and verifies:
    - WAL magic/header
    - Page-frame checksums
    - Commit-frame checksum
    - Commit frame count matches number of page frames
- `multiple_flushes_append_multiple_wal_transactions`
  - Confirms subsequent `flush_all()` calls append more WAL bytes.

## Behavior notes / limitations

- Dirty-page eviction from a full buffer pool still writes pages directly to the DB file.
  - WAL is guaranteed for explicit commit/flush path.
  - Full transactional buffering of evicted dirty pages is deferred to future transaction work.
- WAL replay/checkpoint are not implemented in this task (covered by tasks #16 and #17).
