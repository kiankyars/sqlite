# WAL Recovery + Checkpoint Handoff

## Scope completed

Implemented task #16 in `crates/storage`:

- Added WAL crash recovery on pager startup:
  - `Pager::open*()` now calls WAL recovery before returning.
  - Recovery replays committed WAL page frames into the DB file and truncates WAL.
- Added startup header refresh after recovery:
  - `Pager::open*()` re-reads page 0 header after replay so header metadata (`page_count`, `schema_root`, etc.) matches recovered state.
- Added explicit checkpoint API:
  - `Pager::checkpoint() -> io::Result<usize>`
  - Flushes pending dirty pages first (to preserve commit semantics), then checkpoints committed WAL frames and truncates WAL.

## Storage behavior notes

- WAL recovery applies only fully committed transactions (page frames followed by a valid commit frame).
- Truncated or checksum-invalid tail frames are ignored; earlier committed transactions are still replayed.
- Recovery truncates WAL back to the header after replay, making startup idempotent.

## Tests added

In `crates/storage/src/pager.rs`:

- `open_recovers_committed_wal_frames`
  - Verifies committed WAL-only updates are applied when reopening.
- `recovery_ignores_uncommitted_wal_tail`
  - Verifies trailing uncommitted page frames are ignored on recovery.
- `checkpoint_truncates_wal_and_preserves_data`
  - Verifies `Pager::checkpoint()` truncates WAL and preserves durable page contents.
- `open_reloads_header_after_wal_recovery`
  - Verifies recovered page-0 header state is reflected in `Pager::header()` after open.

## Remaining limitations

- Dirty-page eviction can still flush directly to DB outside explicit commit/checkpoint paths.
- No SQL-level `CHECKPOINT` statement yet; checkpoint is currently a pager API.
