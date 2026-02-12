# Transactions and Write-Ahead Logging

This document describes the ACID transaction implementation and the Write-Ahead Log (WAL) in ralph-sqlite.

## Transaction Model

ralph-sqlite supports explicit transactions via `BEGIN`, `COMMIT`, and `ROLLBACK`.

- **Autocommit**: By default, every SQL statement runs in its own implicit transaction.
- **Explicit Transactions**: `BEGIN` disables autocommit and snapshots the catalog metadata. `COMMIT` flushes changes to the WAL and DB file. `ROLLBACK` reopens the pager and restores the catalog snapshot to discard uncommitted changes.
- **Isolation**: Single-writer, multiple-reader via WAL.

## Write-Ahead Logging (WAL)

All modifications are recorded in a sidecar WAL file (`<db-path>-wal`) before being applied to the main database file. This ensures atomic commits and crash recovery.

### WAL Format
The WAL is a sequence of variable-length records:
1. **Header**: Magic number, version, page size.
2. **Page Frames**: Contains `frame_type=1`, `txn_id`, `page_num`, `payload_len`, `checksum`, and the raw page bytes.
3. **Commit Frame**: Contains `frame_type=2`, `txn_id`, `frame_count`, and a `checksum`. It marks the end of a durable transaction.

### Atomic Commit Path (`flush_all` / `commit`)
1. Stage any changes to the FileHeader (Page 0).
2. Append all dirty page frames to the WAL.
3. Append a commit frame to the WAL and `fsync` the WAL file.
4. (Optional) Apply the dirty pages to the main DB file and `fsync` the DB file.

### Recovery and Checkpoint
- **Recovery**: On `Pager::open`, the engine scans the WAL. It replays only **fully committed** transactions (those with a valid commit frame and matching checksums) into the main DB file. Any trailing uncommitted frames are ignored. After replay, the WAL is truncated.
- **Checkpoint**: Copies all committed WAL frames into the main database file and truncates the WAL. This is currently triggered manually via the Pager API.

## Transactional Correctness

### Dirty Eviction (Spill Map)
When the buffer pool is full and a dirty page must be evicted:
- Instead of writing the uncommitted dirty page to the main DB file, the pager "spills" it to an in-memory `spilled_dirty: HashMap<PageNum, Vec<u8>>`.
- Subsequent reads for that page are served from the spill map.
- This ensures that uncommitted data **never** hits the main database file until an explicit `COMMIT`, preserving `ROLLBACK` correctness.

### Checksums
Every WAL frame includes a checksum. During recovery, frames with invalid checksums or truncated payloads are discarded to ensure that only intact, durable data is replayed.
