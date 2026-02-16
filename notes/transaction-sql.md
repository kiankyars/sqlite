# BEGIN/COMMIT/ROLLBACK SQL Handoff

## Scope completed

Implemented task #17 across parser + integration (`crates/parser`, `crates/ralph-sqlite`):

- Added AST statement variants:
  - `Stmt::Begin`
  - `Stmt::Commit`
  - `Stmt::Rollback`
- Added parser support for:
  - `BEGIN;`
  - `BEGIN TRANSACTION;`
  - `COMMIT;`
  - `COMMIT TRANSACTION;`
  - `ROLLBACK;`
  - `ROLLBACK TRANSACTION;`
- Added transaction execution flow in `Database`:
  - `BEGIN` starts explicit transaction mode and snapshots connection-local catalogs (`tables`, `indexes`)
  - write statements (`CREATE TABLE`, `CREATE INDEX`, `INSERT`, `UPDATE`, `DELETE`) no longer auto-commit while explicit transaction is active
  - `COMMIT` flushes dirty pages via `Pager::commit()` and exits explicit transaction mode
  - `ROLLBACK` reopens pager from DB path (discarding in-memory uncommitted pages) and restores catalog snapshot

## Tests added

Parser tests:
- `test_begin_transaction`
- `test_commit_transaction`
- `test_rollback_transaction`
- plus parser crate-level parse tests for `BEGIN`, `COMMIT TRANSACTION`, `ROLLBACK`

Integration tests in `crates/ralph-sqlite/src/lib.rs`:
- `explicit_transaction_delays_wal_until_commit`
  - verifies WAL file length does not change during explicit transaction writes and grows on `COMMIT`
- `rollback_discards_uncommitted_transaction_changes`
  - verifies table created in explicit transaction disappears after `ROLLBACK`
- `transaction_state_errors_are_reported`
  - verifies `COMMIT`/`ROLLBACK` without active transaction and nested `BEGIN` return errors

## Behavior notes / limitations

- Catalog persistence is still pending task #8, so transaction catalog rollback/commit behavior is currently connection-local only.
- Dirty-page eviction still writes directly to DB file in pager internals; rollback is reliable for buffered uncommitted changes but cannot undo already-evicted dirty writes.
