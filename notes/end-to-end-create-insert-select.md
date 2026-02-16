# End-to-End CREATE/INSERT/SELECT Handoff

## Scope completed

Implemented a minimal SQL execution path in `crates/ralph-sqlite/src/lib.rs`:

- `Database::open(path)` opens pager-backed storage.
- `Database::execute(sql)` parses SQL via `ralph-parser` and dispatches:
  - `CREATE TABLE`
  - `INSERT`
  - `SELECT`

Added execution result/value types:

- `ExecuteResult::{CreateTable, Insert { rows_affected }, Select(QueryResult)}`
- `Value::{Null, Integer, Real, Text}`

## Storage behavior

- Each created table gets its own `ralph-storage::BTree` root page.
- Inserted rows are stored as encoded payloads in the table B+tree.
- Row payload encoding format:
  - `u32` column_count (big-endian)
  - per column: `tag + bytes`
    - `0`: NULL
    - `1`: i64 integer
    - `2`: f64 bits
    - `3`: text (`u32 len` + UTF-8 bytes)
- Rowid keys are assigned by scanning current table rows and appending `max(rowid)+1`.

## Query behavior implemented

- `INSERT` supports optional column list; unspecified columns are filled with `NULL`.
- `SELECT` supports:
  - `SELECT * FROM table`
  - projected expressions and aliases
  - simple `WHERE` expression evaluation
  - `LIMIT` / `OFFSET` (integer expressions)
- `ORDER BY` currently returns a "not supported yet" error.
- Statements other than CREATE/INSERT/SELECT still return "not supported yet."

## Important limitation

- Table catalog is **connection-local** for now (in-memory map in `Database`).
- This intentionally avoids overlap with active task #8 (`Schema table storage`).
- Re-opening a DB file does not yet reconstruct table metadata.

## Tests added

In `crates/ralph-sqlite/src/lib.rs`:

- `create_insert_select_roundtrip`
- `insert_with_column_list_fills_missing_with_null`
- `select_literal_without_from`

All workspace tests pass after these changes.
