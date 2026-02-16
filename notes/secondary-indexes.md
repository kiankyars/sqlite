# Secondary Indexes (`CREATE INDEX`) Handoff

## Scope completed

Implemented a minimal secondary-index path in parser + integration layer:

- Added parser AST and syntax support for:
  - `CREATE INDEX ... ON table(col, ...)`
  - `CREATE UNIQUE INDEX ...` (parsed, but execution rejects UNIQUE for now)
  - `IF NOT EXISTS`
- Added `Database::execute` support for `Stmt::CreateIndex`.
- Added in-memory index metadata in `Database` and on-disk index storage using `ralph_storage::BTree`.

## Runtime behavior

- `CREATE INDEX idx ON t(col)` allocates a new B+tree root page for the index.
- Index creation backfills existing table rows by scanning the table B+tree.
- New `INSERT` rows are also written into every index defined on that table.
- Index payload format supports duplicates and hash collisions:
  - key: stable FNV-1a hash of encoded index value
  - payload: bucketed list of `(exact_value, rowid_list)`

## Current limitations

- Only single-column index execution is supported.
- `UNIQUE` indexes are not executed yet (returns an explicit error).
- Index metadata is still connection-local (same as table catalog); persistence is pending schema task #8.
- SELECT planning still does full table scans; index selection is pending task #14.

## Tests added

- Parser tests:
  - `test_create_index`
  - `test_create_unique_index_if_not_exists`
  - `test_parse_create_index`
- Integration tests in `crates/ralph-sqlite/src/lib.rs`:
  - `create_index_backfills_existing_rows`
  - `insert_updates_secondary_index`
