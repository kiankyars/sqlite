# Single-Column UNIQUE Index Execution

## Scope completed

Implemented execution support for single-column `CREATE UNIQUE INDEX` in `crates/ralph-sqlite`.

- `CREATE UNIQUE INDEX ...` now succeeds (single-column only).
- Backfill now validates existing table rows and rejects duplicate non-`NULL` values.
- `INSERT` and `UPDATE` now enforce unique-index constraints before writes.
- Constraint state now persists across reopen by re-parsing index SQL from schema metadata.

## Semantics

- Duplicate non-`NULL` values on a unique indexed column return:
  - `UNIQUE constraint failed: <table>.<column>`
- Multiple `NULL` values are allowed for unique indexes (matching SQLite behavior).
- Multi-row `UPDATE` validation simulates row-by-row unique state transitions:
  - Updates that create duplicate unique values fail without partial row changes.
  - Value handoff updates are allowed when an earlier row in statement order moves away from the value first.

## Implementation notes

- Added `unique: bool` to in-memory `IndexMeta`.
- `execute_create_index` now:
  - decodes table rows once,
  - validates uniqueness for unique indexes before creating index storage,
  - backfills index entries.
- Added preflight validators:
  - `validate_unique_constraints_for_insert_rows`
  - `validate_unique_constraints_for_updates`
- Added targeted lookup helper:
  - `unique_value_conflicts_with_existing`
- Added schema SQL parser helper:
  - `create_index_stmt_from_sql`

## Current limitations

- UNIQUE enforcement is only implemented for single-column indexes.
- No support for deferrable constraints or conflict-resolution clauses (`ON CONFLICT`).

## Tests added

In `crates/ralph-sqlite/src/lib.rs`:

- `create_unique_index_rejects_existing_duplicates`
- `insert_rejects_duplicate_value_for_unique_index`
- `unique_index_allows_multiple_null_values`
- `update_rejects_duplicate_for_unique_index_without_partial_changes`
- `update_rejects_statement_that_creates_duplicate_unique_values`
- `update_allows_unique_value_handoff_when_prior_row_moves_away`
- `unique_index_constraint_persists_across_reopen`
