# Multi-Column Secondary Index Execution

## Scope completed

Implemented execution support for multi-column secondary indexes in `crates/ralph-sqlite`.

- `CREATE INDEX ... ON t(c1, c2, ...)` now executes for multi-column index definitions.
- `CREATE UNIQUE INDEX ... ON t(c1, c2, ...)` now enforces uniqueness on full tuples.
- INSERT/UPDATE/DELETE index maintenance now handles tuple keys for multi-column indexes.
- Catalog load/reopen now restores multi-column index metadata and unique behavior.

## Implementation details

- In-memory `IndexMeta` now stores ordered `columns` + `column_indices` (instead of one column).
- `Schema::create_index` is called with full index-column metadata (`&[(String, u32)]`).
- Multi-column index entries use:
  - B+tree key: FNV-1a hash of deterministic tuple encoding.
  - Bucket identity value: tagged text payload containing the encoded tuple bytes.
- Single-column indexes keep the previous scalar key/value path (including ordered numeric/text keys for range plans).

## UNIQUE semantics

- UNIQUE checks are tuple-based for multi-column indexes.
- If any indexed column in a row is `NULL`, uniqueness checks are skipped for that row (SQLite-compatible behavior).
- Error message format for multi-column violations matches SQLite style:
  - `UNIQUE constraint failed: table.col1, table.col2`

## Planner safety

- Planner-facing index metadata remains single-column only for now (`planner_indexes_for_table` filters out multi-column indexes).
- This avoids incorrect `IndexEq`/`IndexRange` use until multi-column predicate planning is implemented.

## Tests added

In `crates/ralph-sqlite/src/lib.rs`:

- `create_multi_column_index_backfills_existing_rows`
- `insert_updates_multi_column_index`
- `create_unique_multi_column_index_rejects_existing_duplicates`
- `multi_column_unique_allows_null_values`
- `update_rejects_duplicate_for_multi_column_unique_index`
- `unique_multi_column_index_constraint_persists_across_reopen`

In `crates/storage/src/schema.rs`:

- Updated schema index tests to use `Schema::create_index(..., columns, ...)`.

## Remaining follow-up

- Query planner still does not choose multi-column indexes.
- Multi-column range seeks are not yet implemented.
