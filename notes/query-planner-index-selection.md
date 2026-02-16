# Query Planner Index Selection (Task #14)

## Scope completed

Implemented a minimal planner-driven index-selection path for SELECT queries and integrated it with `ralph-sqlite` execution:

- Replaced planner stub with a concrete `plan_select` API in `crates/planner/src/lib.rs`.
- Added planner access-path model:
  - `AccessPath::TableScan`
  - `AccessPath::IndexEq { index_name, column, value_expr }`
- Planner now recognizes indexable predicates:
  - `indexed_col = <constant_expr>`
  - `<constant_expr> = indexed_col`
  - equality terms nested under `AND`.

## Execution integration

In `crates/ralph-sqlite/src/lib.rs`:

- SELECT execution now calls planner with table/index metadata.
- If planner returns `IndexEq`, execution:
  1. evaluates the planned constant expression,
  2. probes the secondary-index B+tree for matching rowids,
  3. fetches table rows by rowid,
  4. re-applies full `WHERE` predicate for correctness.
- If planner returns `TableScan` (or planned index metadata is missing), execution falls back to full table scan.

## Index-consistency follow-up included

To keep index-based reads correct after writes, this task also added secondary-index maintenance for:

- `UPDATE`: remove old row from index buckets, update table row, then insert new index entry.
- `DELETE`: remove row from index buckets before deleting table row.

This closes a previous correctness gap where only `INSERT` maintained index entries.

## Tests added

- Planner unit tests (`crates/planner/src/lib.rs`):
  - equality predicate chooses `IndexEq`
  - reversed equality chooses `IndexEq`
  - `AND` extraction chooses `IndexEq`
  - non-indexed / non-constant cases fall back to `TableScan`
- Integration tests (`crates/ralph-sqlite/src/lib.rs`):
  - `update_maintains_secondary_index_entries`
  - `delete_maintains_secondary_index_entries`

## Current limitations

- Planner only handles single-table equality predicates on single-column secondary indexes.
- No range planning (`<`, `<=`, `BETWEEN`), `OR` index planning, join planning, or cost-based ranking.
- Table and index catalogs remain connection-local pending schema-table persistence (task #8).
