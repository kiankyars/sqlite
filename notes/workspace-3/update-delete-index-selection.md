# UPDATE/DELETE Index Selection

## Scope completed

Added planner-driven index selection for UPDATE and DELETE statements, so they can use secondary indexes instead of always performing full table scans.

## Changes

### Planner (`crates/planner/src/lib.rs`)

- Added `plan_where(where_clause, table_name, indexes) -> AccessPath` — a general-purpose entry point for planning access paths from arbitrary WHERE clauses (not tied to `SelectStmt`)
- The existing `choose_index_access` internal function is reused; `plan_where` is a thin wrapper
- Added 3 planner unit tests for `plan_where`

### Integration (`crates/ralph-sqlite/src/lib.rs`)

- Added `Database::read_candidate_entries(meta, access_path) -> Vec<Entry>` helper
  - `AccessPath::TableScan`: delegates to `BTree::scan_all()` (unchanged behavior)
  - `AccessPath::IndexEq`: probes the secondary index B+tree for matching rowids, then fetches individual table rows by rowid lookup
- Updated `execute_update` to call `plan_where()` and `read_candidate_entries()` instead of unconditional `scan_all()`
- Updated `execute_delete` to call `plan_where()` and `read_candidate_entries()` instead of unconditional `scan_all()`
- Full WHERE predicate re-evaluation is still applied after index-driven row fetching for correctness (index may over-select due to hash collisions or compound AND predicates)

## Tests added

- `plan_where_returns_table_scan_without_where` (planner)
- `plan_where_chooses_index_for_equality` (planner)
- `plan_where_falls_back_for_non_indexed_column` (planner)
- `update_uses_index_for_where_predicate` (integration)
- `delete_uses_index_for_where_predicate` (integration)
- `update_with_indexed_column_change_maintains_index` (integration)

## Current limitations

- Only equality predicates on single-column indexes are eligible for index-driven UPDATE/DELETE
- Range predicates, OR, and multi-column indexes are not planned
- No cost-based planning — first eligible index is always used
