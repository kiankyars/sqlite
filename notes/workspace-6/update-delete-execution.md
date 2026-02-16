# UPDATE/DELETE Execution Handoff

## Scope completed

Task #12 is now implemented end-to-end in `crates/ralph-sqlite/src/lib.rs`:

- `Database::execute` now dispatches parsed `UPDATE` and `DELETE` statements.
- Added execution handlers:
  - `execute_update(UpdateStmt)` returns `ExecuteResult::Update { rows_affected }`
  - `execute_delete(DeleteStmt)` returns `ExecuteResult::Delete { rows_affected }`
- `WHERE` filtering for UPDATE/DELETE reuses existing expression evaluation.
- UPDATE assignments are evaluated against the original row image, then applied.
- DELETE uses `BTree::delete(key)` for each qualifying row key.

## Tests added

Added integration-focused tests in `crates/ralph-sqlite/src/lib.rs`:

- `update_with_where_updates_matching_rows`
- `delete_with_where_removes_matching_rows`
- `update_and_delete_without_where_affect_all_rows`

These pass under `cargo test --workspace`.

## Behavior notes

- UPDATE/DELETE currently execute as full table scans in key order.
- B+tree delete remains **non-rebalancing** at storage level (no merge/redistribute; task #18).
- Schema/table metadata remains connection-local pending task #8.

## Suggested next steps

1. Persist table metadata via schema-table storage (task #8) so UPDATE/DELETE work across reopen.
2. Integrate planner/index selection to avoid full scans once indexes exist (tasks #13-14).
