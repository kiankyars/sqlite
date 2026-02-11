# SELECT Aggregate Execution Handoff

## Scope completed

Implemented task #19 aggregate slice in `crates/ralph-sqlite/src/lib.rs`:

- Added aggregate SELECT execution for `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`.
- Aggregate mode is enabled when SELECT projection expressions contain aggregate functions.
- Aggregate queries now produce a single result row (no `GROUP BY` support yet).

## Semantics implemented

- `COUNT(*)` counts input rows after `WHERE` filtering.
- `COUNT(expr)` counts non-`NULL` values.
- `SUM(expr)` / `AVG(expr)` ignore `NULL`; empty input returns `NULL`.
- `MIN(expr)` / `MAX(expr)` ignore `NULL`; empty input returns `NULL`.
- Aggregate expressions are allowed (example: `COUNT(*) + 1`).
- Aggregate queries without `FROM` operate on SQLite's single pseudo-row model:
  - `WHERE` true => one input row
  - `WHERE` false => zero input rows

## Guardrails and current limits

- `GROUP BY` / `HAVING` are not implemented.
- In aggregate SELECT mode, bare column references outside aggregate functions are rejected.
- `SELECT *` is rejected in aggregate mode.
- Aggregate functions in `WHERE` are rejected.
- Nested aggregate calls are rejected.

## Tests added

In `crates/ralph-sqlite/src/lib.rs`:

- `select_aggregate_functions_with_where_and_nulls`
- `select_aggregate_functions_over_empty_input`
- `select_aggregate_without_from_respects_where`
- `select_mixed_aggregate_and_column_without_group_by_errors`

Behavior was cross-checked with `sqlite3` for:

- NULL behavior on empty aggregate input
- `COUNT(*)` semantics with and without `FROM`
- aggregate expression composition (`COUNT(*) + 1`)
