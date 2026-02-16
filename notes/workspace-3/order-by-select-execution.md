# SELECT ORDER BY Execution Handoff

## Scope completed

Implemented task #19 slice (`ORDER BY`) in `crates/ralph-sqlite/src/lib.rs`:

- Removed the `ORDER BY is not supported yet` error path from `execute_select`.
- Added sort-key evaluation for each selected row using `OrderByItem` expressions.
- Applied ordering before `OFFSET`/`LIMIT` so row windows are taken from sorted output.
- Allowed `ORDER BY` expressions to reference columns that are not in the projection.

## Ordering behavior

- Supports multi-key ordering with mixed `ASC`/`DESC`.
- Uses a total sort order across supported value types:
  - `NULL` < numeric (`INTEGER`/`REAL`) < `TEXT` for ascending order.
  - Descending reverses the comparison per sort key.
- Numeric comparisons handle `INTEGER` and `REAL` together.

This behavior was cross-checked against `sqlite3` for null placement in ascending and descending sorts.

## Tests added

In `crates/ralph-sqlite/src/lib.rs`:

- `select_order_by_non_projected_column_desc`
- `select_order_by_expression_with_limit_and_offset`
- `select_order_by_nulls_and_secondary_key`

## Remaining limitations

- Aggregate function execution is still not implemented.
- `ORDER BY` positional references (for example `ORDER BY 1`) and alias resolution are not yet special-cased.
