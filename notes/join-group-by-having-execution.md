# JOIN GROUP BY / HAVING Execution

## Scope completed

Implemented grouped and aggregate SELECT execution semantics for JOIN queries in
`crates/ralph-sqlite/src/lib.rs`.

## What changed

- Extended `execute_select_join` to mirror single-table SELECT branching:
  - grouped path when `GROUP BY` is present
  - aggregate path for aggregate/HAVING queries without `GROUP BY`
  - row-by-row projection path for non-aggregate queries
- Replaced JOIN `WHERE` filtering that previously swallowed expression errors with
  explicit evaluation that propagates errors.
- Added JOIN-aware expression helpers for grouped and aggregate execution:
  - `evaluate_join_group_by_key`
  - `eval_grouped_join_expr`
  - `eval_join_aggregate_expr`
  - `eval_join_aggregate_function`
- Added JOIN-aware projection/order-key helpers for grouped and aggregate paths so
  `HAVING`, `ORDER BY`, `LIMIT`, and `OFFSET` behave consistently with existing
  non-join grouped semantics.

## Semantics

- `GROUP BY` on JOIN results now groups filtered joined rows and evaluates grouped
  projection/HAVING/ORDER BY expressions.
- Aggregate JOIN queries without `GROUP BY` now return a single aggregate row over
  the filtered join result.
- Bare column references outside aggregate functions in aggregate JOIN queries
  continue to return:
  `column references outside aggregate functions are not supported without GROUP BY`.

## Tests added

In `crates/ralph-sqlite/src/lib.rs`:

- `select_join_group_by_and_having`
- `select_join_aggregate_without_group_by`
- `select_join_aggregate_without_group_by_rejects_bare_column`

## Validation

- `CARGO_INCREMENTAL=0 RUSTFLAGS='-Ccodegen-units=1 -Cdebuginfo=0' cargo test -p ralph-sqlite`
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-Ccodegen-units=1 -Cdebuginfo=0' ./test.sh --fast`

(Build flags were used in this environment to avoid inode/quota failures during
compilation; query behavior is unaffected.)
