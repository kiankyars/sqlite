# SELECT GROUP BY / HAVING Execution

## Scope completed

Implemented grouped SELECT execution semantics in `crates/ralph-sqlite/src/lib.rs`.

## What changed

- Added grouped execution path in `Database::execute_select` when `GROUP BY` is present.
  - Forms groups by evaluating `GROUP BY` expressions over filtered candidate rows.
  - Evaluates `HAVING` per group.
  - Supports grouped projection expressions and grouped `ORDER BY` expressions.
- Added grouped expression evaluation helpers that combine:
  - aggregate function evaluation (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) over group rows
  - non-aggregate expression evaluation against a representative row for the group
- Added validation for `GROUP BY` expressions:
  - Aggregate functions in `GROUP BY` are rejected with
    `aggregate functions are not allowed in GROUP BY`.
- Added `HAVING` support without `GROUP BY` for aggregate queries.
  - `SELECT COUNT(*) FROM t HAVING COUNT(*) > ...` now executes.
  - `HAVING` on non-aggregate, no-`GROUP BY` queries now errors with
    `HAVING clause on a non-aggregate query`.
- Added scalar no-`FROM` grouped behavior for single pseudo-row semantics.

## Tests added/updated

In `crates/ralph-sqlite/src/lib.rs`:

- `select_group_by_aggregate_and_having_filters_groups`
- `select_group_by_without_aggregates_deduplicates_rows`
- `select_having_without_group_by_aggregate_query`
- `select_having_without_group_by_non_aggregate_errors`
- `select_group_by_rejects_aggregate_expression`
- `select_group_by_without_from_uses_single_scalar_row`

Replaced prior guardrail tests that expected `GROUP BY` / `HAVING` unsupported errors.

## Current limits

- No JOIN/subquery support (existing engine-wide limitation).
- Aggregate queries without `GROUP BY` still reject bare column references outside aggregates.
- Range index access still scans hash buckets; true ordered index seeks remain pending.
