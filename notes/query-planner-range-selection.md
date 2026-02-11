# Query Planner Range Selection

## Scope completed

Extended planner-driven index selection to support range-style predicates in addition to equality:

- Added `AccessPath::IndexRange` in `crates/planner` with lower/upper optional bounds.
- Planner now recognizes indexable range predicates for single-table/single-column indexes:
  - `col > const`, `col >= const`, `col < const`, `col <= const`
  - reversed comparisons like `const <= col`
  - `col BETWEEN low AND high` (non-negated)
- Equality planning remains preferred when both equality and range opportunities exist.

## Integration behavior

In `crates/ralph-sqlite`:

- Added index-range candidate row selection for both `SELECT` and `plan_where` consumers (`UPDATE`/`DELETE`).
- Range candidate selection scans index payload buckets and applies bound checks to indexed values, then fetches matching table rows by rowid.
- Full `WHERE` is still reapplied after candidate fetch for correctness.

## Important implementation note

Secondary index keys are hash-based (`fnv1a`), not value-ordered. Because of that, range access currently performs a full index scan of buckets rather than an ordered B+tree seek. This is functionally correct but not yet a true ordered range index scan.

## Tests added

- Planner unit tests:
  - range predicate (`>`)
  - reversed comparison (`<=` with constant on left)
  - `BETWEEN`
  - `plan_where` range selection
- Integration tests:
  - `select_supports_index_range_predicates`
  - `update_uses_index_for_range_predicate`

## Suggested next steps

1. Add OR/IN multi-probe planning for indexed equality terms.
2. Move secondary index keying from hash-only to order-preserving encoded keys to enable true ordered range seeks.
3. Add simple cost heuristics to choose between table scan and index-range candidate scan.

## Follow-up completed: OR predicate index unions

Planner and execution now support `OR` index unions for single-table/single-column index predicates:

- Planner adds `AccessPath::IndexOr { branches }`.
- OR paths are planned only when all OR branches are independently indexable.
- Branches can mix equality and range paths.
- Execution unions and deduplicates branch-selected rowids, then reapplies full WHERE filtering.

This closes the OR-planning gap from this note's suggested next steps while preserving correctness for mixed predicates.
