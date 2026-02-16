# Multi-Column Index Planner/Execution Selection

## Scope completed

Implemented planner + execution support for using multi-column secondary indexes when predicates match all indexed columns by equality.

- `ralph-planner` now models index metadata as ordered column vectors.
- Planner `AccessPath::IndexEq` now carries ordered equality expressions for all matched index columns.
- Planner selects a composite equality probe when a query has constant equalities for every indexed column in a multi-column index.
- `ralph-sqlite` now evaluates all planned equality expressions and probes tuple index keys for candidate rowids in SELECT/UPDATE/DELETE paths.

## Behavior details

- Multi-column index probes require full-tuple equality (`col1 = const AND col2 = const ...`) for the indexed columns.
- Planner still prefers equality plans over range plans.
- If both single-column and multi-column equality plans are possible, planner prefers the longest matching index (most columns).
- SELECT execution now uses candidate-materialization path for composite `IndexEq` probes (the single-column `IndexEqScan` operator remains in use for single-column equality).

## Test coverage

In `crates/planner/src/lib.rs`:
- `chooses_multi_column_index_for_matching_equalities`
- `plan_where_chooses_multi_column_index_for_matching_equalities`

In `crates/ralph-sqlite/src/lib.rs`:
- `select_plans_multi_column_index_for_matching_equalities`
- `update_uses_multi_column_index_for_where_predicate`
- `delete_uses_multi_column_index_for_where_predicate`

## Remaining follow-up

- No partial-prefix planning for multi-column indexes (e.g. only first key column constrained).
- No multi-column range planning.
- No OR/IN multi-probe or multi-index plan composition.
