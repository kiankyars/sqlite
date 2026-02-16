# Multi-Index AND-Intersection Selection

## Scope completed

Implemented planner + execution support for intersecting multiple index-driven branches under `AND` predicates.

- Added `AccessPath::IndexAnd { branches }` in `crates/planner`.
- Planner now flattens `AND` terms and builds intersection plans when two or more terms are independently indexable.
- Composite equality indexes are still preferred when available (for example, `(a, b)` over separate `a` and `b` probes).
- Added execution support in `crates/ralph-sqlite` to evaluate each branch, intersect rowids, and fetch only surviving table entries.

## Behavior details

- `AND` intersection can combine branch types (`IndexEq`, `IndexRange`, `IndexOr`) as long as each branch is index-plannable.
- Duplicate branch plans are deduplicated before building `IndexAnd`.
- Residual full `WHERE` filtering is still applied after candidate fetch for correctness.

## Tests added

In `crates/planner/src/lib.rs`:
- `chooses_index_inside_and_predicate`
- `chooses_index_and_for_multi_column_equality_without_composite_index`
- `plan_where_chooses_index_and_for_mixed_and_predicate`

In `crates/ralph-sqlite/src/lib.rs`:
- `select_supports_index_and_predicates`
- `update_uses_index_for_and_predicate`
- `delete_uses_index_for_and_predicate`

## Remaining follow-up

- No `IN (...)` multi-probe planning yet.
- No cost-based ranking between table scan, single-index paths, and multi-index intersections.
