# IN Multi-Probe Planner/Execution Support

## Scope completed

Implemented planner and execution support for using single-column secondary indexes with `IN (...)` predicates.

- Planner now recognizes positive `col IN (value, ...)` predicates where all list items are row-independent expressions.
- `IN` is planned as:
  - `AccessPath::IndexEq` for a single distinct probe value.
  - `AccessPath::IndexOr` of `IndexEq` branches for multiple distinct probe values.
- Duplicate `IN` list entries are deduplicated at planning time to avoid redundant index probes.

## Integration behavior

No execution-path shape changes were required in `crates/ralph-sqlite` because existing `IndexOr` candidate reads already:

- recursively materialize each branch,
- deduplicate rowids across branches,
- reapply full `WHERE` filtering for correctness.

This means SELECT/UPDATE/DELETE automatically benefit from index-driven candidate reads for indexable `IN` predicates.

## Guardrails and limitations

- `NOT IN (...)` is not planned as index probes yet (falls back to table scan).
- `IN` list items that contain column references are not planned as index probes.
- Scope is single-column index probing only; no multi-column tuple-`IN` planning.

## Tests added

Planner (`crates/planner/src/lib.rs`):

- `chooses_index_or_for_in_predicate`
- `chooses_single_probe_index_for_single_value_in_predicate`
- `falls_back_for_negated_in_predicate`
- `falls_back_for_in_predicate_with_row_dependent_item`
- `plan_where_chooses_index_for_in_predicate`

Integration (`crates/ralph-sqlite/src/lib.rs`):

- `select_supports_index_in_predicates`
- `update_uses_index_for_in_predicate`
- `delete_uses_index_for_in_predicate`

## Follow-up

- Add multi-index AND-intersection planning/execution for conjunctive predicates.
- Add lightweight cost heuristics to avoid index paths when probe fanout is high.
