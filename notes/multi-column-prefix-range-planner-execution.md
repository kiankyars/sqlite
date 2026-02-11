# Multi-Column Prefix/Range Planner + Execution Support

## Scope completed

Implemented planner and execution support for using multi-column secondary indexes when predicates match a left-prefix of indexed columns by equality, with optional range predicates on the next indexed column.

- Added `AccessPath::IndexPrefixRange` in `crates/planner`.
- Planner now emits `IndexPrefixRange` when:
  - At least one leading index column has a constant equality predicate.
  - Optional `<` / `<=` / `>` / `>=` / `BETWEEN` bounds exist on the next index column.
- Added `ralph-sqlite` execution support to materialize candidates for `IndexPrefixRange` in SELECT/UPDATE/DELETE.

## Execution behavior

- Multi-column prefix/range candidate reads scan index entries, decode tuple bucket values, and filter by:
  - left-prefix equality values, then
  - optional bounds on the next tuple component.
- Candidate rowids are deduplicated, and full `WHERE` is still reapplied for correctness.
- Added tuple-bucket decoding helpers for multi-column index payload values.

## Limitations

- Multi-column prefix/range paths currently scan index entries rather than doing ordered key seeks because multi-column index keys are hash-based tuple hashes.
- No multi-index AND-intersection or cost heuristics yet.

## Tests added

Planner (`crates/planner/src/lib.rs`):
- `chooses_multi_column_index_prefix_for_leading_equality`
- `chooses_multi_column_index_prefix_with_trailing_range`
- `plan_where_chooses_multi_column_index_prefix_with_range`

Integration (`crates/ralph-sqlite/src/lib.rs`):
- `select_plans_multi_column_index_prefix_for_leading_equality`
- `select_supports_multi_column_index_prefix_range_predicate`
- `update_uses_multi_column_index_prefix_range_for_where_predicate`
- `delete_uses_multi_column_index_prefix_for_where_predicate`
