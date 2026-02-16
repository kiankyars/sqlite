# Multi-Column Prefix/Range Scan Reduction

## Scope completed

Reduced planner selection of expensive `IndexPrefixRange` plans that currently require full composite-index scans.

- Updated `IndexPrefixRange` cost estimation in `crates/planner` to account for:
  - equality-prefix depth (`eq_prefix_value_exprs.len()`), and
  - presence of lower/upper bounds.
- Added a stronger penalty for weak prefix-only probes (for example, only one leading equality and no trailing range bounds), so planner can fall back to `TableScan`.
- Kept existing `AND`-intersection behavior intact, but when both `IndexPrefixRange` and `IndexAnd` are candidates in an `AND` predicate, the planner now compares their estimated costs and picks the cheaper path.

## Behavior changes

- Composite-only query shapes like `WHERE score = 10` now fall back to `TableScan` instead of choosing `IndexPrefixRange`.
- Bounded prefix/range shapes such as `WHERE score = 10 AND age >= 21 AND age < 30` continue to use `IndexPrefixRange`.
- Existing non-prefix `IndexAnd` planning remains unchanged for single-column conjunctions.

## Tests updated/added

In `crates/planner/src/lib.rs`:
- Renamed/updated: `falls_back_for_weak_multi_column_index_prefix_without_range`
- Added: `plan_where_falls_back_for_weak_multi_column_prefix_without_range`
- Existing range-prefix and `IndexAnd` expectations remain passing.

In `crates/ralph-sqlite/src/lib.rs`:
- Renamed/updated: `select_falls_back_for_weak_multi_column_index_prefix_without_range`
- Strengthened `select_supports_multi_column_index_prefix_range_predicate` to assert planner path (`AccessPath::IndexPrefixRange`) before execution.

## Remaining follow-up

- The planner still uses static costs; it does not yet consume table/index statistics.
- Multi-column prefix/range execution still scans composite index entries due tuple-hash keying; this change reduces selection frequency for weak forms rather than adding ordered composite seeks.
