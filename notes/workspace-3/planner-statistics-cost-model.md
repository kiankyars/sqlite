# Planner Statistics-Driven Cost Model

## Scope completed

Added statistics-aware access-path costing in `crates/planner` and wired runtime stats collection in `crates/ralph-sqlite`.

- Added new planner APIs:
  - `plan_where_with_stats(...)`
  - `plan_select_with_stats(...)`
- Added new planner stats types:
  - `PlannerStats { estimated_table_rows, index_stats }`
  - `IndexStats { index_name, estimated_rows, estimated_distinct_keys }`
- Kept existing `plan_where(...)` and `plan_select(...)` APIs unchanged (legacy static heuristics remain default when stats are not provided).

## Costing behavior

When stats are present:

- Table scan cost scales with estimated table rows.
- `IndexEq` cost uses estimated rows per distinct key when available.
- `IndexRange` cost uses bound shape plus index row estimate.
- `IndexOr` and `IndexAnd` aggregate branch costs with simple union/intersection row estimates.

When stats are absent:

- Planner uses prior static heuristic constants, preserving existing behavior and tests.

## Integration behavior (`ralph-sqlite`)

`Database` now collects lightweight runtime stats per planned statement:

- Table row estimate: `BTree::scan_all().len()` on the table root.
- Index row/distinct estimates: full index scan + decoded index buckets.

These stats are fed to planner for SELECT/UPDATE/DELETE access-path selection.

## Tests added/updated

- Planner:
  - `plan_where_with_stats_keeps_large_in_probe_for_selective_index`
  - `plan_where_with_stats_falls_back_for_low_cardinality_index_eq`
- Existing planner and integration tests remain green.

## Limitations / follow-up

- Stats are runtime scans, not persisted catalog statistics.
- No histograms/correlation statistics yet.
- Stats collection can add planning overhead on large tables/indexes.
