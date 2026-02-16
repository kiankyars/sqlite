# Planner Cost Heuristics Selection

## Scope completed

Added lightweight cost heuristics in `crates/planner` so index-driven access paths are chosen only when their estimated cost is lower than a table scan.

- Added a simple path-cost model for:
  - `AccessPath::IndexEq`
  - `AccessPath::IndexRange`
  - `AccessPath::IndexOr`
  - `AccessPath::IndexAnd`
- Added a table-scan baseline cost and planner fallback to `TableScan` when an index plan cost meets or exceeds that baseline.
- Kept existing access-path shape generation logic intact (`IndexEq`/`IndexRange`/`IndexOr`/`IndexAnd`), but gated final plan selection with cost checks.

## Behavior details

- Small probe fanout still chooses index paths.
  - Example: `score IN (1,2,3,4,5)` plans as `IndexOr`.
- Larger fanout falls back to table scans.
  - Example: `score IN (1,2,3,4,5,6)` plans as `TableScan`.
- High-cost multi-branch intersections also fall back.
  - Example: conjunctions across many independently indexed columns can now choose `TableScan`.

## Tests added

In `crates/planner/src/lib.rs`:

- `plan_where_keeps_index_for_small_in_probe_fanout`
- `plan_where_falls_back_for_large_in_probe_fanout`
- `plan_where_falls_back_for_high_cost_index_intersection`

## Remaining follow-up

- Heuristics are static and do not use table/index statistics.
- No true cardinality estimation yet.
- Multi-column prefix/range planning is still pending.
