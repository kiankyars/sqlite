# Planner Histogram/Fanout Statistics

## Scope completed

Added persisted prefix fanout statistics for indexes and used them in stats-aware planner costing, with emphasis on multi-column prefix/range access paths.

- Extended persisted index stats metadata:
  - `prefix_distinct_counts: Vec<usize>` (distinct counts per leading prefix length).
- Extended planner stats model:
  - `IndexStats` now includes `prefix_distinct_counts`.
- Refined stats-aware `IndexPrefixRange` costing:
  - Prefix-only probes now estimate rows from `estimated_rows / distinct(prefix_len)`.
  - Prefix+range probes now apply range selectivity derived from adjacent prefix fanout levels.
- Kept legacy behavior unchanged when stats are absent.

## Storage / integration changes

- `Schema::upsert_index_stats(...)` now persists `prefix_distinct_counts`.
- `Schema::list_index_stats(...)` now loads `prefix_distinct_counts` (backward-compatible fallback to `[estimated_distinct_keys]` when older metadata lacks the field).
- `ralph-sqlite` stats refresh now computes per-prefix distinct counts during index cardinality scans and persists them.
- `Database::open` now reloads `prefix_distinct_counts` into in-memory planner stats caches.

## Tests added/updated

In `crates/planner/src/lib.rs`:

- `plan_where_with_stats_uses_prefix_fanout_for_composite_prefix_probe`
- `plan_where_with_stats_avoids_unselective_composite_prefix_probe`

Updated existing stats tests to include `prefix_distinct_counts` payloads.

In `crates/storage/src/schema.rs` and `crates/ralph-sqlite/src/lib.rs`:

- Updated persisted planner stats tests to assert roundtrip behavior for `prefix_distinct_counts`.

## Validation

- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-planner -p ralph-storage -p ralph-sqlite`
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast`

## Limitations

- Prefix fanout stats are aggregate (global) and do not model per-value skew.
- No value-domain histograms yet for tighter bound-aware range estimates.
