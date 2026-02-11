# Persisted Planner Statistics Metadata

## Scope completed

Implemented persisted planner cardinality statistics in schema storage and integrated planner consumption in `crates/ralph-sqlite`.

- Added schema object type `Stats` and persistence APIs:
  - `Schema::upsert_table_stats`
  - `Schema::upsert_index_stats`
  - `Schema::list_table_stats`
  - `Schema::list_index_stats`
  - `Schema::drop_table_stats`
  - `Schema::drop_index_stats`
- Added `TableStatsEntry` / `IndexStatsEntry` metadata decoding with validation.
- Added schema tests for upsert/list/drop and reopen persistence.

## Runtime integration (`ralph-sqlite`)

- `Database::open` now loads persisted table/index planner stats alongside table/index catalogs.
- Added in-memory cached maps:
  - `table_stats: HashMap<String, usize>`
  - `index_stats: HashMap<String, PersistedIndexStats>`
- `plan_where_with_stats` / `plan_select_with_stats` are now fed from persisted stats caches rather than per-query B+tree full scans.
- Write paths refresh and persist stats for affected tables/indexes:
  - `CREATE TABLE`
  - `CREATE INDEX`
  - `INSERT`
  - `UPDATE` (when rows change)
  - `DELETE` (when rows change)
- `DROP TABLE` / `DROP INDEX` now remove persisted stats metadata.
- Transaction rollback snapshots now include stats caches for connection-local consistency.

## Tests added

`crates/storage/src/schema.rs`:
- `planner_stats_upsert_list_and_drop`

`crates/ralph-sqlite/src/lib.rs`:
- `planner_stats_persist_across_reopen`
- `planner_stats_refresh_on_write_statements`

## Current limitations

- Persisted stats include table row count plus index row/distinct-key counts only.
- No histogram/fanout or multi-column distribution stats yet.
- Stats refresh currently re-scans affected table/index B+trees on writes.
