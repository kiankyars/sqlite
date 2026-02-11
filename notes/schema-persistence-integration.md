# Schema Persistence Integration Handoff

## Scope completed

Integrated persisted schema-table metadata (`ralph-storage::Schema`) into `crates/ralph-sqlite` so table/index catalogs survive database reopen:

- `Database::open` now:
  - initializes schema storage on first open (`Schema::initialize`) when `header.schema_root == 0`
  - loads table + index metadata from persisted schema entries into runtime catalogs
- `CREATE TABLE` now persists catalog metadata via `Schema::create_table` instead of only mutating in-memory maps.
- `CREATE INDEX` now persists catalog metadata via new `Schema::create_index` API.

## Storage schema API additions

In `crates/storage/src/schema.rs`:

- Added index metadata APIs:
  - `Schema::create_index(...) -> PageNum`
  - `Schema::find_index(...) -> Option<SchemaEntry>`
  - `Schema::list_indexes(...) -> Vec<SchemaEntry>`
- Refactored internal entry listing/lookup:
  - centralized `list_entries` + `list_by_type` + `find_by_name`
  - populated `SchemaEntry.id` from B+tree key on reads
- Added unit test: `create_and_find_index`.

## Runtime behavior details

- Index schema entries persist:
  - index name
  - table name
  - index root page
  - indexed column name + ordinal
- On open, index metadata is validated against loaded table metadata; malformed schema entries (missing table or unknown column) return an open-time error.

## Tests added

`crates/ralph-sqlite/src/lib.rs`:
- `table_catalog_persists_across_reopen`
- `index_catalog_persists_across_reopen`

`crates/storage/src/schema.rs`:
- `create_and_find_index`

Validation runs:
- `cargo test -p ralph-storage -p ralph-sqlite` (pass)
- `cargo test --workspace` (pass)
- `./test.sh --fast` (pass, deterministic sample with all 5 checks skipped)
