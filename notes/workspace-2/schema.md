# Schema Table Notes

## Overview

The schema table is a B+tree rooted at `header.schema_root`. It stores one entry per database object (table or index), keyed by a sequential i64 ID.

## API

```rust
Schema::initialize(pager) -> PageNum       // Create schema B+tree, set header.schema_root
Schema::create_table(pager, name, cols, sql) -> PageNum  // Returns new table's root page
Schema::find_table(pager, name) -> Option<SchemaEntry>
Schema::list_tables(pager) -> Vec<SchemaEntry>
```

## SchemaEntry Fields

- `id`: i64 — B+tree key (sequential)
- `object_type`: Table | Index
- `name`: object name
- `table_name`: for indexes, the associated table
- `root_page`: PageNum of the object's B+tree root
- `sql`: original CREATE statement
- `columns`: Vec<ColumnInfo> with name, data_type, index

## Serialization Format

Binary, big-endian:
```
[u8 object_type] [u32 root_page] [str name] [str table_name] [str sql]
[u16 col_count] { [str col_name] [str col_type] [u32 col_index] }*
```
Where `str` = `[u16 len] [utf-8 bytes]`.

## Integration Points

- **CREATE TABLE**: Parser produces `CreateTableStatement` → `Schema::create_table` stores metadata and allocates a data B+tree.
- **INSERT/SELECT**: Look up table via `Schema::find_table` to get the data B+tree root_page.
- **End-to-end** (task #9): Wire parser → schema → btree for full SQL execution.
