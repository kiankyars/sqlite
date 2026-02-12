# Schema and Secondary Indexes

This document describes how ralph-sqlite manages database metadata and secondary indexes.

## Schema Table (Catalog)

The schema table (equivalent to `sqlite_master`) is the root of the database's metadata. It is stored in a B+tree rooted at **Page 1** (as indicated in `header.schema_root`).

### Storage and Persistence
- **Structure**: A B+tree keyed by object name (string).
- **Entries**: Each entry persists metadata for one object:
  - `type`: `table`, `index`, or `stats`.
  - `name`: Object name (e.g., `users`).
  - `table_name`: For indexes and stats, the name of the associated table.
  - `root_page`: The PageNum of the object's B+tree root.
  - `sql`: The original `CREATE` statement.
- **Lifecycle**:
  - `Database::open` loads the schema tree into in-memory catalogs.
  - `CREATE` and `DROP` statements update both the in-memory maps and the on-disk schema B+tree.
  - Schema initialization happens on the first open if `header.schema_root` is 0.

## Secondary Indexes

Secondary indexes provide efficient access paths by mapping indexed column values to rowids.

### Index Types
- **Single-Column**: Indexed on a single table column.
- **Multi-Column (Composite)**: Indexed on a sequence of columns. Uses tuple-based key encoding.
- **Unique**: Enforces that no two rows share the same non-NULL indexed values.

### Index Key Encoding
Index keys are 64-bit integers (`i64`) used as B+tree keys.
- **Numeric**: `INTEGER` and finite `REAL` values use an order-preserving encoding, mapping them to a monotonic `i64` space. This enables true B+tree range seeks.
- **Text**: Strings use a lexicographic prefix encoding (approx. 8 bytes) to enable range seeks, with residual value-level filtering to resolve collisions.
- **Hash Fallback**: Values that cannot be ordered (e.g., `NULL`, `NaN`, or multi-column hashes for point lookups) use a hash-based key (`FNV-1a`).
- **Multi-Column Encoding**: Composite values are encoded as tuples and either hashed (for equality) or prefix-encoded (for range scans).

### Maintenance
- **Backfill**: `CREATE INDEX` scans the table to populate the new index B+tree.
- **DML Integration**: `INSERT`, `UPDATE`, and `DELETE` automatically maintain all indexes on the affected table.
- **Uniqueness**: `UNIQUE` constraints are validated before writes. `NULL` values are excluded from uniqueness checks (multiple NULLs are allowed).

## Page Reclamation

When objects are removed, ralph-sqlite performs physical page reclamation to keep the database file compact:
- **Schema Removal**: The metadata entry is deleted from the schema B+tree.
- **Tree Reclamation**: `BTree::reclaim_tree` traverses the object's B+tree and returns all pages to the `Pager`'s freelist.
- **Cascading Drop**: Dropping a table automatically reclaims all associated secondary indexes.

## Planner Statistics

The schema table also stores `stats` entries used for cost-based optimization.
- **Table Stats**: Row counts.
- **Index Stats**: Row counts, distinct key counts, and **prefix fanout** (distinct counts for each leading prefix level in composite indexes).
- **Maintenance**: Stats are refreshed after DML operations and persisted alongside other schema metadata.
