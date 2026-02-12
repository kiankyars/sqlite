# Query Planner and Cost Estimation

The `ralph-planner` crate is responsible for choosing the most efficient access path for a given SQL query. It transforms a logical `WHERE` clause or `SelectStmt` into a physical `AccessPath`.

## Access Paths

The planner can choose between several access methods:

- **TableScan**: A full scan of the table's B+tree.
- **IndexEq**: Probing a secondary index for a constant equality predicate.
- **IndexRange**: Scanning a secondary index using lower (`>`, `>=`) and/or upper (`<`, `<=`, `BETWEEN`) bounds.
- **IndexPrefixRange**: Probing a multi-column index with a left-prefix of equality predicates and an optional trailing range.
- **IndexOr**: A union of multiple index-driven branches, used for `OR` predicates and `IN (...)` lists.
- **IndexAnd**: An intersection of multiple index-driven branches, used for conjunctive (`AND`) predicates.

## Selection Logic

- **Indexability**: A predicate is indexable if it's of the form `col OP constant` (or `constant OP col`) and an index exists on `col`.
- **In-Memory Deduplication**: For `IN (...)` predicates, duplicate list items are deduplicated at planning time to avoid redundant probes.
- **Composite Indexes**:
  - Full-tuple equality is preferred when all indexed columns have constant equality predicates.
  - Partial-prefix matches are supported via `IndexPrefixRange` if at least the first indexed column has an equality predicate.
- **AND Intersection**: If multiple independent indexes match different parts of an `AND` clause, the planner can emit an `IndexAnd` path which intersects rowids before fetching table rows.
- **OR Union**: `OR` predicates are indexable if *all* branches are indexable. `IN` lists are treated as a series of `OR` equality terms.
- **Preference Order**: Equality probes (`IndexEq`) are generally preferred over range scans (`IndexRange`). The longest matching index (most columns) is preferred for composite probes.

## Cost-Based Optimization

The planner uses a cost model to decide between candidate paths:

### Static Heuristics
When statistics are absent, the planner uses fixed costs (e.g., `TableScan` = 100, `IndexEq` = 14).
- **Fanout Gating**: `IndexOr` and `IndexAnd` paths are gated by static fanout thresholds. For example, large `IN (...)` lists may fall back to `TableScan` if the probe count is too high.
- **Penalty for Weak Prefixes**: Weak multi-column prefix probes (e.g., only one column matched in a wide index) are penalized to favor table scans.

### Statistics-Driven Costing
When table/index cardinality stats are available, the planner estimates the cost based on:
- **Table Cardinality**: Table scan cost scales linearly with the estimated number of rows.
- **Selectivity**:
  - `IndexEq`: Estimated rows per distinct key.
  - `IndexRange`: Estimated via bound shape and total rows.
  - `IndexPrefixRange`: Uses **prefix fanout** (distinct counts per leading prefix length) to estimate selectivity for partial matches.
- **Probabilistic Combination**:
  - `AND` selectivity: `Π(s_i)` (multiplication of branch selectivities).
  - `OR` selectivity: `1 - Π(1 - s_i)` (probability-style union).
- **Cost Comparison**: The planner compares the estimated cost of candidate paths (e.g., comparing a composite index probe vs. an intersection of single-column indexes) and picks the cheapest.

## Planner Statistics

### Metadata and Persistence
Planner statistics are persisted in the schema table (page 1) as `Stats` entries and loaded into memory on `Database::open`.
- **Table Stats**: Total row count.
- **Index Stats**: Row count, distinct key count, and **prefix fanout** (a vector of distinct counts for each leading prefix level).

### Maintenance
Statistics are refreshed and persisted after significant DML operations:
- `CREATE TABLE`, `CREATE INDEX`.
- `INSERT`, `UPDATE`, `DELETE` (when row counts change).
- Stats refresh currently involves re-scanning the affected B+trees.

## Execution Integration

### DML Support
Both `UPDATE` and `DELETE` use the planner via `plan_where()` to select rows efficiently.
1. Evaluate planned constant expressions for index probes.
2. Probe secondary index(es) for matching rowids.
3. Fetch candidate table rows by rowid.
4. **Re-apply full WHERE**: Because indexes may over-select (due to hash collisions, multi-column prefix scans, or complex `AND` terms), the full `WHERE` clause is always evaluated against the fetched table rows for correctness.

### Secondary Index Maintenance
To ensure index-driven reads remain correct, the execution layer maintains secondary indexes during DML:
- `INSERT`: Adds new entries to all secondary indexes.
- `UPDATE`: Removes old entries and adds new entries for modified rows.
- `DELETE`: Removes entries from all secondary indexes before deleting the table row.
