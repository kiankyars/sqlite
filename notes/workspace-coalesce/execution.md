# Query Execution Engine

The `ralph-executor` crate and `ralph-sqlite` integration layer work together to execute SQL statements.

## Volcano Iterator Model

The core execution engine uses a Volcano-style iterator model. Each physical operator implements the `Operator` trait:

```rust
pub trait Operator<'a> {
    fn open(&mut self) -> ExecResult<()>;
    fn next(&mut self) -> ExecResult<Option<Row>>;
    fn close(&mut self) -> ExecResult<()>;
}
```

### Supported Operators
- **TableScan**: Full scan of a table B+tree.
- **IndexEqScan**: Equality probe into a secondary index.
- **IndexRangeScan**: Range scan of a secondary index (optimized with B+tree seeks for numeric/text bounds).
- **Filter**: Applies a predicate expression to each row.
- **Project**: Computes output expressions for each row.
- **NestedLoopJoin**: Joins two or more tables with optional index-probe optimization.
- **Sort**: Materializes and sorts rows for `ORDER BY`.
- **Limit/Offset**: Applies windowing to the final result set.

## Expression Evaluation

Expressions are evaluated recursively against a row context (`Value` vector).

- **Literals**: `INTEGER`, `REAL`, `TEXT`, `NULL`.
- **Column References**: Resolved by name (case-insensitive) or table-qualified (e.g., `table.col`).
- **Operators**: Unary (`-`, `NOT`) and Binary (`+`, `-`, `*`, `/`, `%`, `=`, `!=`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `LIKE`, `||`).
- **LIKE Matcher**: Implemented using a bottom-up Dynamic Programming approach for $O(N \times M)$ performance. `%` (zero or more) and `_` (exactly one) wildcards are supported.
- **Scalar Functions**: `LENGTH`, `UPPER`, `LOWER`, `ABS`, `COALESCE`, `IFNULL`, `NULLIF`, `SUBSTR`, `INSTR`, `REPLACE`, `TRIM`, `LTRIM`, `RTRIM`, `TYPEOF`.
- **NULL Propagation**: Standard SQL semantics; most operators return NULL if any operand is NULL.

## Joins

- **Strategy**: Nested-loop join.
- **Join Types**: `INNER`, `LEFT`, `RIGHT`, `FULL OUTER`, and `CROSS` (including comma syntax).
- **Index Probe Optimization**: If an `ON` condition is a simple equality between a right-table indexed column and a left-side expression, the join uses an index probe instead of a full right-table scan.
- **Null Extension**: `LEFT`, `RIGHT`, and `FULL` joins preserve unmatched rows by null-extending the corresponding table's columns.

## Aggregates and Grouping

- **Aggregate Functions**: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`.
- **Grouping**: `GROUP BY` partitions rows into groups; aggregate functions are evaluated per group.
- **Filtering**: `HAVING` filters groups after aggregation.
- **Global Aggregation**: If aggregate functions are used without `GROUP BY`, the entire result set is treated as a single group.

## DML and DDL Execution

- **INSERT**: Encodes values into row payloads and inserts into table B+tree.
- **UPDATE**: Selects rows (optionally via index), evaluates assignments, and updates table and indexes.
- **DELETE**: Selects rows and removes from table and indexes.
- **UNIQUE Enforcement**: Enforces `UNIQUE` constraints (single or multi-column) during `INSERT` and `UPDATE`. `NULL` values are treated as distinct.
- **CREATE TABLE/INDEX**: Initializes B+trees and persists metadata in the schema table.
- **DROP TABLE/INDEX**: Reclaims all B+tree pages to the pager's freelist via `BTree::reclaim_tree` and removes schema metadata.

## Secondary Index Execution

- **Maintenance**: Indexes are automatically updated during `INSERT`, `UPDATE`, and `DELETE`.
- **Multi-Column**: Supports composite indexes with tuple-based key encoding.
- **Ordered Seeks**:
  - **Numeric**: Integers and floats use order-preserving encoding for true B+tree range seeks.
  - **Text**: Strings use an 8-byte lexicographic prefix for range seeks, with residual filtering for correctness.
