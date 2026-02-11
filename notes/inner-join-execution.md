# INNER JOIN / CROSS JOIN Execution

## What was implemented

Basic JOIN support for SELECT queries, including INNER JOIN, CROSS JOIN, and
implicit cross join (comma syntax).

## Changes

### Parser (`crates/parser`)
- Added `Join`, `Inner`, `Cross` keywords to `token.rs`
- Extended `FromClause` AST with a `joins: Vec<JoinClause>` field
- Added `JoinClause` struct (`join_type`, `table`, `alias`, `condition`)
- Added `JoinType` enum (`Inner`, `Cross`)
- Extended `parse_from_clause` to parse:
  - `FROM a, b` (implicit cross join)
  - `FROM a JOIN b ON expr` (inner join)
  - `FROM a INNER JOIN b ON expr` (explicit inner join)
  - `FROM a CROSS JOIN b` (explicit cross join)
  - Multi-table joins: `FROM a JOIN b ON ... JOIN c ON ...`
  - Table aliases: `FROM a AS x JOIN b AS y ON x.id = y.id`

### Execution (`crates/ralph-sqlite`)
- Added `execute_join()` method — reads rows from all joined tables via full
  table scans, then performs nested-loop cross products with ON-condition
  filtering at each join step
- Added `execute_select_join()` — separate SELECT path for join queries that
  handles WHERE filtering, column projection, ORDER BY, OFFSET, and LIMIT
  using the joined row context
- Added `eval_join_expr()` — expression evaluator for joined row contexts with
  table-qualified column resolution (resolves `a.id` via table ranges) and
  ambiguity detection for unqualified columns
- Added join-specific projection and output column helpers

### Design decisions
- Join path is isolated from the existing single-table SELECT path to avoid
  regressions; single-table queries still use the planner-optimized path
- Table-qualified column references use alias-based resolution from a
  `table_ranges` vector that maps (alias, start_col, end_col) to the
  concatenated joined row
- `SELECT *` on a join expands all columns from all tables; duplicate column
  names are qualified with the table alias
- No index-driven join optimization yet — all joins use full table scans
  with nested-loop cross products

## Tests

### Parser tests (6 new)
- `test_cross_join_with_comma`, `test_inner_join_on`, `test_inner_join_explicit`
- `test_cross_join_explicit`, `test_join_with_aliases`, `test_multi_table_join`

### Integration tests (9 new)
- `select_cross_join_comma_syntax` — implicit CROSS JOIN with comma
- `select_inner_join_on` — INNER JOIN with ON condition
- `select_inner_join_with_where` — JOIN + WHERE combination
- `select_join_with_alias` — JOIN with AS aliases
- `select_cross_join_explicit` — explicit CROSS JOIN keyword
- `select_join_star_expands_all_columns` — SELECT * on join
- `select_join_with_limit` — JOIN + ORDER BY + LIMIT
- `select_join_unqualified_column_resolution` — unambiguous unqualified cols
- `select_three_way_join` — three-table join chain

## Limitations
- LEFT/RIGHT/FULL OUTER JOIN not implemented
- No join-order optimization or hash joins
- Aggregate queries (GROUP BY, HAVING) not supported with joins yet
- Self-joins are not tested/guaranteed
