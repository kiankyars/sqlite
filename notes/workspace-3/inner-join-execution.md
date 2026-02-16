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

## Follow-up: Equality ON index probes

A follow-up optimization now uses right-table index probes during JOIN execution when the ON clause includes an indexable equality term.

### What was added
- JOIN ON probe extraction helpers in `crates/ralph-sqlite/src/lib.rs`:
  - `find_join_index_probe`
  - `flatten_and_conjuncts`
  - `build_join_index_probe_side`
  - `expr_references_join_table_alias`
  - `expr_is_resolvable_in_join_prefix`
  - `resolve_join_column_index`
- A `JoinIndexProbe` plan struct.
- Rowid-aware helpers for probe lookups:
  - `read_all_rows_with_rowids`
  - `lookup_table_rows_by_rowids`

### Optimization shape
- ON has an equality term (`=`), including under `AND`.
- One side is a right-table qualified column (e.g. `o.user_id`) with a single-column index.
- The opposite side resolves from the already-joined left prefix and does not reference the right table.

### Semantics
- Full ON expression evaluation still runs after probing to preserve correctness for residual ON terms.
- If probe extraction fails, execution falls back to the prior full nested-loop right-table scan.
- RIGHT/FULL joins still materialize right rows for unmatched-row emission; probes only reduce per-left-row candidate comparisons.

### Tests added
- `join_index_probe_extracts_reversed_equality_from_on_conjunction`
- `join_index_probe_rejects_value_expr_that_references_right_table`
- `select_inner_join_with_index_probe_and_residual_on_filter`
- `select_right_join_with_index_probe_preserves_unmatched_right_rows`

### Validation
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target CARGO_INCREMENTAL=0 cargo test -p ralph-sqlite`
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target CARGO_INCREMENTAL=0 ./test.sh --fast`
