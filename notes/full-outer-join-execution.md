# FULL OUTER JOIN Execution

## Scope completed

Implemented `FULL JOIN` / `FULL OUTER JOIN` support in parser and execution.

## What changed

- Parser (`crates/parser`):
  - Added `FULL` SQL keyword.
  - Extended join AST with `JoinType::Full`.
  - Extended join parsing to recognize both `FULL JOIN` and `FULL OUTER JOIN`.
- Execution (`crates/ralph-sqlite`):
  - Updated nested-loop join execution to preserve unmatched rows from both sides for `FULL` joins.
  - Unmatched left rows are null-extended on right-table columns (same as LEFT JOIN behavior).
  - Unmatched right rows are null-extended on left-side columns after ON-condition matching.
  - Existing `WHERE`/`GROUP BY`/`HAVING`/`ORDER BY` paths continue to operate over the joined rows.

## Tests added

- Parser tests:
  - `test_full_join_on`
  - `test_full_outer_join_on`
- Integration tests:
  - `select_full_outer_join_preserves_unmatched_rows_from_both_sides`
  - `select_full_outer_join_where_can_match_right_null_extended_rows`

## Validation

- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-parser -p ralph-sqlite`
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast`

## Remaining limitation

- `RIGHT JOIN` is still not implemented.
