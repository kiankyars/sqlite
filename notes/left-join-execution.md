# LEFT JOIN Execution

## Scope completed

Implemented `LEFT JOIN` support in parser and execution.

## What changed

- Parser (`crates/parser`):
  - Added `LEFT` and `OUTER` SQL keywords.
  - Extended join AST with `JoinType::Left`.
  - Extended join parsing to recognize both `LEFT JOIN` and `LEFT OUTER JOIN`.
- Execution (`crates/ralph-sqlite`):
  - Updated nested-loop join execution to preserve unmatched left rows for `LEFT` joins.
  - Unmatched rows are null-extended on right-table columns.
  - Existing `WHERE`/`GROUP BY`/`HAVING`/`ORDER BY` paths continue to run over the joined rows, so predicates like `right_col IS NULL` work on null-extended rows.

## Tests added

- Parser tests:
  - `test_left_join_on`
  - `test_left_outer_join_on`
- Integration tests:
  - `select_left_join_preserves_unmatched_left_rows`
  - `select_left_join_where_can_match_null_extended_rows`

## Validation

- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-parser -p ralph-sqlite`
- `./test.sh --fast`

## Remaining limitation

- `RIGHT JOIN` and `FULL OUTER JOIN` are still not implemented.
