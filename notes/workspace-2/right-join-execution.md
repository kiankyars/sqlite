# RIGHT JOIN Execution

## Scope completed

Implemented `RIGHT JOIN` support in parser and execution.

## What changed

- Parser (`crates/parser`):
  - Added `RIGHT` SQL keyword.
  - Extended join AST with `JoinType::Right`.
  - Extended join parsing to recognize both `RIGHT JOIN` and `RIGHT OUTER JOIN`.
- Execution (`crates/ralph-sqlite`):
  - Updated nested-loop join execution to preserve unmatched right rows for `RIGHT` joins.
  - Unmatched rows are null-extended on all left-side columns (the accumulated join prefix).
  - Existing `WHERE`/`GROUP BY`/`HAVING`/`ORDER BY` paths continue to run over the joined rows, so predicates like `left_col IS NULL` work on null-extended rows.

## Tests added

- Parser tests:
  - `test_right_join_on`
  - `test_right_outer_join_on`
- Integration tests:
  - `select_right_join_preserves_unmatched_right_rows`
  - `select_right_join_where_can_match_null_extended_rows`

## Validation

- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-parser -p ralph-sqlite`
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast`

## Remaining limitation

- `FULL OUTER JOIN` is still not implemented.
