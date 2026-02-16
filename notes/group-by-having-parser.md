# SELECT GROUP BY / HAVING Parser Support

## Scope completed

Implemented parser-side support for SELECT `GROUP BY` and `HAVING` clauses.

Files touched:
- `crates/parser/src/ast.rs`
- `crates/parser/src/parser.rs`
- `crates/parser/src/lib.rs`
- `crates/planner/src/lib.rs`
- `crates/ralph-sqlite/src/lib.rs`

## What changed

- Extended `SelectStmt` with:
  - `group_by: Vec<Expr>`
  - `having: Option<Expr>`
- Updated SELECT parsing order to handle:
  - `WHERE`
  - `GROUP BY`
  - `HAVING`
  - `ORDER BY`
  - `LIMIT` / `OFFSET`
- Added parser helper for GROUP BY expression lists.
- Added parser tests for:
  - `GROUP BY` parsing
  - `GROUP BY` + `HAVING` + `ORDER BY`
  - `HAVING` without `GROUP BY`
- Updated planner unit-test fixture construction for the new `SelectStmt` fields.

## Integration behavior

Grouped execution semantics are still not implemented in `crates/ralph-sqlite`.
To prevent silent misexecution now that parsing succeeds, `execute_select` explicitly returns:
- `GROUP BY is not supported yet`
- `HAVING is not supported yet`

Added integration tests to lock these errors.

## Validation

- `cargo test -p ralph-parser -p ralph-planner -p ralph-sqlite` passed.
- `./test.sh --fast` passed (`seed: 4`, `0 failed`, `5 skipped`).
