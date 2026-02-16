# DROP INDEX SQL execution

## Scope

Implemented parser and integration-layer support for `DROP INDEX` with `IF EXISTS` handling and index-tree page reclamation.

## What changed

- `crates/parser/src/ast.rs`
  - Added `Stmt::DropIndex(DropIndexStmt)` and `DropIndexStmt { if_exists, index }`.

- `crates/parser/src/parser.rs`
  - Extended `DROP` parsing to accept both `TABLE` and `INDEX` forms.
  - Added `parse_drop_index` supporting `DROP INDEX [IF EXISTS] <index_name>`.

- `crates/ralph-sqlite/src/lib.rs`
  - Added `ExecuteResult::DropIndex`.
  - Added `Stmt::DropIndex` dispatch in `Database::execute`.
  - Added `execute_drop_index` implementation:
    - validates presence of the target index (or no-op with `IF EXISTS`),
    - removes index metadata from schema storage via `Schema::drop_index`,
    - reclaims the index B+tree pages via `BTree::reclaim_tree`,
    - removes in-memory index metadata,
    - honors autocommit/explicit transaction behavior via `commit_if_autocommit`.

## Tests added

- Parser:
  - `parser::tests::test_drop_index`
  - `parser::tests::test_drop_index_if_exists`
  - `tests::test_parse_drop_index`

- Integration (`crates/ralph-sqlite/src/lib.rs`):
  - `drop_index_removes_index_and_reclaims_pages`
  - `drop_index_if_exists_is_noop_for_missing_index`

## Remaining limitations

- `DROP INDEX` currently drops by index name only; schema object ownership checks beyond catalog metadata are not needed with current object model.
- Multi-column and `UNIQUE` index execution remain unsupported (existing limitation).
