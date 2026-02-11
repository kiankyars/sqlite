# DROP TABLE execution + page reclamation

## Scope

Implemented executable `DROP TABLE` in `crates/ralph-sqlite` with dependent-index cleanup and freelist reclamation for dropped table/index B+tree pages.

## What changed

- `crates/ralph-sqlite/src/lib.rs`
  - Added `ExecuteResult::DropTable`.
  - Added `Stmt::DropTable` execution path.
  - Implemented `execute_drop_table`:
    - honors `IF EXISTS`,
    - removes dependent index schema entries,
    - removes table schema entry,
    - reclaims index/table trees with `BTree::reclaim_tree`,
    - updates in-memory catalogs and autocommit behavior.

- `crates/storage/src/schema.rs`
  - Added deletion/listing APIs for object lifecycle:
    - `Schema::drop_table`
    - `Schema::drop_index`
    - `Schema::list_indexes_for_table`
  - Added internal `delete_by_name` helper to remove schema entries by key.

- `crates/storage/src/btree.rs`
  - Added `BTree::reclaim_tree(pager, root_page)` that traverses tree pages and returns them to `Pager::free_page()`.
  - Traversal validates references and errors on duplicate/cyclic page references.

## Tests added

- Storage:
  - `btree::tests::reclaim_tree_returns_pages_to_freelist`
  - `schema::tests::drop_table_removes_schema_entry`
  - `schema::tests::drop_index_removes_schema_entry`
- Integration:
  - `tests::drop_table_removes_table_indexes_and_reclaims_pages`
  - `tests::drop_table_if_exists_is_noop_for_missing_table`

## Remaining limitations

- `DROP INDEX` SQL statement execution is still not implemented.
- UNIQUE/multi-column index execution remains unsupported.
