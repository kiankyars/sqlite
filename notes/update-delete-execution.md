# UPDATE/DELETE Execution Handoff

## Scope completed

Implemented a storage-layer delete primitive in `crates/storage/src/btree.rs` as groundwork for task #12:

- Added `BTree::delete(key) -> io::Result<bool>`.
- Delete traverses the tree to the target leaf and removes the key if present.
- Return value semantics:
  - `Ok(true)` when a row is deleted.
  - `Ok(false)` when the key is not present.

## Tests added

- `delete_existing_and_missing_keys`
- `delete_after_leaf_splits`

Both are in `crates/storage/src/btree.rs` tests and pass under `cargo test --workspace`.

## Important behavior note

Delete is currently **non-rebalancing**. It does not merge/redistribute underflowing nodes and does not shrink roots. This matches current staged roadmap expectations (task #18 handles merge/rebalance).

## Suggested next steps for task #12

1. Build a table-level row codec + scan/filter path in executor/storage integration.
2. Wire parsed `UPDATE` and `DELETE` AST nodes to row selection + write/delete operations.
3. Reuse expression evaluation implementation (task #11) for `WHERE` predicate matching.
