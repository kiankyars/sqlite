# B+tree Delete Rebalance/Merge Handoff

## Scope completed (task #18)

Implemented delete-time underflow handling in `crates/storage/src/btree.rs`:

- `BTree::delete` now performs recursive delete with underflow propagation.
- Empty leaf children are rebalanced at the parent:
  - Non-leftmost empty leafs are removed from the parent and leaf links are patched.
  - Leftmost empty leafs are compacted by copying the right sibling into the leftmost page, then removing the sibling pointer from the parent.
- Empty interior children are compacted by replacing the child pointer in the parent with the child’s only remaining subtree pointer.
- Root compaction is implemented: when the root interior has 0 separator keys, its only child page is copied into the root page so the externally-visible root page number stays stable.

## Tests added

- `delete_compacts_root_after_leftmost_leaf_becomes_empty`
- `delete_compacts_multi_level_tree_to_single_leaf`

Both are in `crates/storage/src/btree.rs` and exercise root-height reduction/compaction after deletes.

## Important behavior notes

- Rebalancing currently triggers on **empty-node underflow** (`cell_count == 0`), not byte-level occupancy thresholds.
- Reclaimed pages are not returned to freelist yet (no public `free_page()` API in pager), so this is logical compaction of tree structure, not physical page reclamation.
- Root page number stability is preserved during delete compaction, which avoids catalog updates in higher layers.
