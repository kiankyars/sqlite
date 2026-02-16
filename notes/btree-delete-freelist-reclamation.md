# B+tree Delete Freelist Reclamation

## Scope

Wired pager freelist reclamation into B+tree delete compaction paths in
`crates/storage/src/btree.rs`.

## What changed

- Root compaction (`compact_root_if_possible`) now calls `Pager::free_page` on
  the copied-out child page.
- Empty-leaf child rebalance now frees the removed child page:
  - leftmost-empty case frees the removed right sibling page after copying it,
  - non-leftmost-empty case frees the removed empty child page after pointer
    repair.
- Empty-interior child rebalance now frees the compacted-away interior child
  page after replacing it with its remaining subtree.

## Validation

- Added storage unit test:
  - `delete_compaction_reclaims_pages_to_freelist`
    - builds a multi-level tree,
    - deletes to trigger compaction,
    - asserts freelist count grows,
    - asserts subsequent allocations reuse reclaimed pages without extending
      page count.
- Existing delete-compaction tests continue to pass.
