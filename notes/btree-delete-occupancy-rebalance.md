# B+tree Delete Occupancy Rebalance

## Scope

Implemented leaf-level occupancy-based rebalance for delete paths in
`crates/storage/src/btree.rs`.

## What changed

- Delete underflow detection for leaf pages now triggers based on logical page
  occupancy instead of only `cell_count == 0`.
  - Threshold: leaf page is considered underfull below 35% logical utilization.
  - Utilization is computed from live cells (`key + payload_size + payload`)
    and pointer array bytes, so stale deleted-cell bytes do not mask underflow.
- Replaced empty-only leaf child compaction with generalized sibling rebalance:
  - If two adjacent leaf siblings fit in one page, merge them and remove one
    parent separator/child pointer.
  - Otherwise redistribute entries across the siblings and update the parent
    separator key to the right leaf's first key.
- Existing empty-interior child compaction behavior remains unchanged.

## Tests added

- `delete_merges_non_empty_underfull_leaf`
  - Verifies delete merges a non-empty sparse leaf (before it becomes empty),
    and root compaction preserves the root page number.
- `delete_redistributes_non_empty_underfull_leaf`
  - Verifies delete redistributes between two non-empty siblings when merge
    would overflow one page, and confirms parent separator key update.

## Validation

- `cargo test -p ralph-storage`: pass (53 passed, 0 failed)
- `./test.sh --fast` (seed: 3): pass (1 passed, 0 failed, 4 skipped)
