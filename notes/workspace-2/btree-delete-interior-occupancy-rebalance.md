# B+tree Delete Interior Occupancy Rebalance

## Scope

Implemented delete-time occupancy-based rebalance for **interior** B+tree nodes in
`crates/storage/src/btree.rs`.

## What changed

- Interior-node underflow detection now uses logical occupancy thresholds for
  non-root pages instead of `cell_count == 0` only.
  - Threshold: 35% logical utilization (aligned with existing leaf threshold).
  - Logical interior utilization is computed as:
    `PAGE_HEADER_SIZE + key_count * (INTERIOR_CELL_SIZE + CELL_PTR_SIZE)`.
- Replaced empty-only interior child compaction with generalized sibling rebalance:
  - Merge interior siblings when combined keys (including parent separator) fit in
    one interior page.
  - Redistribute interior siblings when merge would overflow, and update the parent
    separator key to the promoted split key.
- Root compaction behavior is preserved: if parent merge reduces root interior
  separator count to zero, `compact_root_if_possible` still compacts in place.

## Tests added

- `delete_rebalances_non_empty_underfull_interior`
  - Builds a 3-level tree shape manually and verifies delete-triggered rebalance
    for a non-empty underfull interior child (merge + root compaction path).
- `rebalance_interior_child_redistributes_when_merge_does_not_fit`
  - Verifies interior redistribution path when sibling merge exceeds one-page
    interior capacity, including parent separator key update and no page
    reclamation.

## Validation

- `cargo test -p ralph-storage`: pass, 0 failed (55 tests).
- `./test.sh --fast` (seed: 4): pass, 0 failed, 5 skipped (deterministic sample).
