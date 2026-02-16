# Ordered Text Range Index Seeks

## Scope completed

Extended secondary-index keying so `TEXT` values now use order-preserving
keys, enabling `IndexRange` scans for text bounds instead of unconditional
full index scans.

## Changes

- `crates/executor`
  - `ordered_index_key_for_value` now returns an ordered key for `Value::Text`.
  - Added `ordered_text_key` using an 8-byte lexicographic prefix mapped to the
    B+tree `i64` key space.
  - Preserved hash-based fallback in `index_key_for_value` for non-orderable
    values (for example `NULL` and `NaN`).
- `crates/ralph-sqlite`
  - Existing `ordered_range_key_bounds` and `IndexRange` candidate read paths
    now automatically use `BTree::scan_range` for text bounds because text keys
    are now orderable.
  - Value-level range filtering remains in place, so strict bound semantics and
    prefix-collision correctness are preserved.

## Tradeoffs

- Text ordering uses an 8-byte prefix key, so strings sharing the same prefix
  can collide to one B+tree key.
- Correctness is preserved via per-bucket value filtering, but highly
  colliding prefixes can still increase scan work within the selected key span.

## Tests added

- `crates/executor/src/lib.rs`
  - `ordered_index_key_is_monotonic_for_text_values`
  - `ordered_index_key_collides_for_text_prefixes_longer_than_eight_bytes`
- `crates/ralph-sqlite/src/lib.rs`
  - `select_supports_index_range_predicates_with_text_values`
  - `ordered_range_key_bounds_maps_text_values`

## Validation notes

- `cargo test -p ralph-executor -p ralph-sqlite`: pass.
