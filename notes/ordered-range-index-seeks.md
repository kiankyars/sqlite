# Ordered Range Index Seeks

## Scope completed

Reworked secondary-index keying and range read paths so range predicates can use
true B+tree key-range scans for numeric bounds instead of always scanning every
index bucket.

## Changes

- `crates/executor`
  - `index_key_for_value` now uses an order-preserving numeric key for
    `INTEGER`/`REAL` values.
  - Added `ordered_index_key_for_value` helper for callers that need to know
    whether a value can participate in ordered key-range seeks.
  - Kept hash-key fallback for non-orderable values (currently `TEXT` and
    non-finite numeric edge cases such as `NaN`).
- `crates/ralph-sqlite`
  - `index_range_rowids` now computes key bounds and uses
    `BTree::scan_range(min_key, max_key)` when both bounds are orderable.
  - Retains full-index scan fallback when either bound is non-orderable.
  - Existing per-bucket value comparisons and rowid de-duplication remain in
    place, so inclusive/exclusive SQL range semantics are still enforced by
    value-level filtering.

## Tests added

- `crates/executor/src/lib.rs`
  - `ordered_index_key_is_monotonic_for_numeric_values`
  - `ordered_index_key_ignores_text_values`
- `crates/ralph-sqlite/src/lib.rs`
  - `select_supports_index_range_predicates_with_real_values`
  - `ordered_range_key_bounds_falls_back_for_text_bounds`
  - `ordered_range_key_bounds_maps_numeric_values`

## Validation notes

- `cargo test -p ralph-executor`: pass.
- `cargo test -p ralph-planner`: pass.
- `cargo test -p ralph-sqlite`: pass.
- `./test.sh --fast` (seed: 3): pass (sample skipped build/unit sections).
