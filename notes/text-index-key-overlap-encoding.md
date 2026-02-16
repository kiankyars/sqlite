# Text Index Key Overlap Encoding

## Scope

Improve `TEXT` ordered index keying beyond the previous fixed 8-byte prefix
approach while preserving non-decreasing key order required by `IndexRange`
seeks.

## Implementation

- Updated `crates/executor/src/lib.rs` (`ordered_text_key`).
- New mapping:
  - Keep bytes `0..7` (first 7 bytes) exact.
  - Use byte 8 as an overlap channel (`b8 - 1` to `b8`) and drive selection
    with a suffix bit derived from byte 9 (`b9 >= 0x70`).
- This preserves non-decreasing order:
  - Different first-7-byte prefixes remain fully ordered.
  - Different 8th-byte values map to ordered, overlapping one-step intervals.
  - Same first-8-byte prefixes can split into two keys via the suffix bit.

## Why this shape

- A strict 64-bit key cannot encode a full 8-byte prefix plus arbitrary suffix
  ordering signal without tradeoffs.
- The overlap-channel approach keeps range correctness guarantees and adds a
  bounded amount of >8-byte discrimination, reducing collisions for some long
  shared-prefix text ranges.

## Tradeoffs

- Collisions are reduced, not eliminated.
- Some adjacent 8th-byte prefixes now collide (intentional overlap), exchanged
  for suffix-based discrimination when first 8 bytes are identical.
- Value-level filtering remains the final correctness guard for strict SQL
  predicate semantics.

## Tests

- `crates/executor/src/lib.rs`
  - `ordered_index_key_distinguishes_some_long_text_suffixes_beyond_eight_bytes`
  - `ordered_index_key_is_non_decreasing_for_sorted_text_series`
- `crates/ralph-sqlite/src/lib.rs`
  - `ordered_range_key_bounds_split_long_text_suffixes`
