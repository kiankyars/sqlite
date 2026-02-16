# LIKE Operator Fix

## Summary

Fixed LIKE pattern matching to use correct SQL semantics instead of naive `String::contains`.

## Changes

### `crates/executor/src/lib.rs`
- Added `pub fn sql_like_match(haystack, pattern) -> bool` using a DP-based matcher
- `%` matches zero or more characters, `_` matches exactly one character
- Matching is case-insensitive for ASCII (SQLite default behavior)
- NULL operands now return NULL (previously would stringify NULL and match incorrectly)

### `crates/ralph-sqlite/src/lib.rs`
- Updated `eval_binary_op` LIKE branch to use `sql_like_match` from executor
- Added NULL handling (returns NULL instead of incorrect match)
- Imports `sql_like_match` from executor crate

## Tests Added

### Executor (7 tests)
- `like_exact_match` — exact string equality via LIKE
- `like_case_insensitive` — ASCII case folding
- `like_percent_wildcard` — prefix/suffix/contains/trailing %
- `like_underscore_wildcard` — single-char matching
- `like_combined_wildcards` — mixed `%` and `_`
- `like_empty_patterns` — empty string / empty pattern edge cases
- `like_null_operands_return_null` — NULL propagation

### Integration (3 tests)
- `like_filters_rows_with_pattern_matching` — end-to-end LIKE with prefix/suffix/contains/underscore/case patterns
- `not_like_filters_rows_inversely` — NOT LIKE integration
- `like_with_null_returns_no_match` — NULL column values filtered out by LIKE

## Previous Behavior

The old LIKE implementation simply stripped `%` from the pattern and used `String::contains`, which:
- Treated `%` as a no-op (not as a wildcard)
- Ignored `_` wildcards entirely
- Was case-sensitive
- Did not handle NULL operands correctly

## Implementation Approach

Used a bottom-up DP approach (O(n*m) time and space) rather than recursive backtracking to avoid worst-case exponential behavior on patterns like `%a%a%a%a%...`.
