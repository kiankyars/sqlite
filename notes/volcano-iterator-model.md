# Volcano Iterator Model (Task #10)

## Summary
- Implemented a minimal Volcano execution core in `crates/executor/src/lib.rs`.
- Added `Operator` trait with lifecycle methods:
  - `open()`
  - `next()`
  - `close()`
- Added concrete operators:
  - `Scan`: emits rows from an in-memory row vector
  - `Filter`: wraps a child operator and applies a predicate callback
  - `Project`: wraps a child operator and applies a projection callback
- Added `execute(root)` helper that opens, drains, and closes an operator pipeline.

## Data Types
- Added executor-local `Value` and `Row` abstractions to support pipeline tests and composition.
- Added `ExecutorError`/`ExecResult` for consistent error handling.

## Tests Added
- `scan_emits_rows_in_order`
- `scan_next_before_open_errors`
- `filter_selects_only_matching_rows`
- `project_transforms_rows`
- `scan_filter_project_pipeline`
- `predicate_error_is_returned`

## Scope and Follow-up
- Predicate/projection logic is callback-based on purpose, so SQL expression semantics stay in task #11.
- The executor pipeline is not yet integrated with `ralph-sqlite` statement execution.
- Planner integration (task #14) can target this trait by producing pipelines equivalent to Scan/Filter/Project.
