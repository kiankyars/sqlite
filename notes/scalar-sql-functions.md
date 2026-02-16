<<<<<<< HEAD
# Scalar SQL Functions Execution

## Summary

Implemented core scalar SQL function evaluation across both expression engines:

- `crates/executor/src/lib.rs`
- `crates/ralph-sqlite/src/lib.rs`

Supported scalar functions now include:

- `LENGTH`
- `UPPER`
- `LOWER`
- `TYPEOF`
- `ABS`
- `COALESCE`
- `IFNULL`
- `NULLIF`
- `SUBSTR`
- `INSTR`
- `REPLACE`
- `TRIM`
- `LTRIM`
- `RTRIM`

## Execution-path integration

`ralph-sqlite` now routes `Expr::FunctionCall` through a shared scalar-function evaluator for:

- regular row expressions (`eval_expr`)
- join row expressions (`eval_join_expr`)
- grouped expressions (`eval_grouped_expr` / `eval_grouped_join_expr`)
- aggregate-query expression evaluation (`eval_aggregate_expr` / `eval_join_aggregate_expr`) when the function is not an aggregate

This allows scalar wrappers around aggregates (for example: `COALESCE(MAX(v), 0)`) to evaluate correctly.

## Oracle check

Compared representative outputs with `sqlite3`:

- `LENGTH`, `UPPER`, `LOWER`, `SUBSTR`, `INSTR`, `TRIM`, `REPLACE`, `TYPEOF`
- `COALESCE`, `IFNULL`, `NULLIF`, `ABS`
- scalar wrapper over aggregate: `COALESCE(MAX(v), 0)`

## Tests added

### `crates/executor/src/lib.rs`

- `eval_expr_supports_scalar_functions`
- `eval_expr_scalar_function_errors_for_unsupported_name`

### `crates/ralph-sqlite/src/lib.rs`

- `scalar_functions_execute_in_selects`
- `scalar_functions_handle_null_and_aggregate_wrapping`

## Limitations

- Scalar `MIN`/`MAX` (multi-argument form) is not yet implemented; `MIN`/`MAX` are still treated as aggregate functions.
- `HEX`, `QUOTE`, and some SQLite type-affinity casting edge cases are not yet implemented.
=======
# Scalar SQL Functions

## Summary

Added scalar SQL function evaluation to both `crates/executor` and `crates/ralph-sqlite`, enabling function calls like `LENGTH()`, `UPPER()`, `ABS()`, `COALESCE()`, etc. in SELECT, WHERE, and JOIN contexts.

## Supported Functions

- **String**: `LENGTH`, `UPPER`, `LOWER`, `TRIM`, `LTRIM`, `RTRIM`, `SUBSTR`/`SUBSTRING`, `REPLACE`, `INSTR`, `HEX`, `QUOTE`
- **Numeric**: `ABS`
- **Type**: `TYPEOF`
- **Character**: `UNICODE`, `CHAR`
- **Conditional**: `COALESCE`, `IFNULL`, `NULLIF`
- **Multi-value**: `MIN` (2+ args), `MAX` (2+ args)

## Implementation

- `eval_scalar_function(name, args)` in `crates/executor/src/lib.rs` dispatches by uppercase function name
- Both `eval_expr` (single-table) and `eval_join_expr` (join context) in `ralph-sqlite` now delegate `Expr::FunctionCall` to the executor's scalar function evaluator
- All functions handle NULL propagation per SQLite semantics

## NULL Comparison Fix

During this work, a pre-existing bug was discovered: comparison operators (`<`, `<=`, `>`, `>=`) would error with "cannot compare values of different types" when one operand was NULL. In SQL, any comparison with NULL should return NULL. Fixed in both `crates/executor/src/lib.rs` and `crates/ralph-sqlite/src/lib.rs`.

## Tests

- 3 integration tests in `ralph-sqlite`: `scalar_functions_in_select`, `scalar_functions_on_table_columns`, `scalar_functions_in_join_context`
- Verified against `sqlite3` behavioral oracle
>>>>>>> 663e618 (feat: add scalar SQL functions and fix NULL comparison propagation)
