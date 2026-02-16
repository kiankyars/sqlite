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
- `HEX`
- `QUOTE`
- `MIN` (multi-argument scalar)
- `MAX` (multi-argument scalar)

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
