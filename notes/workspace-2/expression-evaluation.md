# Expression Evaluation (Task #11)

## Scope completed

Implemented parser-AST-driven expression evaluation in `crates/executor/src/lib.rs`:

- Added `eval_expr(&Expr, row_ctx)` returning executor `Value`.
- Supported expression forms:
  - literals (`INTEGER`, `REAL`, `TEXT`, `NULL`)
  - column references (by provided column-name vector, case-insensitive)
  - unary ops (`-`, `NOT`)
  - binary ops (`+`, `-`, `*`, `/`, `%`, comparisons, `AND`/`OR`, `LIKE`, `||`)
  - `IS NULL` / `IS NOT NULL`
  - `BETWEEN` / `NOT BETWEEN`
  - `IN (...)` / `NOT IN (...)`

## Operator integration

Added expression-backed constructors while keeping callback APIs:

- `Filter::from_expr(input, predicate_expr, columns)`
- `Project::from_exprs(input, projection_exprs, columns)`

These evaluate parser `Expr` nodes directly on executor rows.

## Tests added

In `crates/executor/src/lib.rs`:

- `eval_expr_handles_arithmetic_and_boolean_ops`
- `eval_expr_resolves_columns_from_row_context`
- `filter_from_expr_applies_sql_predicate`
- `project_from_exprs_materializes_expression_outputs`
- `eval_expr_errors_on_unknown_column`

All existing Volcano pipeline tests continue to pass.

## Behavior notes / limitations

- Table-qualified column references currently return an explicit executor error.
- SQL functions are still unsupported in executor expression evaluation.
- The evaluator is implemented in `crates/executor`, but planner/sqlite integration to produce expression-based pipelines remains task #14.
