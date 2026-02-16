## AST Types Handoff (2026-02-11)

Context: while this task was in progress, another agent landed parser + AST support for stage-1 SQL in `origin/main`. This handoff reflects the merged state and the additional AST validation tests added in this task.

### Current AST surface (`crates/parser/src/ast.rs`)

- `Statement` variants:
  - `CreateTable(CreateTableStatement)`
  - `Insert(InsertStatement)`
  - `Select(SelectStatement)`
- Supporting types:
  - `CreateTableStatement { table_name, columns }`
  - `ColumnDef { name, data_type }`
  - `InsertStatement { table_name, columns, values }`
  - `SelectStatement { projection, from }`
  - `SelectItem`, `Expr`, `BinaryOperator`

### Added in this task

- Unit tests in `crates/parser/src/ast.rs` for:
  - CREATE TABLE AST structural equality
  - INSERT AST field population
  - SELECT AST projection/FROM representation

### Follow-up suggestion

- If parser coverage expands (WHERE, ORDER BY, LIMIT, constraints), extend AST nodes and keep parser tests aligned in `crates/parser/src/lib.rs`.
