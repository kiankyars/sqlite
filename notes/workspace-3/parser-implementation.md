# Parser Implementation Notes

## Module Structure

The `ralph-parser` crate is organized into 4 modules:

- **`token.rs`** — `Token` enum and `Keyword` enum with `from_str` lookup
- **`ast.rs`** — All AST node types: `Stmt`, `Expr`, `SelectStmt`, `InsertStmt`, `CreateTableStmt`, `UpdateStmt`, `DeleteStmt`, `DropTableStmt`, plus supporting types
- **`tokenizer.rs`** — `Tokenizer` struct that converts SQL text → `Vec<Token>`
- **`parser.rs`** — `Parser` struct that converts `Vec<Token>` → `Stmt`
- **`lib.rs`** — Public `parse(input: &str) -> Result<Stmt, String>` convenience function

## Supported SQL

- `SELECT` with expressions, `*`, aliases, `FROM`, `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`
- `INSERT INTO ... VALUES` with optional column list, multiple value rows
- `CREATE TABLE` with `IF NOT EXISTS`, column type names, constraints (PRIMARY KEY, AUTOINCREMENT, NOT NULL, UNIQUE, DEFAULT)
- `UPDATE ... SET ... WHERE`
- `DELETE FROM ... WHERE`
- `DROP TABLE` with `IF EXISTS`

## Expression Precedence (low to high)

1. OR
2. AND
3. NOT
4. Comparison (=, !=, <, <=, >, >=, LIKE, IS NULL, BETWEEN, IN)
5. Addition/Subtraction/Concat (+, -, ||)
6. Multiplication/Division/Modulo (*, /, %)
7. Unary (-, +)
8. Primary (literals, column refs, function calls, parenthesized)

## Design Decisions

- Keywords are case-insensitive (uppercased during tokenization lookup)
- `COUNT(*)` represents `*` as a `ColumnRef { column: "*" }` in the args
- String escaping uses SQLite's doubled-single-quote convention (`''`)
- Line comments (`--`) are supported
- The parser consumes optional trailing semicolons
- No `JOIN` support yet — single table FROM only

## Test Coverage

43 unit tests across tokenizer and parser modules covering:
- All statement types
- Operator precedence
- Complex WHERE clauses (AND/OR/NOT, IS NULL, BETWEEN, IN)
- Qualified column references (table.column)
- Aggregate functions
- Error cases

## What's Next

The parser is ready for integration with the planner and executor. Key items:
- The planner needs to consume `Stmt` and produce a plan
- `GROUP BY` / `HAVING` parsing is defined in tokens/keywords but not yet implemented in the parser
- `JOIN` support is not implemented
- Subqueries are not supported
