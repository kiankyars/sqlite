# SQL Parser Implementation

The `ralph-parser` crate converts raw SQL text into an Abstract Syntax Tree (AST).

## Module Structure

- **`token.rs`**: `Token` and `Keyword` enums.
- **`tokenizer.rs`**: Hand-written lexer that produces a stream of `Token`s.
- **`parser.rs`**: Recursive-descent parser that consumes tokens and produces AST nodes.
- **`ast.rs`**: Definitions for statement types (`SelectStmt`, `InsertStmt`, etc.) and expression nodes (`Expr`).
- **`lib.rs`**: Public `parse(input: &str)` API.

## Supported SQL Features

- **SELECT**: Column expressions, aliases, `*`, `FROM`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`.
- **JOIN**: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`, and comma cross-joins.
- **INSERT**: `INSERT INTO ... [(cols)] VALUES (...)`.
- **UPDATE**: `UPDATE ... SET col=expr WHERE ...`.
- **DELETE**: `DELETE FROM ... WHERE ...`.
- **CREATE TABLE**: `IF NOT EXISTS`, column types, and constraints (PRIMARY KEY, UNIQUE, NOT NULL, AUTOINCREMENT, DEFAULT).
- **CREATE INDEX**: `IF NOT EXISTS`, `UNIQUE`, multi-column index definitions.
- **DROP TABLE / DROP INDEX**: `IF EXISTS`.
- **Transactions**: `BEGIN`, `COMMIT`, `ROLLBACK`.

## Expression Precedence (low to high)

1. OR
2. AND
3. NOT
4. Comparison (=, !=, <, <=, >, >=, LIKE, IS NULL, BETWEEN, IN)
5. Addition/Subtraction/Concat (+, -, ||)
6. Multiplication/Division/Modulo (*, /, %)
7. Unary (-, +)
8. Primary (literals, column refs, function calls, parenthesized)

## Tokenization and Lexing

- **Keywords**: Case-insensitive.
- **Identifiers**: Quoted with `""` for case-sensitive or reserved words.
- **Literals**: Numeric (integer and real with exponent support), string (single quotes, doubled `''` for escaping).
- **Comments**: Supports `-- line comments` and `/* block comments */`.

## Design Decisions

- **Recursive Descent**: Chosen for flexibility and easy error reporting.
- **Hand-written Tokenizer**: Avoids dependencies on lexer generators and handles SQL-specific rules (like string escaping) cleanly.
- **SQLite Compatibility**: Mimics SQLite's behavior for most common SQL constructs.
- **Position-aware Errors**: `LexError` and `ParseError` include byte offsets for precise error reporting.

## Abstract Syntax Tree (AST)

The parser produces AST nodes defined in `ast.rs`. The top-level node is `Stmt`, which has variants for all supported SQL commands. Expressions are represented by the `Expr` enum, supporting literals, column references, unary/binary operations, and more complex constructs like `BETWEEN`, `IN`, and `IS NULL`.
