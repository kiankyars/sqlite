# Parser Handoff: CREATE/INSERT/SELECT

## What was implemented

- Added AST types in `crates/parser/src/ast.rs`.
- Added a parser in `crates/parser/src/parser.rs` with support for:
  - `CREATE TABLE table_name (col_name TYPE, ...)`
  - `INSERT INTO table_name [(col, ...)] VALUES (expr, ...)`
  - `SELECT expr[, ...] [FROM table_name]`
  - `SELECT * [FROM table_name]`
- Exposed `parse()` and `ParseError` from `crates/parser/src/lib.rs`.
- Added parser unit tests in `crates/parser/src/lib.rs`.

## Current behavior notes

- Statement terminator `;` is optional.
- Keywords are case-insensitive.
- String literals support SQLite-style escaped single quote via doubled quote (`''`).
- Expression support is minimal: identifier, integer, string, and `+`.

## Coordination note

- This parser currently embeds its own lexer to keep the task self-contained.
- Another active task (`sql-tokenizer`) should reconcile lexer ownership/API and remove duplication if needed.
