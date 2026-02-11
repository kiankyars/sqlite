# Progress

## Current Status

**Phase: Stage 1 (in progress)** — Tokenizer implementation has started in `ralph-parser`.

Latest completion (2026-02-11):
- Implemented SQL tokenizer (lexer) in `crates/parser`:
  - Case-insensitive keyword recognition
  - Identifiers (including quoted identifiers)
  - Integer/real literals (including exponent form)
  - String literals with doubled-quote escaping
  - Core punctuation/operators and `?` placeholder token
  - `--` line comments and `/* ... */` block comments
  - Error reporting with byte positions for invalid/unterminated lexemes
- Tests:
  - `cargo test`: 13/13 tests passing across workspace
  - `./test.sh --fast`: 1 passed, 0 failed, 4 skipped (deterministic sample)

## Prioritized Task Backlog

1. SQL parser for CREATE TABLE, INSERT, SELECT
2. AST type definitions
3. Basic pager: read/write 4KB pages, file header
4. Page allocator with freelist stub
5. B+tree insert and point lookup
6. B+tree leaf-linked range scan
7. Schema table storage
8. End-to-end: CREATE TABLE + INSERT + SELECT
9. Volcano iterator model (Scan, Filter, Project)
10. Expression evaluation
11. UPDATE and DELETE execution
12. Secondary indexes (CREATE INDEX)
13. Query planner (index selection)
14. WAL write path and commit
15. Checkpoint and crash recovery
16. BEGIN/COMMIT/ROLLBACK SQL
17. B+tree split/merge
18. ORDER BY, LIMIT, aggregates

## Completed Tasks

- [x] Project skeleton: Cargo workspace with 5 crates (parser, planner, executor, storage, ralph-sqlite)
- [x] Stub implementations with passing unit tests
- [x] README.md, DESIGN.md, PROGRESS.md documentation
- [x] test.sh harness with --fast mode and sqlite3 oracle integration
- [x] Lock-file protocol defined in DESIGN.md
- [x] .gitignore configured for build artifacts and logs
- [x] SQL tokenizer (lexer) implemented in `ralph-parser` with unit tests

## Known Issues

- None yet (bootstrap only).
