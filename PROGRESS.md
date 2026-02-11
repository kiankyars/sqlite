# Progress

## Current Status

**Phase: Bootstrap** — Project skeleton and documentation established. No implementation has begun.

The workspace builds, all stub tests pass, and the test harness is operational.

## Prioritized Task Backlog

1. SQL tokenizer (lexer) in `ralph-parser`
2. SQL parser for CREATE TABLE, INSERT, SELECT
3. AST type definitions
4. Basic pager: read/write 4KB pages, file header
5. Page allocator with freelist stub
6. B+tree insert and point lookup
7. B+tree leaf-linked range scan
8. Schema table storage
9. End-to-end: CREATE TABLE + INSERT + SELECT
10. Volcano iterator model (Scan, Filter, Project)
11. Expression evaluation
12. UPDATE and DELETE execution
13. Secondary indexes (CREATE INDEX)
14. Query planner (index selection)
15. WAL write path and commit
16. Checkpoint and crash recovery
17. BEGIN/COMMIT/ROLLBACK SQL
18. B+tree split/merge
19. ORDER BY, LIMIT, aggregates

## Completed Tasks

- [x] Project skeleton: Cargo workspace with 5 crates (parser, planner, executor, storage, ralph-sqlite)
- [x] Stub implementations with passing unit tests
- [x] README.md, DESIGN.md, PROGRESS.md documentation
- [x] test.sh harness with --fast mode and sqlite3 oracle integration
- [x] Lock-file protocol defined in DESIGN.md
- [x] .gitignore configured for build artifacts and logs

## Known Issues

- None yet (bootstrap only).
