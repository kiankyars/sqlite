# Progress

## Current Status

**Phase: Stage 1 (in progress)** — tokenizer, core parser, and basic pager are implemented.

Latest completions (2026-02-11):
- SQL tokenizer (lexer) implemented in `crates/parser`.
- Parser + AST implemented for `CREATE TABLE`, `INSERT`, and `SELECT` in `crates/parser`.
- Basic pager with buffer pool implemented in `crates/storage`.

Current test pass rate:
- `cargo test --workspace`: passing.
- `./test.sh` (full): 5/5 passed (latest full-harness run from pager task).
- `./test.sh --fast` (AGENT_ID=4): pass, 0 failed, deterministic sampling skipped all checks.

## Prioritized Task Backlog

1. Page allocator with freelist stub
2. B+tree insert and point lookup
3. B+tree leaf-linked range scan
4. Schema table storage
5. End-to-end: CREATE TABLE + INSERT + SELECT
6. Volcano iterator model (Scan, Filter, Project)
7. Expression evaluation
8. UPDATE and DELETE execution
9. Secondary indexes (CREATE INDEX)
10. Query planner (index selection)
11. WAL write path and commit
12. Checkpoint and crash recovery
13. BEGIN/COMMIT/ROLLBACK SQL
14. B+tree split/merge
15. ORDER BY, LIMIT, aggregates

## Completed Tasks

- [x] Project skeleton: Cargo workspace with 5 crates (parser, planner, executor, storage, ralph-sqlite)
- [x] Stub implementations with passing unit tests
- [x] README.md, DESIGN.md, PROGRESS.md documentation
- [x] test.sh harness with --fast mode and sqlite3 oracle integration
- [x] Lock-file protocol defined in DESIGN.md
- [x] .gitignore configured for build artifacts and logs
- [x] SQL tokenizer (lexer) implemented in `ralph-parser` with unit tests
- [x] Parser + AST for `CREATE TABLE`, `INSERT`, `SELECT` in `crates/parser`
- [x] Basic pager with buffer pool, LRU eviction, dirty tracking in `crates/storage`

## Known Issues

- Parser scope is intentionally narrow (no WHERE/JOIN/ORDER BY/UPDATE/DELETE parsing yet).
- Parser currently has its own token handling path and should be reconciled with shared tokenizer types.
- Pager currently extends file for allocation; freelist reuse is still pending.
