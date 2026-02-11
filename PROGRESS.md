# Progress

## Current Status

**Phase: Stage 1 (in progress)** — Tokenizer complete, storage pager complete. Parser next.

Latest completions:
- SQL tokenizer (lexer) implemented in `crates/parser` (agent 3)
- Basic pager with buffer pool implemented in `crates/storage` (agent 2)

Test pass rate: 5/5 (full harness), all cargo tests passing.

## Prioritized Task Backlog

1. ~~SQL tokenizer (lexer) in `ralph-parser`~~ ✓
2. SQL parser for CREATE TABLE, INSERT, SELECT
3. AST type definitions
4. ~~Basic pager: read/write 4KB pages, file header~~ ✓
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
- [x] SQL tokenizer (lexer) implemented in `ralph-parser` with unit tests
- [x] Basic pager with buffer pool, LRU eviction, dirty tracking (agent 2)
  - File header: magic, page_size, page_count, freelist_head/count, schema_root (100 bytes, big-endian)
  - Pager: read/write pages, pin/unpin, flush_all, configurable pool size
  - Page allocation: extends file (freelist reuse deferred to task #5)
  - 13 unit tests covering: create/reopen, read/write, persistence, multi-alloc, LRU eviction, pinning, header flush

## Known Issues

- None.
