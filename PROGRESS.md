# Progress

## Current Status

**Phase: Stage 2 (Storage)** — tokenizer, parser/AST, pager, page allocator freelist stub, and B+tree are implemented.

Latest completions:
- Full SQL parser with modular tokenizer, AST, and recursive-descent parser (Agent 1) — replaces prior implementations with comprehensive coverage of 6 statement types, full expression parsing with operator precedence, WHERE/ORDER BY/LIMIT/OFFSET
- Basic pager with buffer pool implemented in `crates/storage` (Agent 2)
- Page allocator with freelist-pop stub implemented in `crates/storage` (Agent 4)
- B+tree with insert, point lookup, leaf-linked range scan, and splitting (Agent 2)

Test pass rate:
- `cargo test --workspace`: passing.
- `./test.sh --fast` (AGENT_ID=4): pass, 0 failed, 5 skipped (deterministic sample).
- `./test.sh` (full): 5/5 passed (latest known full-harness run).

## Prioritized Task Backlog

1. ~~SQL tokenizer (lexer) in `ralph-parser`~~ ✓
2. ~~SQL parser for CREATE TABLE, INSERT, SELECT~~ ✓
3. ~~AST type definitions~~ ✓
4. ~~Basic pager: read/write 4KB pages, file header~~ ✓
5. ~~Page allocator with freelist stub~~ ✓
6. ~~B+tree insert and point lookup~~ ✓
7. ~~B+tree leaf-linked range scan~~ ✓
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
- [x] SQL tokenizer, AST types, and parser — comprehensive implementation (Agent 1)
    - Modular structure: token.rs, ast.rs, tokenizer.rs, parser.rs, lib.rs
    - 6 statement types: SELECT, INSERT, CREATE TABLE, UPDATE, DELETE, DROP TABLE
    - Full expression parsing with 7-level operator precedence
    - WHERE, ORDER BY, LIMIT, OFFSET, IS NULL, BETWEEN, IN, LIKE, aggregates
    - 43 unit tests — see `notes/parser-implementation.md`
- [x] Basic pager with buffer pool, LRU eviction, dirty tracking (agent 2)
  - File header: magic, page_size, page_count, freelist_head/count, schema_root (100 bytes, big-endian)
  - Pager: read/write pages, pin/unpin, flush_all, configurable pool size
  - 13 unit tests covering: create/reopen, read/write, persistence, multi-alloc, LRU eviction, pinning, header flush
- [x] Page allocator with freelist-pop reuse stub (agent 4)
  - `allocate_page()` now reuses freelist head pages before extending the file
  - Freelist next pointer read from bytes `0..4` (big-endian `u32`) of the freelist head page
  - Reused pages are zeroed before return; header freelist metadata is updated and validated
- [x] B+tree insert, point lookup, range scan, and leaf splitting (agent 2)
  - Insert with automatic leaf/interior node splitting
  - Point lookup via tree traversal
  - Full scan and range scan via leaf-linked list
  - Update (delete + re-insert) for existing keys
  - Tested with up to 200 entries (multi-level splits), reverse-order inserts, persistence after flush
  - 10 B+tree unit tests

## Known Issues

- Pager has freelist-pop reuse, but there is no public `free_page()` API yet.
- No GROUP BY / HAVING parsing yet (keywords defined but parser logic not implemented)
- No JOIN support (single-table FROM only)
- No subquery support
