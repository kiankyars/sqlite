# Progress

## Current Status

**Phase: Stage 2 (Storage)** — tokenizer, parser/AST, pager, page allocator freelist stub, and B+tree are implemented.

Latest completions:
- SQL tokenizer (lexer) implemented in `crates/parser` (agent 3)
- Parser + AST implemented for `CREATE TABLE`, `INSERT`, and `SELECT` in `crates/parser` (agents 3/4)
- Basic pager with buffer pool implemented in `crates/storage` (agent 2)
- Page allocator with freelist-pop stub implemented in `crates/storage` (agent 4)
- B+tree with insert, point lookup, leaf-linked range scan, and splitting (agent 2)

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
- [x] SQL tokenizer (lexer) implemented in `ralph-parser` with unit tests
- [x] Parser + AST for `CREATE TABLE`, `INSERT`, `SELECT` in `crates/parser`
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

- Parser scope is intentionally narrow (no WHERE/JOIN/ORDER BY/UPDATE/DELETE parsing yet).
- Parser currently has its own token handling path and should be reconciled with shared tokenizer types.
- Pager has freelist-pop reuse, but there is no public `free_page()` API yet.
