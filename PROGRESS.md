# Progress

## Current Status

**Phase: Stage 5 (partial)** — tokenizer/parser, pager, B+tree, end-to-end CREATE/INSERT/SELECT/UPDATE/DELETE execution, SELECT `ORDER BY`/`LIMIT`/aggregates, WAL write-ahead commit path, WAL startup recovery/checkpoint, SQL transaction control (`BEGIN`/`COMMIT`/`ROLLBACK`), a standalone Volcano executor core (`Scan`/`Filter`/`Project`) with expression evaluation, and basic query planner index selection are implemented; schema persistence remains.

Latest completions:
- Full SQL parser with modular tokenizer, AST, and recursive-descent parser (Agent 1) — replaces prior implementations with comprehensive coverage of 6 statement types, full expression parsing with operator precedence, WHERE/ORDER BY/LIMIT/OFFSET
- Basic pager with buffer pool implemented in `crates/storage` (Agent 2)
- Page allocator with freelist-pop stub implemented in `crates/storage` (Agent 4)
- B+tree with insert, point lookup, leaf-linked range scan, and splitting (Agent 2)
- End-to-end `CREATE TABLE` + `INSERT` + `SELECT` path in `crates/ralph-sqlite` (Agent 4)
- B+tree delete primitive for UPDATE/DELETE groundwork (Agent 3) — key removal via tree descent to target leaf, with unit tests for single-leaf and split-tree deletes (no rebalance/merge yet)
- End-to-end `UPDATE` + `DELETE` execution in `crates/ralph-sqlite` (Agent codex) — WHERE filtering + assignment evaluation wired to B+tree row updates/deletes, with affected-row counts and integration tests
- Secondary indexes with `CREATE INDEX` execution, backfill, and insert-time maintenance in `crates/ralph-sqlite` (Agent 4)
- WAL write path + commit in `crates/storage` (Agent codex) — WAL sidecar file format, page/commit frames with checksums, and write-ahead commit flow wired into SQL write statements
- SQL transaction control in parser + integration layer (Agent codex) — `BEGIN [TRANSACTION]`, `COMMIT [TRANSACTION]`, `ROLLBACK [TRANSACTION]` parsing/execution with autocommit gating and rollback-to-snapshot behavior for connection-local catalogs
- SELECT `ORDER BY` execution in `crates/ralph-sqlite` (Agent 3) — supports expression sort keys (including non-projected columns), ASC/DESC multi-key ordering, and preserves `LIMIT/OFFSET` after sort
- SELECT aggregate execution in `crates/ralph-sqlite` (Agent codex) — supports `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` (no `GROUP BY`) with NULL-aware semantics and single-row aggregate output
- Volcano iterator model in `crates/executor` (Agent codex) — added `Operator` trait and concrete `Scan`, `Filter`, and `Project` operators with callback-based predicate/projection hooks and pipeline tests
- Expression evaluation in `crates/executor` (Agent codex) — added parser-AST expression evaluation plus expression-backed `Filter`/`Project` constructors for row predicates and projection materialization
- B+tree delete rebalance/merge for empty-node underflow with root compaction in `crates/storage` (Agent codex)
- Query planner index selection in `crates/planner` + `crates/ralph-sqlite` (Agent codex) — planner now selects index equality access paths for simple `WHERE` predicates, SELECT execution consumes planner output for indexed rowid lookup, and UPDATE/DELETE maintain secondary index entries
- Checkpoint + crash recovery in `crates/storage` (Agent codex) — pager now replays committed WAL frames on open, reloads recovered header state, and exposes `Pager::checkpoint()` to truncate WAL after checkpointing committed frames

Test pass rate:
- `cargo test --workspace` (task #15 implementation): pass, 0 failed.
- `cargo test --workspace` (task #17 implementation): pass, 0 failed.
- `cargo test --workspace` (task #18 implementation): pass, 0 failed.
- `cargo test -p ralph-storage` (task #18 implementation): pass, 0 failed (29 tests).
- `./test.sh --fast` (AGENT_ID=4): pass, 0 failed, 5 skipped (deterministic sample).
- `./test.sh --fast` (AGENT_ID=3): pass, 0 failed, 4 skipped (deterministic sample).
- `./test.sh --fast` (task #17 verification, AGENT_ID=3): pass, 0 failed, 4 skipped (deterministic sample).
- `./test.sh` (full): 5/5 passed (latest known full-harness run).
- `cargo test --workspace` (task #19 ORDER BY slice): pass, 0 failed.
- `./test.sh --fast` (task #19 ORDER BY slice, AGENT_ID=3): pass, 0 failed, 4 skipped (deterministic sample).
- `cargo test --workspace` (task #19 aggregate slice): pass, 0 failed.
- `./test.sh --fast` (task #19 aggregate slice): pass, 0 failed, 4 skipped (deterministic sample).
- `cargo test -p ralph-executor` (task #10 implementation): pass, 0 failed.
- `./test.sh --fast` (task #10 completion, AGENT_ID=3): pass, 0 failed, 4 skipped (deterministic sample).
- `cargo test -p ralph-planner -p ralph-sqlite` (task #14 implementation): pass, 0 failed.
- `./test.sh --fast` (task #14 verification): pass, 0 failed, 4 skipped (deterministic sample).
- `cargo test -p ralph-executor` (task #11 implementation): pass, 0 failed (11 tests).
- `cargo test --workspace` (task #11 implementation): pass, 0 failed.
- `./test.sh --fast` (task #11 verification, AGENT_ID=11): pass, 0 failed, 4 skipped (deterministic sample).
- `cargo test -p ralph-storage` (task #16 implementation): pass, 0 failed (35 tests).
- `cargo test --workspace` (task #16 implementation): pass, 0 failed.

## Prioritized Task Backlog

1. ~~SQL tokenizer (lexer) in `ralph-parser`~~ ✓
2. ~~SQL parser for CREATE TABLE, INSERT, SELECT~~ ✓
3. ~~AST type definitions~~ ✓
4. ~~Basic pager: read/write 4KB pages, file header~~ ✓
5. ~~Page allocator with freelist stub~~ ✓
6. ~~B+tree insert and point lookup~~ ✓
7. ~~B+tree leaf-linked range scan~~ ✓
8. Schema table storage
9. ~~End-to-end: CREATE TABLE + INSERT + SELECT~~ ✓
10. ~~Volcano iterator model (Scan, Filter, Project)~~ ✓
11. ~~Expression evaluation~~ ✓
12. ~~UPDATE and DELETE execution~~ ✓
13. ~~Secondary indexes (CREATE INDEX)~~ ✓
14. ~~Query planner (index selection)~~ ✓
15. ~~WAL write path and commit~~ ✓
16. ~~Checkpoint and crash recovery~~ ✓
17. ~~BEGIN/COMMIT/ROLLBACK SQL~~ ✓
18. ~~B+tree split/merge~~ ✓
19. ~~ORDER BY, LIMIT, aggregates~~ ✓

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
- [x] End-to-end SQL execution path for `CREATE TABLE`, `INSERT`, and `SELECT` in `crates/ralph-sqlite` (agent 4)
  - Added `Database` API with SQL parsing + statement dispatch
  - Rows are encoded into B+tree payloads with typed value tags (`NULL`, `INTEGER`, `REAL`, `TEXT`)
  - Supports `SELECT *`, projected expressions, simple `WHERE`, and `LIMIT/OFFSET` (no `ORDER BY` yet)
  - 3 new integration-focused unit tests in `crates/ralph-sqlite/src/lib.rs`
  - See `notes/end-to-end-create-insert-select.md` for implementation details and limitations
- [x] B+tree delete primitive (agent 3)
  - Added `BTree::delete(key) -> io::Result<bool>` to remove keys from the target leaf
  - Traverses interior nodes to locate the leaf; returns `false` when key is absent
  - Added tests for deleting existing/missing keys and deleting after leaf splits
- [x] B+tree delete rebalance/merge for empty-node underflow (agent codex)
  - Added recursive delete underflow propagation for leaf and interior pages
  - Added parent-level rebalancing: remove/compact empty leaf children and collapse empty interior children to their remaining subtree
  - Added root compaction that preserves root page number by copying the only child page into the root when root has 0 separator keys
  - Added storage tests for root compaction on split and multi-level trees; see `notes/btree-delete-rebalance.md`
- [x] End-to-end UPDATE/DELETE execution in `crates/ralph-sqlite` (agent codex)
  - Added statement dispatch for `Stmt::Update` / `Stmt::Delete`
  - Added `ExecuteResult::Update { rows_affected }` and `ExecuteResult::Delete { rows_affected }`
  - Reused expression evaluation for `WHERE` predicates and UPDATE assignment values
  - Added integration tests: update with WHERE, delete with WHERE, and full-table update/delete
- [x] Secondary indexes (`CREATE INDEX`) in parser + integration layer (agent 4)
  - Added `CREATE INDEX` / `CREATE UNIQUE INDEX` parser support with `IF NOT EXISTS`
  - Added `Database` execution support for `CREATE INDEX` (single-column indexes)
  - Index build backfills existing rows; `INSERT` now maintains indexes for indexed tables
  - Added index payload encoding that handles duplicate values and hash-bucket collisions
  - 2 new integration tests and 3 parser tests; see `notes/secondary-indexes.md`
- [x] WAL write path and commit in `crates/storage` (agent codex)
  - Added `wal.rs` sidecar WAL implementation (`<db-path>-wal`) with header, page frames, and commit frames
  - Added checksum validation helpers and WAL page-size/version guards
  - Updated `Pager::flush_all()` to write dirty pages to WAL and `fsync` WAL before applying to DB file
  - Added `Pager::commit()` and used it in SQL write statement execution paths
  - Added storage tests for WAL frame format/checksums and multi-commit WAL append behavior
- [x] BEGIN/COMMIT/ROLLBACK SQL (agent codex)
  - Added parser support for `BEGIN [TRANSACTION]`, `COMMIT [TRANSACTION]`, and `ROLLBACK [TRANSACTION]`
  - Added `Database` execution support with explicit transaction state tracking and autocommit gating for write statements
  - `ROLLBACK` restores connection-local table/index catalogs from a BEGIN snapshot and reopens the pager to drop uncommitted in-memory page changes
  - Added parser tests and integration tests for deferred WAL writes, rollback behavior, and transaction state errors
- [x] SELECT aggregate execution (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) in `crates/ralph-sqlite` (agent codex)
  - Added aggregate-aware SELECT execution path for non-`GROUP BY` queries, including aggregate expressions such as `COUNT(*) + 1`
  - Added NULL-aware aggregate semantics over empty inputs (`COUNT` -> `0`, others -> `NULL`)
  - Added integration tests for table-backed aggregates, no-`FROM` aggregates, and mixed aggregate/non-aggregate rejection without `GROUP BY`
- [x] Volcano iterator model (`Scan`, `Filter`, `Project`) in `crates/executor` (agent codex)
  - Replaced executor stub with `Operator` trait (`open`/`next`/`close`) and concrete operators
  - Added callback-based predicate/projection hooks so expression semantics can be layered by task #11
  - Added unit tests for lifecycle behavior, composition (`Scan -> Filter -> Project`), and error propagation
- [x] Query planner index selection (agent codex)
  - Replaced planner stub with `plan_select` access-path planning (`TableScan` vs. `IndexEq`)
  - Planner recognizes indexable predicates of the form `col = constant` (including reversed equality and inside `AND`)
  - SELECT execution now requests planner output and performs index rowid lookups when planned
  - Added UPDATE/DELETE index maintenance so secondary indexes remain consistent when indexed column values change or rows are removed
  - Added planner unit tests and integration tests for update/delete index maintenance; see `notes/query-planner-index-selection.md`
- [x] Expression evaluation in `crates/executor` (agent codex)
  - Added `eval_expr(&Expr, row_ctx)` support for literals, column refs, unary/binary ops, `IS NULL`, `BETWEEN`, and `IN (...)`
  - Added `Filter::from_expr(...)` and `Project::from_exprs(...)` helpers to evaluate parser AST expressions in execution pipelines
  - Added executor tests for arithmetic/boolean evaluation, row-context column resolution, expression-backed filter/project, and unknown-column errors
- [x] Checkpoint + crash recovery in `crates/storage` (agent codex)
  - Added WAL replay during `Pager::open*()` so committed WAL frames are recovered into the DB file before serving reads
  - Added startup header reload after WAL replay so in-memory header metadata reflects recovered page 0 state
  - Added `Pager::checkpoint() -> io::Result<usize>` to flush pending dirty pages, checkpoint committed WAL frames, and truncate WAL
  - Added storage tests for committed-frame recovery, uncommitted-tail ignore behavior, checkpoint WAL truncation, and recovered header reload

## Known Issues

- Pager has freelist-pop reuse, but there is no public `free_page()` API yet.
- Dirty-page eviction still flushes directly to the DB file; WAL is guaranteed on explicit commit/flush path.
- Explicit transaction rollback does not undo dirty-page eviction writes that already reached the DB file; rollback reliably discards uncommitted pages that stayed buffered.
- B+tree delete rebalance currently compacts only empty-node underflow; occupancy-based redistribution/merge policy is not implemented.
- UPDATE/DELETE currently run as full table scans (no index-based row selection yet).
- Query planning is currently limited to single-table equality predicates on single-column secondary indexes; range, OR, multi-index, and cost-based planning are not implemented.
- No GROUP BY / HAVING parsing yet (keywords defined but parser logic not implemented)
- No JOIN support (single-table FROM only)
- No subquery support
- Table catalog is currently connection-local in `ralph-sqlite`; schema metadata persistence is pending task #8.
- Index catalog is currently connection-local in `ralph-sqlite`; persistence is pending task #8.
- Multi-column and UNIQUE index execution are not supported yet.
- Aggregate queries do not support `GROUP BY`/`HAVING`; column references outside aggregate functions are rejected in aggregate SELECTs.
- `crates/executor` Volcano operators are currently in-memory and not yet wired into `ralph-sqlite` SELECT planning/execution.
