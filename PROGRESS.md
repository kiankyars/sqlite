# Progress

## Current Status

**Phase: Stage 5 (partial)** — Tokenizer/parser, pager, B+tree, schema table + catalog persistence integration, end-to-end CREATE/INSERT/SELECT/UPDATE/DELETE/`DROP TABLE`/`DROP INDEX` execution, SELECT `ORDER BY`/`LIMIT`/aggregates, WAL write-ahead commit path, WAL startup recovery/checkpoint, SQL transaction control (`BEGIN`/`COMMIT`/`ROLLBACK`), a standalone Volcano executor core (`Scan`/`Filter`/`Project`) with expression evaluation, and query planner index selection (equality + simple range) for SELECT/UPDATE/DELETE are implemented.

Latest completions:
- Full SQL parser with modular tokenizer, AST, and recursive-descent parser (Agent 1)
- Basic pager with buffer pool implemented in `crates/storage` (Agent 2)
- Page allocator with freelist-pop stub implemented in `crates/storage` (Agent 4)
- Pager freelist management API in `crates/storage` (Agent 3) — added `Pager::free_page()` with validation/duplicate detection and allocation-reuse persistence coverage
- B+tree with insert, point lookup, leaf-linked range scan, and splitting (Agent 2)
- Schema table (sqlite_master equivalent) with create/find/list operations (Agent 2)
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
- UPDATE/DELETE index selection in `crates/planner` + `crates/ralph-sqlite` (Agent opus) — added `plan_where` general-purpose planner entry point; UPDATE and DELETE now use planner-driven index selection instead of unconditional full table scans; index consistency maintained for indexed column value changes
- Schema persistence integration in `crates/storage` + `crates/ralph-sqlite` (Agent codex) — `Database::open` now loads persisted table/index catalogs from schema entries, `CREATE TABLE`/`CREATE INDEX` now persist metadata via `Schema`, and reopen retains both table and index catalogs
- Transactional dirty-page eviction isolation in `crates/storage` (Agent codex) — dirty LRU victims now spill to an in-memory pending-dirty map instead of writing directly to the DB file; `flush_all` now commits both buffered and spilled dirty pages via WAL, preserving rollback correctness when eviction occurs before COMMIT
- B+tree delete freelist reclamation in `crates/storage` (Agent 3) — delete-time compaction now returns removed leaf/interior/root-child pages to `Pager::free_page()` so reclaimed pages are reusable via the freelist
- DROP TABLE execution + object-tree reclamation in `crates/ralph-sqlite` + `crates/storage` (Agent codex) — `DROP TABLE` now removes schema entries and dependent index metadata, then reclaims table/index B+tree pages through a new `BTree::reclaim_tree` helper so pages return to the freelist
- DROP INDEX SQL execution in `crates/parser` + `crates/ralph-sqlite` (Agent codex) — parser now supports `DROP INDEX [IF EXISTS]`, integration now executes index drops via schema removal plus `BTree::reclaim_tree` page reclamation, and query paths fall back to table scans after index removal
- Range predicate index selection in `crates/planner` + `crates/ralph-sqlite` (Agent 3) — planner now emits `IndexRange` access paths for indexed `<`/`<=`/`>`/`>=`/`BETWEEN` predicates (including reversed comparisons), and SELECT/UPDATE candidate reads consume planner range paths with residual WHERE filtering

Test pass rate:
- `./test.sh` (full, DROP INDEX execution): pass, 5/5 passed.
- `cargo test --workspace` (DROP INDEX execution): pass, 0 failed (156 tests).
- `cargo test -p ralph-parser -p ralph-sqlite` (DROP INDEX execution): pass, 0 failed.
- `./test.sh --fast` (DROP INDEX execution, seed: 4): pass, 0 failed, 5 skipped (deterministic sample).
- `cargo test --workspace` (DROP TABLE + reclamation): pass, 0 failed (151 tests).
- `cargo test -p ralph-storage` (DROP TABLE + reclamation): pass, 0 failed (51 tests).
- `cargo test -p ralph-sqlite` (DROP TABLE + reclamation): pass, 0 failed (28 tests).
- `./test.sh --fast` (DROP TABLE + reclamation, seed: 4): pass, 0 failed, 5 skipped (deterministic sample).
- `cargo test --workspace` (range predicate index selection): pass, 0 failed (157 tests).
- `cargo test -p ralph-planner -p ralph-sqlite` (range predicate index selection): pass, 0 failed (43 tests).
- `./test.sh --fast` (range predicate index selection, AGENT_ID=3): pass, 0 failed, 4 skipped (deterministic sample).
- `cargo test -p ralph-storage` (B+tree delete freelist reclamation): pass, 0 failed (46 tests).
- `./test.sh --fast` (B+tree delete freelist reclamation, AGENT_ID=3): pass, 0 failed, 4 skipped (deterministic sample).
- `cargo test -p ralph-storage` (freelist management): pass, 0 failed (43 tests).
- `cargo test -p ralph-storage -p ralph-sqlite` (schema persistence integration): pass, 0 failed.
- `cargo test --workspace` (schema persistence integration): pass, 0 failed.
- `./test.sh --fast` (schema persistence integration): pass, 0 failed, 5 skipped (deterministic sample).
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
- `cargo test --workspace` (UPDATE/DELETE index selection): pass, 0 failed (131 tests).
- `./test.sh` (full, UPDATE/DELETE index selection): 5/5 passed.
- `cargo test -p ralph-storage` (dirty-eviction isolation): pass, 0 failed (47 tests).
- `cargo test --workspace` (dirty-eviction isolation): pass, 0 failed.
- `./test.sh --fast` (dirty-eviction isolation, seed: 4): pass, 0 failed, 5 skipped (deterministic sample).

## Prioritized Task Backlog

1. ~~SQL tokenizer (lexer) in `ralph-parser`~~ ✓
2. ~~SQL parser for CREATE TABLE, INSERT, SELECT~~ ✓
3. ~~AST type definitions~~ ✓
4. ~~Basic pager: read/write 4KB pages, file header~~ ✓
5. ~~Page allocator with freelist stub~~ ✓
6. ~~B+tree insert and point lookup~~ ✓
7. ~~B+tree leaf-linked range scan~~ ✓
8. ~~Schema table storage~~ ✓
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
20. ~~Transactional dirty-page eviction isolation~~ ✓
21. ~~DROP TABLE execution + schema/index page reclamation~~ ✓
22. ~~DROP INDEX SQL execution + index-tree page reclamation~~ ✓

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
- [x] Freelist management API in pager (agent 3)
  - Added `Pager::free_page(page_num)` to recycle pages back onto the freelist
  - `free_page` validates page range, rejects duplicate free requests, and relinks freed pages as freelist head pages
  - Added pager tests for reuse ordering, invalid/duplicate free rejection, and free-list persistence across reopen
- [x] B+tree insert, point lookup, range scan, and leaf splitting (agent 2)
  - Insert with automatic leaf/interior node splitting
  - Point lookup via tree traversal
  - Full scan and range scan via leaf-linked list
  - Update (delete + re-insert) for existing keys
  - Tested with up to 200 entries (multi-level splits), reverse-order inserts, persistence after flush
  - 10 B+tree unit tests
- [x] Schema table storage — sqlite_master equivalent (agent 2)
  - SchemaEntry: object type, name, root_page, SQL text, column definitions
  - Binary serialization/deserialization of schema entries
  - Schema::initialize, create_table, find_table, list_tables
  - Duplicate table detection, persistence across close/reopen
  - 6 schema unit tests
- [x] Schema persistence integration in `ralph-sqlite` (agent codex)
  - Added storage schema APIs for indexes: `create_index`, `find_index`, `list_indexes`
  - `Database::open` now initializes schema root (if needed) and rebuilds in-memory table/index catalogs from persisted schema entries
  - `CREATE TABLE` and `CREATE INDEX` now persist metadata through `ralph_storage::Schema`
  - Added reopen integration tests for table/index catalog persistence; see `notes/schema-persistence-integration.md`
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
- [x] B+tree delete compaction freelist reclamation (agent 3)
  - Wired `Pager::free_page()` into delete compaction paths so removed leaf/interior pages are returned to freelist
  - Added `delete_compaction_reclaims_pages_to_freelist` coverage in storage tests
  - See `notes/btree-delete-freelist-reclamation.md`
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
  - Wired into `ralph-sqlite` SELECT execution: implemented `TableScan` and `IndexEqScan` in `ralph-executor` with lifetime support, and updated `execute_select` to build physical operator trees
- [x] Query planner index selection (agent codex)
  - Replaced planner stub with `plan_select` access-path planning (`TableScan` vs. `IndexEq`)
  - Planner recognizes indexable predicates of the form `col = constant` (including reversed equality and inside `AND`)
  - SELECT execution now requests planner output and performs index rowid lookups when planned
  - Added UPDATE/DELETE index maintenance so secondary indexes remain consistent when indexed column values change or rows are removed
  - Added planner unit tests and integration tests for update/delete index maintenance; see `notes/query-planner-index-selection.md`
- [x] Query planner range predicate index selection (agent 3)
  - Added `AccessPath::IndexRange` planning for indexed `<`/`<=`/`>`/`>=` and non-negated `BETWEEN` predicates, including reversed comparisons
  - Added `ralph-sqlite` range-candidate row reads for planner-driven SELECT/UPDATE/DELETE paths
  - Added planner unit tests plus integration coverage for SELECT and UPDATE range predicates
  - See `notes/query-planner-range-selection.md` for implementation details and current hash-index limitation
- [x] Expression evaluation in `crates/executor` (agent codex)
  - Added `eval_expr(&Expr, row_ctx)` support for literals, column refs, unary/binary ops, `IS NULL`, `BETWEEN`, and `IN (...)`
  - Added `Filter::from_expr(...)` and `Project::from_exprs(...)` helpers to evaluate parser AST expressions in execution pipelines
  - Added executor tests for arithmetic/boolean evaluation, row-context column resolution, expression-backed filter/project, and unknown-column errors
- [x] Checkpoint + crash recovery in `crates/storage` (agent codex)
  - Added WAL replay during `Pager::open*()` so committed WAL frames are recovered into the DB file before serving reads
  - Added startup header reload after WAL replay so in-memory header metadata reflects recovered page 0 state
  - Added `Pager::checkpoint() -> io::Result<usize>` to flush pending dirty pages, checkpoint committed WAL frames, and truncate WAL
  - Added storage tests for committed-frame recovery, uncommitted-tail ignore behavior, checkpoint WAL truncation, and recovered header reload
- [x] UPDATE/DELETE planner-driven index selection (agent opus)
  - Added `plan_where(where_clause, table_name, indexes) -> AccessPath` general-purpose planner API
  - Updated `execute_update` and `execute_delete` to call planner and use `read_candidate_entries` helper for index-driven row selection
  - Full WHERE predicate still re-applied after index probe for correctness
  - Added 3 planner tests and 3 integration tests; see `notes/update-delete-index-selection.md`
- [x] Transactional dirty-page eviction isolation in pager (agent codex)
  - Dirty pages evicted from a full buffer pool are now spilled into an in-memory pending-dirty map instead of being written directly to the DB file
  - `ensure_loaded` now reloads spilled dirty pages before disk reads so uncommitted changes stay visible inside the current transaction
  - `flush_all` now WAL-commits and applies both in-pool dirty pages and spilled dirty pages
  - Added pager tests for dirty-page visibility after eviction and non-durability across reopen without commit; see `notes/wal-eviction-transactional-correctness.md`
- [x] DROP TABLE execution + object-tree reclamation (agent codex)
  - Added `Stmt::DropTable` execution with `IF EXISTS` behavior and `ExecuteResult::DropTable`
  - Added schema deletion APIs: `Schema::drop_table`, `Schema::drop_index`, `Schema::list_indexes_for_table`
  - Added `BTree::reclaim_tree` to free full table/index trees back to the freelist during object drop
  - Added storage + integration coverage; see `notes/drop-table-page-reclamation.md`
- [x] DROP INDEX SQL execution + index-tree reclamation (agent codex)
  - Added parser support for `DROP INDEX [IF EXISTS]` via `Stmt::DropIndex`
  - Added `ExecuteResult::DropIndex` and integration execution path that removes schema metadata + in-memory catalog entries
  - Reuses `BTree::reclaim_tree` to reclaim dropped index pages to the freelist
  - Added parser + integration coverage; see `notes/drop-index-sql-execution.md`

## Known Issues

- Dirty-page eviction now preserves rollback correctness by spilling uncommitted page bytes in memory; long-running write transactions can still increase memory usage if many dirty pages are evicted before commit.
- B+tree delete rebalance currently compacts only empty-node underflow; occupancy-based redistribution/merge policy is not implemented.
- UPDATE/DELETE use index-driven row selection when a suitable equality or simple range index predicate exists; they fall back to full table scan otherwise.
- Query planning currently supports single-table equality and simple range predicates on single-column secondary indexes; OR, multi-index, and cost-based planning are not implemented.
- Range index planning currently does full index-bucket scans because secondary index keys are hash-based; true ordered range seeks are not implemented.
- No GROUP BY / HAVING parsing yet (keywords defined but parser logic not implemented)
- No JOIN support (single-table FROM only)
- No subquery support
- Multi-column and UNIQUE index execution are not supported yet.
- Aggregate queries do not support `GROUP BY`/`HAVING`; column references outside aggregate functions are rejected in aggregate SELECTs.
