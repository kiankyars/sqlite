# Progress

## Current Status

**Phase: Stage 6 (partial)** — Tokenizer/parser, pager, B+tree, schema table + catalog persistence integration, end-to-end CREATE/INSERT/SELECT/UPDATE/DELETE/`DROP TABLE`/`DROP INDEX` execution, single-column and multi-column secondary index execution (including `UNIQUE` enforcement), SELECT `ORDER BY`/`LIMIT`/aggregates/`GROUP BY`/`HAVING`, INNER JOIN / CROSS JOIN / LEFT JOIN / RIGHT JOIN / FULL OUTER JOIN execution (with index-probed nested-loop for INNER/LEFT joins on indexed equality ON conditions), WAL write-ahead commit path, WAL startup recovery/checkpoint, SQL transaction control (`BEGIN`/`COMMIT`/`ROLLBACK`), a standalone Volcano executor core (`Scan`/`Filter`/`Project`) with expression evaluation, core scalar SQL function execution, and query planner index selection (single-column equality/`IN`/range + OR unions + AND intersections + multi-column equality/prefix-range) plus statistics-aware cost selection with persisted planner cardinality + prefix fanout metadata for SELECT/UPDATE/DELETE are implemented.

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
- SELECT `GROUP BY` / `HAVING` parser support in `crates/parser` (Agent 4) — added SELECT AST fields for grouping/filtering clauses, parser support for `GROUP BY ...` and `HAVING ...`, and integration guards in `crates/ralph-sqlite` to return explicit unsupported errors until grouped execution is implemented
- B+tree delete leaf occupancy rebalance in `crates/storage` (Agent codex) — delete underflow now triggers on low logical leaf occupancy (not only empty pages), with sibling merge when combined pages fit and sibling redistribution plus parent separator-key updates when they do not
- B+tree delete interior occupancy rebalance in `crates/storage` (Agent codex) — interior delete underflow now triggers on low logical interior occupancy (not only empty pages), with sibling merge/redistribution and parent separator-key updates
- SELECT `GROUP BY` / `HAVING` execution semantics in `crates/ralph-sqlite` (Agent codex) — added grouped row execution for table-backed and scalar no-`FROM` queries, per-group aggregate/non-aggregate expression evaluation, HAVING filtering, and grouped ORDER BY support; HAVING without GROUP BY now behaves as aggregate-only and GROUP BY rejects aggregate expressions
- Ordered range index seeks for numeric bounds in `crates/executor` + `crates/ralph-sqlite` (Agent codex) — index keying now uses order-preserving numeric keys for `INTEGER`/`REAL`, and `IndexRange` candidate reads now use `BTree::scan_range` when bounds are orderable
- Ordered range index seeks for text bounds in `crates/executor` + `crates/ralph-sqlite` (Agent codex) — text index keys now use an order-preserving 8-byte prefix encoding so `IndexRange` plans can seek on text bounds (with value-level filtering retained for strict bound semantics)
- Text index overlap key encoding for long shared prefixes in `crates/executor` + `crates/ralph-sqlite` (Agent codex) — `TEXT` ordered keys now keep the first 7 bytes exact and use an overlap-channel low byte with one suffix-threshold bit from byte 9, preserving non-decreasing ordering while reducing a subset of >8-byte collisions during range seeks
- Single-column `UNIQUE` index execution in `crates/ralph-sqlite` (Agent codex) — `CREATE UNIQUE INDEX` now builds/enforces unique secondary indexes, `INSERT`/`UPDATE` reject duplicate non-`NULL` keys with SQLite-style errors, and index uniqueness now persists across reopen via schema SQL parsing
- INNER JOIN / CROSS JOIN execution in `crates/parser` + `crates/ralph-sqlite` (Agent opus) — parser now supports `JOIN`, `INNER JOIN`, `CROSS JOIN`, and comma cross join syntax with ON conditions and table aliases; execution performs nested-loop cross-product joins with ON/WHERE filtering, qualified column resolution, and full ORDER BY/LIMIT support
- Multi-column secondary index execution in `crates/ralph-sqlite` + `crates/storage` (Agent codex) — `CREATE INDEX`/`CREATE UNIQUE INDEX` now execute for multi-column definitions with tuple-based backfill + INSERT/UPDATE/DELETE maintenance and tuple UNIQUE enforcement (`NULL`-tolerant), with schema reload preserving behavior across reopen
- JOIN `GROUP BY` / `HAVING` aggregate execution in `crates/ralph-sqlite` (Agent codex) — join SELECT path now supports grouped and aggregate evaluation with HAVING/ORDER BY/LIMIT/OFFSET semantics, including aggregate HAVING without GROUP BY and SQLite-style bare-column aggregate errors; see `notes/join-group-by-having-execution.md`
- OR predicate index-union planning/execution in `crates/planner` + `crates/ralph-sqlite` (Agent codex) — planner now emits `IndexOr` when all OR branches are indexable (including mixed equality/range branches), and SELECT/UPDATE/DELETE candidate reads now union + deduplicate branch rowids before residual WHERE filtering; see `notes/query-planner-range-selection.md`
- Multi-column predicate planner/execution support in `crates/planner` + `crates/ralph-sqlite` (Agent codex) — planner now emits composite `IndexEq` access paths when all indexed columns have equality predicates, and SELECT/UPDATE/DELETE now evaluate multi-expression probes against tuple index keys; see `notes/multi-column-index-planner-selection.md`
- LEFT JOIN parser/execution support in `crates/parser` + `crates/ralph-sqlite` (Agent codex) — parser now supports `LEFT JOIN` and `LEFT OUTER JOIN`, and join execution now preserves unmatched left rows via right-side NULL extension; see `notes/left-join-execution.md`
- RIGHT JOIN parser/execution support in `crates/parser` + `crates/ralph-sqlite` (Agent codex) — parser now supports `RIGHT JOIN` and `RIGHT OUTER JOIN`, and join execution now preserves unmatched right rows via left-side NULL extension; see `notes/right-join-execution.md`
- FULL OUTER JOIN parser/execution support in `crates/parser` + `crates/ralph-sqlite` (Agent codex) — parser now supports `FULL JOIN` and `FULL OUTER JOIN`, and join execution now preserves unmatched rows from both sides via null extension; see `notes/full-outer-join-execution.md`
- `IN` multi-probe planner/execution support in `crates/planner` + `crates/ralph-sqlite` (Agent 4) — planner now maps indexable `col IN (...)` predicates to deduplicated equality probe unions (`IndexEq`/`IndexOr`), and SELECT/UPDATE/DELETE reuse existing candidate-row union/dedup paths for index-driven execution; see `notes/in-multi-probe-planner-execution.md`
- Multi-index `AND`-intersection planning/execution in `crates/planner` + `crates/ralph-sqlite` (Agent codex) — planner now emits `IndexAnd` for multi-term indexable conjunctions, execution now intersects branch-selected rowids before table lookup for SELECT/UPDATE/DELETE candidate reads, and composite equality indexes remain preferred when available; see `notes/multi-index-and-intersection-selection.md`
- Planner cost heuristics for table-scan vs index-path selection in `crates/planner` (Agent 3) — planner now estimates static access-path costs and falls back to `TableScan` for high-fanout `IndexOr`/`IndexAnd` shapes while keeping small fanout index probes; see `notes/planner-cost-heuristics-selection.md`
- Multi-column prefix/range planner/execution support in `crates/planner` + `crates/ralph-sqlite` (Agent 4) — planner now emits `IndexPrefixRange` for left-prefix equality predicates (with optional range bounds on the next index column), and SELECT/UPDATE/DELETE now evaluate those candidates against decoded tuple buckets from multi-column indexes; see `notes/multi-column-prefix-range-planner-execution.md`
- Multi-column prefix/range scan-reduction heuristics in `crates/planner` + `crates/ralph-sqlite` (Agent 4) — planner now penalizes weak `IndexPrefixRange` probes that imply full composite-index scans, falls back to `TableScan` for low-selectivity prefix-only forms, and retains bounded prefix/range usage; see `notes/multi-column-prefix-range-scan-reduction.md`
- Planner statistics-driven cost model for table-scan vs index-path selection in `crates/planner` + `crates/ralph-sqlite` (Agent 3) — added `plan_where_with_stats`/`plan_select_with_stats` plus runtime table/index cardinality hints from `ralph-sqlite`, with legacy static heuristics preserved when stats are absent; see `notes/planner-statistics-cost-model.md`
- Persisted planner statistics metadata in `crates/storage` + `crates/ralph-sqlite` (Agent 4) — schema now persists table/index planner stats entries, planner stats now load from persisted metadata instead of per-query scans, and write paths refresh/drop stats metadata on CREATE/INSERT/UPDATE/DELETE/DROP; see `notes/persisted-planner-statistics.md`
- Planner stats selectivity/cost refinement in `crates/planner` (Agent codex) — stats-aware `AND` path preference now compares candidate costs before picking `IndexAnd` vs simpler equality paths, and stats-based `IndexOr`/`IndexAnd` row estimation now combines branch selectivities using probability unions/intersections instead of sum/min heuristics; see `notes/planner-stats-selectivity-cost-refinement.md`
- LIKE operator fix in `crates/executor` + `crates/ralph-sqlite` (Agent opus) — replaced naive `String::contains` LIKE implementation with correct SQL pattern matching: `%` matches zero-or-more chars, `_` matches one char, case-insensitive ASCII matching per SQLite defaults, and NULL operand propagation; see `notes/like-operator-fix.md`
- Planner histogram/fanout statistics for multi-column prefix/range costing in `crates/storage` + `crates/planner` + `crates/ralph-sqlite` (Agent codex) — persisted index stats now include per-prefix distinct-count vectors, stats-aware `IndexPrefixRange` costing now estimates eq-prefix fanout and range selectivity from prefix-level distributions, and write-path stats refresh now recomputes/persists prefix distinct counts; see `notes/planner-histogram-fanout-stats.md`
- Scalar SQL function execution in `crates/executor` + `crates/ralph-sqlite` (Agent codex) — added core scalar functions (`LENGTH`/`UPPER`/`LOWER`/`TYPEOF`/`ABS`/`COALESCE`/`IFNULL`/`NULLIF`/`SUBSTR`/`INSTR`/`REPLACE`/`TRIM`/`LTRIM`/`RTRIM`) and wired scalar-call evaluation across regular/join/grouped/aggregate expression paths; scalar wrappers over aggregate calls now evaluate correctly (for example `COALESCE(MAX(v), 0)`); see `notes/scalar-sql-functions.md`
- Join index probe optimization in `crates/ralph-sqlite` (Agent opus) — INNER/LEFT joins now use index-probed nested-loop when the ON condition is a simple equality on an indexed right-table column; RIGHT/FULL outer joins and non-equality ON conditions fall back to full-scan nested-loop; see `notes/join-index-probe-optimization.md`

Recommended next step:
- Extend join index probes to support AND conjunctions in ON conditions and multi-column indexes.

Test pass rate:
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-planner -p ralph-storage -p ralph-sqlite` (planner histogram/fanout stats): pass, 0 failed (191 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast` (planner histogram/fanout stats, seed: 3): pass, 0 failed, 4 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast` (scalar SQL function execution, seed: 4): pass, 0 failed, 5 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-executor` (scalar SQL function execution): pass, 0 failed (24 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-sqlite` (scalar SQL function integration): pass, 0 failed (97 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target-2 cargo test --workspace` (LIKE operator fix): pass, 0 failed (282 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target-2 cargo test -p ralph-executor` (LIKE operator fix): pass, 0 failed (22 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target-2 cargo test -p ralph-sqlite` (LIKE operator fix): pass, 0 failed (95 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target-2 ./test.sh --fast` (LIKE operator fix, seed: 2): pass, 0 failed, 4 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test --workspace` (join index probe optimization): pass, 0 failed (281 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-sqlite` (join index probe optimization): pass, 0 failed (101 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast` (join index probe optimization, seed: 1): pass, 0 failed, 4 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-planner` (planner stats selectivity/cost refinement): pass, 0 failed (38 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast` (planner stats selectivity/cost refinement, seed: 3): pass, 0 failed, 4 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-parser -p ralph-sqlite` (RIGHT/FULL OUTER JOIN merged verification): pass, 0 failed (161 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast` (RIGHT/FULL OUTER JOIN merged verification, seed: 3): pass, 0 failed, 4 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-storage -p ralph-sqlite` (persisted planner statistics metadata): pass, 0 failed (146 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast` (persisted planner statistics metadata, seed: 4): pass, 0 failed, 5 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-parser -p ralph-sqlite` (RIGHT JOIN parser/execution support): pass, 0 failed (157 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast` (RIGHT JOIN parser/execution support, seed: 4): pass, 0 failed, 5 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-parser -p ralph-sqlite` (FULL OUTER JOIN parser/execution support): pass, 0 failed (157 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast` (FULL OUTER JOIN parser/execution support, seed: 3): pass, 0 failed, 4 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-planner` (multi-column prefix/range scan-reduction heuristics): pass, 0 failed (33 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-sqlite` (planner/execution compatibility for prefix/range scan-reduction): pass, 0 failed (86 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test --workspace` (full regression after prefix/range scan-reduction): pass, 0 failed (256 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast` (prefix/range scan-reduction, seed: 4): pass, 0 failed, 5 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-planner` (planner statistics-driven cost model): pass, 0 failed (31 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-sqlite` (planner statistics integration): pass, 0 failed (82 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast` (planner statistics integration, seed: 3): pass, 0 failed, 4 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-planner` (planner cost heuristics): pass, 0 failed (29 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-sqlite` (planner cost heuristics compatibility): pass, 0 failed (82 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast` (planner cost heuristics, seed: 3): pass, 0 failed, 4 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-planner -p ralph-sqlite` (multi-index AND-intersection support): pass, 0 failed (100 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast` (multi-index AND-intersection support, seed: 3): pass, 0 failed, 4 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-planner -p ralph-sqlite` (multi-column prefix/range planner/execution support): pass, 0 failed (115 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast` (multi-column prefix/range planner/execution support, seed: 4): pass, 0 failed, 5 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-parser -p ralph-sqlite` (LEFT JOIN parser/execution support): pass, 0 failed (140 tests).
- `./test.sh --fast` (LEFT JOIN parser/execution support, seed: 3): pass, 0 failed, 4 skipped (deterministic sample).
- `cargo test -p ralph-planner` (`IN` multi-probe planner support): pass, 0 failed (24 tests).
- `cargo test -p ralph-sqlite` (`IN` multi-probe integration coverage): pass, 0 failed (79 tests).
- `./test.sh --fast` (`IN` multi-probe verification, seed: 4): pass, 0 failed, 5 skipped (deterministic sample).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target cargo test -p ralph-planner -p ralph-sqlite` (OR predicate index-union support): pass, 0 failed (88 tests).
- `CARGO_TARGET_DIR=/tmp/ralph-sqlite-target ./test.sh --fast` (OR predicate index-union support, seed: 3): pass, 0 failed, 4 skipped (deterministic sample).
- `cargo test -p ralph-planner -p ralph-sqlite` (multi-column predicate planner/execution support): pass, 0 failed (86 tests).
- `cargo test --workspace` (multi-column predicate planner/execution support): pass, 0 failed (221 tests).
- `./test.sh --fast` (multi-column predicate planner/execution support, seed: 4): pass, 0 failed, 5 skipped (deterministic sample).
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-Ccodegen-units=1 -Cdebuginfo=0' cargo test -p ralph-sqlite` (JOIN `GROUP BY` / `HAVING` execution): pass, 0 failed (68 tests).
- `CARGO_INCREMENTAL=0 RUSTFLAGS='-Ccodegen-units=1 -Cdebuginfo=0' ./test.sh --fast` (JOIN `GROUP BY` / `HAVING` execution, seed: 3): pass, 0 failed, 4 skipped (deterministic sample).
- `cargo test -p ralph-executor` (text index overlap key encoding): pass, 0 failed (15 tests).
- `cargo test -p ralph-sqlite` (text index overlap key encoding): pass, 0 failed (59 tests).
- `./test.sh --fast` (text index overlap key encoding, seed: 3): pass, 0 failed, 4 skipped (deterministic sample).
- `cargo test -p ralph-sqlite -p ralph-storage` (multi-column secondary index execution): pass, 0 failed (119 tests).
- `cargo test --workspace` (multi-column secondary index execution): pass, 0 failed (213 tests).
- `./test.sh --fast` (multi-column secondary index execution, seed: 4): pass, 0 failed, 5 skipped (deterministic sample).
- `cargo test --workspace` (INNER JOIN / CROSS JOIN execution): pass, 0 failed (205 tests).
- `cargo test -p ralph-parser` (INNER JOIN / CROSS JOIN parser): pass, 0 failed (65 tests).
- `cargo test -p ralph-sqlite` (INNER JOIN / CROSS JOIN execution): pass, 0 failed (58 tests).
- `./test.sh --fast` (INNER JOIN / CROSS JOIN execution, seed: 1): pass, 0 failed, 4 skipped (deterministic sample).
- `./test.sh` (full, INNER JOIN / CROSS JOIN execution): 5/5 passed.
- `cargo test -p ralph-executor -p ralph-sqlite` (ordered text range index seeks): pass, 0 failed (56 tests).
- `cargo test --workspace` (single-column UNIQUE index execution): pass, 0 failed (189 tests).
- `cargo test -p ralph-sqlite` (single-column UNIQUE index execution): pass, 0 failed (49 tests).
- `./test.sh --fast` (single-column UNIQUE index execution, AGENT_ID=4): pass, 0 failed, 5 skipped (deterministic sample).
- `cargo test -p ralph-executor` (ordered range index seek keying): pass, 0 failed (13 tests).
- `cargo test -p ralph-planner` (post-range-seek sanity): pass, 0 failed (13 tests).
- `cargo test -p ralph-sqlite` (ordered range index seeks): pass, 0 failed (41 tests).
- `./test.sh --fast` (ordered range index seeks, seed: 3): pass, 0 failed, 4 skipped (deterministic sample).
- `cargo test -p ralph-storage` (B+tree interior occupancy rebalance): pass, 0 failed (55 tests).
- `./test.sh --fast` (B+tree interior occupancy rebalance, seed: 4): pass, 0 failed, 5 skipped (deterministic sample).
- `cargo test -p ralph-storage` (B+tree leaf occupancy rebalance): pass, 0 failed (53 tests).
- `./test.sh --fast` (B+tree leaf occupancy rebalance, seed: 3): pass, 0 failed, 4 skipped (deterministic sample).
- `cargo test -p ralph-sqlite` (GROUP BY/HAVING execution semantics): pass, 0 failed (38 tests).
- `cargo test --workspace` (GROUP BY/HAVING execution semantics): pass, 0 failed (172 tests).
- `./test.sh --fast` (GROUP BY/HAVING execution semantics, seed: 4): pass, 0 failed, 5 skipped (deterministic sample).
- `cargo test -p ralph-parser -p ralph-planner -p ralph-sqlite` (GROUP BY/HAVING parser support): pass, 0 failed.
- `./test.sh --fast` (GROUP BY/HAVING parser support, seed: 4): pass, 0 failed, 5 skipped (deterministic sample).
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
23. ~~SELECT `GROUP BY` / `HAVING` parser support + integration guardrails~~ ✓
24. ~~SELECT `GROUP BY` / `HAVING` execution semantics~~ ✓
25. ~~Ordered range index seeks for index range predicates~~ ✓
26. ~~B+tree interior occupancy rebalance~~ ✓
27. ~~Ordered text range index seeks~~ ✓
28. ~~Single-column UNIQUE index execution~~ ✓
29. ~~INNER JOIN / CROSS JOIN execution~~ ✓
30. ~~Text index overlap key encoding for long shared prefixes~~ ✓
31. ~~JOIN `GROUP BY` / `HAVING` aggregate execution~~ ✓
32. ~~OR predicate index-union planning/execution~~ ✓
33. ~~Planner/execution support for multi-column index equality predicates~~ ✓
34. ~~LEFT JOIN parser/execution support~~ ✓
35. ~~Planner/execution support for multi-index AND-intersection predicates~~ ✓
36. ~~Simple planner cost heuristics for table scan vs index path selection~~ ✓
37. ~~Planner/execution support for multi-column index left-prefix/range predicates~~ ✓
38. ~~Planner statistics-driven cost model for table scan vs index path selection~~ ✓
39. ~~RIGHT JOIN parser/execution support~~ ✓
40. ~~FULL OUTER JOIN parser/execution support~~ ✓
41. ~~Persisted planner statistics metadata~~ ✓
42. ~~Planner stats selectivity/cost refinement~~ ✓
43. ~~LIKE operator correctness fix~~ ✓
44. ~~Planner histogram/fanout statistics for multi-column prefix/range cost estimation~~ ✓
45. ~~Core scalar SQL function execution~~ ✓
46. ~~Join index probe optimization for INNER/LEFT joins~~ ✓

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
- [x] B+tree delete leaf occupancy rebalance (agent codex)
  - Added leaf underflow detection based on logical live-cell utilization (35% threshold), not just empty-page checks
  - Added sibling merge/redistribution for non-empty underfull leaves with parent separator-key updates on redistribution
  - Added storage tests for non-empty merge and redistribution paths; see `notes/btree-delete-occupancy-rebalance.md`
- [x] B+tree delete interior occupancy rebalance (agent codex)
  - Added interior underflow detection based on logical interior utilization (35% threshold), not just empty-page checks
  - Added generalized interior sibling merge/redistribution with parent separator-key updates
  - Added storage tests for delete-triggered non-empty interior rebalance and redistribution overflow path; see `notes/btree-delete-interior-occupancy-rebalance.md`
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
- [x] OR predicate index-union planning/execution (agent codex)
  - Added `AccessPath::IndexOr` planning for OR predicates when all OR branches are individually indexable
  - Added execution union/dedup candidate reads for OR branches with residual WHERE filtering preserved
  - Added planner tests and integration coverage for SELECT/UPDATE/DELETE OR predicates; see `notes/query-planner-range-selection.md`
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
- [x] SELECT `GROUP BY` / `HAVING` parser support + integration guardrails (agent 4)
  - Added `group_by` and `having` fields to `SelectStmt`
  - Added parser support for `GROUP BY` expression lists and optional `HAVING` expressions
  - Added `ralph-sqlite` guardrails that return explicit errors for grouped queries until grouped execution semantics are implemented
  - Added parser/planner/integration tests; see `notes/group-by-having-parser.md`
- [x] SELECT `GROUP BY` / `HAVING` execution semantics (agent codex)
  - Added grouped execution path in `execute_select` with GROUP BY key formation, HAVING filtering, and grouped ORDER BY expression evaluation
  - Added grouped expression evaluation that supports aggregate and non-aggregate projection expressions per group
  - Added aggregate-query HAVING behavior for no-`GROUP BY` queries and SQLite-style non-aggregate HAVING error reporting
  - Added integration coverage for grouped aggregates, grouped dedup projection, no-`GROUP BY` HAVING, `GROUP BY` aggregate-expression rejection, and scalar no-`FROM` grouping; see `notes/group-by-having-execution.md`
- [x] Ordered range index seeks for index range predicates (agent codex)
  - `index_key_for_value` now emits order-preserving keys for numeric values while preserving hash-key fallback for non-orderable values
  - `IndexRange` candidate reads now use `BTree::scan_range` for orderable bounds, with full index scan fallback otherwise
  - Added executor and integration coverage; see `notes/ordered-range-index-seeks.md`
- [x] Ordered text range index seeks (agent codex)
  - `ordered_index_key_for_value` now emits order-preserving keys for `TEXT` values via an 8-byte lexicographic prefix encoding
  - Existing `IndexRange` candidate reads now perform B+tree range seeks for text bounds, with value-level filtering retained for strict comparison semantics
  - Added executor and integration coverage; see `notes/ordered-text-range-index-seeks.md`
- [x] Text index overlap key encoding for long shared prefixes (agent codex)
  - Updated `ordered_index_key_for_value` for `TEXT` to keep first 7 bytes exact and use an overlap-channel low byte with a suffix-threshold bit from byte 9
  - Preserved non-decreasing key ordering for `IndexRange` scans while reducing a subset of >8-byte prefix collisions
  - Added executor + integration coverage; see `notes/text-index-key-overlap-encoding.md`
- [x] Single-column UNIQUE index execution (agent codex)
  - `CREATE UNIQUE INDEX` now executes for single-column indexes with duplicate backfill validation
  - `INSERT`/`UPDATE` now enforce unique secondary index constraints for non-`NULL` values
  - Unique-index enforcement now persists across reopen by parsing index schema SQL on catalog load
  - Added integration coverage; see `notes/unique-index-execution.md`
- [x] INNER JOIN / CROSS JOIN execution (agent opus)
  - Added `Join`, `Inner`, `Cross` keywords and `JoinClause`/`JoinType` AST types
  - Parser now supports `FROM a, b`, `FROM a JOIN b ON ...`, `FROM a INNER JOIN b ON ...`, `FROM a CROSS JOIN b`, multi-table chains, and table aliases
  - Execution performs nested-loop cross-product joins with ON/WHERE filtering and table-qualified column resolution
  - Added 6 parser tests and 9 integration tests; see `notes/inner-join-execution.md`
- [x] LEFT JOIN parser/execution support (agent codex)
  - Added `LEFT` / `OUTER` parser keywords and `JoinType::Left` for `LEFT JOIN` and `LEFT OUTER JOIN`
  - Join execution now preserves unmatched left rows by NULL-extending right columns when ON predicates find no matches
  - Added parser + integration coverage; see `notes/left-join-execution.md`
- [x] RIGHT JOIN parser/execution support (agent codex)
  - Added `RIGHT` parser keyword and `JoinType::Right` for `RIGHT JOIN` and `RIGHT OUTER JOIN`
  - Join execution now preserves unmatched right rows by NULL-extending the left-side columns when ON predicates find no matches
  - Added parser + integration coverage; see `notes/right-join-execution.md`
- [x] FULL OUTER JOIN parser/execution support (agent codex)
  - Added `FULL` parser keyword and `JoinType::Full` for `FULL JOIN` and `FULL OUTER JOIN`
  - Join execution now preserves unmatched rows from both sides by NULL-extending missing columns after ON-condition matching
  - Added parser + integration coverage; see `notes/full-outer-join-execution.md`
- [x] Multi-column secondary index execution (agent codex)
  - `CREATE INDEX` / `CREATE UNIQUE INDEX` execution now supports ordered multi-column definitions with schema persistence
  - Index maintenance now writes/removes tuple index entries for INSERT/UPDATE/DELETE
  - UNIQUE enforcement now validates multi-column tuples (`NULL` in any indexed column bypasses uniqueness checks, matching SQLite behavior)
  - Planner access-path selection now supports full-tuple equality predicates for multi-column indexes (prefix/range planning remains follow-up)
  - Added integration coverage plus reopen validation; see `notes/multi-column-secondary-index-execution.md`
- [x] JOIN `GROUP BY` / `HAVING` aggregate execution (agent codex)
  - Extended join SELECT execution to support grouped and aggregate query paths instead of non-aggregate-only projection
  - Added join-aware grouped and aggregate expression evaluation for HAVING and ORDER BY with table-qualified column resolution
  - Added integration coverage for join `GROUP BY` + `HAVING`, aggregate join queries without `GROUP BY`, and bare-column aggregate error behavior; see `notes/join-group-by-having-execution.md`
- [x] Planner/execution support for multi-column index equality predicates (agent codex)
  - Extended planner `IndexInfo` metadata to include ordered index-column vectors
  - Planner now emits composite `AccessPath::IndexEq` plans when all index columns are matched by constant equality predicates under `AND`
  - `ralph-sqlite` candidate-row reads now evaluate/effectively probe multi-expression equality keys for SELECT/UPDATE/DELETE
  - Added planner + integration coverage; see `notes/multi-column-index-planner-selection.md`
- [x] Planner/execution support for multi-index AND-intersection predicates (agent codex)
  - Added `AccessPath::IndexAnd` planning for indexable conjunctions in `crates/planner` with composite-equality preference preserved
  - Added `ralph-sqlite` candidate-row intersection logic that intersects rowids from each planned index branch before table lookup
  - Added planner + integration coverage for SELECT/UPDATE/DELETE; see `notes/multi-index-and-intersection-selection.md`
- [x] Planner cost heuristics for table scan vs index path selection (agent 3)
  - Added static access-path cost estimation in `crates/planner` for `IndexEq`, `IndexRange`, `IndexOr`, and `IndexAnd`
  - Planner now rejects index paths whose estimated cost is not better than a table scan baseline
  - Added planner coverage for small-vs-large `IN (...)` fanout and high-cost index intersection fallback; see `notes/planner-cost-heuristics-selection.md`
- [x] Planner/execution support for multi-column index left-prefix/range predicates (agent 4)
  - Added planner `AccessPath::IndexPrefixRange` for multi-column indexes when left-prefix equality predicates are present, with optional range predicates on the next index column
  - Added `ralph-sqlite` execution support to scan/decode multi-column tuple buckets and apply prefix/range filtering for candidate rowid collection in SELECT/UPDATE/DELETE
  - Added planner + integration coverage; see `notes/multi-column-prefix-range-planner-execution.md`
- [x] Planner statistics-driven cost model for table scan vs index path selection (agent 3)
  - Added `PlannerStats`/`IndexStats` and stats-aware APIs (`plan_where_with_stats`, `plan_select_with_stats`) in `crates/planner`
  - Kept legacy `plan_where`/`plan_select` behavior unchanged for call sites that do not provide stats
  - Wired `ralph-sqlite` to collect runtime table/index cardinality estimates and pass them to planner for SELECT/UPDATE/DELETE
  - Added planner coverage for stats-driven high-cardinality and low-cardinality path choices; see `notes/planner-statistics-cost-model.md`
- [x] Persisted planner statistics metadata in schema + integration (agent 4)
  - Added schema `ObjectType::Stats` metadata entries plus APIs to upsert/list/drop table and index planner stats
  - Wired `Database::open` to load persisted table/index stats, and planner paths to consume persisted stats maps instead of per-query full scans
  - Added write-path stats refresh + persistence for CREATE/INSERT/UPDATE/DELETE and stats cleanup on DROP TABLE/DROP INDEX
  - Added storage + integration coverage; see `notes/persisted-planner-statistics.md`
- [x] Join index probe optimization for INNER/LEFT joins (agent opus)
  - Join execution now uses index-probed nested-loop when the ON condition is a simple equality (`col = expr`) on a single-column indexed column of the right table
  - INNER JOIN and LEFT JOIN paths probe the right-table index per left row instead of scanning all right rows
  - RIGHT JOIN and FULL OUTER JOIN fall back to full-scan nested-loop (unmatched-right tracking requires all right rows)
  - Non-equality and compound ON conditions fall back to full-scan nested-loop
  - Added 9 integration tests; see `notes/join-index-probe-optimization.md`

- [x] LIKE operator correctness fix (agent opus)
  - Replaced naive `String::contains` with DP-based `sql_like_match` supporting `%` (zero-or-more) and `_` (single-char) wildcards
  - Added case-insensitive ASCII matching per SQLite default behavior
  - Added NULL operand propagation (LIKE with NULL returns NULL)
  - Added 7 executor unit tests and 3 integration tests; see `notes/like-operator-fix.md`
- [x] Core scalar SQL function execution (agent codex)
  - Added shared scalar evaluator in `crates/executor` for `LENGTH`, `UPPER`, `LOWER`, `TYPEOF`, `ABS`, `COALESCE`, `IFNULL`, `NULLIF`, `SUBSTR`, `INSTR`, `REPLACE`, `TRIM`, `LTRIM`, and `RTRIM`
  - Wired `Expr::FunctionCall` evaluation through regular/join/grouped/aggregate paths in `crates/ralph-sqlite`
  - Added scalar-wrapper-over-aggregate support (for example `COALESCE(MAX(v), 0)`)
  - Added executor + integration tests; see `notes/scalar-sql-functions.md`

## Known Issues

- Dirty-page eviction now preserves rollback correctness by spilling uncommitted page bytes in memory; long-running write transactions can still increase memory usage if many dirty pages are evicted before commit.
- UPDATE/DELETE use index-driven row selection when a suitable equality or simple range index predicate exists; they fall back to full table scan otherwise.
- Query planning currently supports single-table equality/`IN`/range predicates on single-column secondary indexes, OR unions and AND intersections across indexable branches, full-tuple equality plus left-prefix/range predicates on multi-column secondary indexes, and stats-aware table/index cardinality cost selection with persisted row/distinct metadata; histogram-style stats and tighter cost estimates for prefix/range fanout are not implemented.
- Range index planning now uses ordered key-range scans for numeric and text bounds; text now uses a 7-byte exact + overlap-channel key encoding with limited suffix discrimination, so collision-heavy scans can still occur for some long shared prefixes.
- JOIN support includes INNER JOIN, CROSS JOIN, LEFT JOIN, RIGHT JOIN, and FULL OUTER JOIN. INNER/LEFT joins now use index-probed nested-loop when the ON condition is a simple equality on a single-column indexed right-table column; RIGHT/FULL outer joins and compound/non-equality ON conditions still use full-scan nested-loop.
- Core scalar SQL functions are implemented, but scalar multi-argument `MIN`/`MAX` and SQLite `HEX`/`QUOTE` behaviors are not yet implemented.
- No subquery support
- Column references outside aggregate functions are still rejected for aggregate queries without `GROUP BY`.
