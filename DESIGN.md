# Design Document

## Architecture and Module Boundaries

### Crate Dependency Graph

```
ralph-sqlite (integration)
├── ralph-parser    (no internal deps)
├── ralph-planner   (depends on: ralph-parser)
├── ralph-executor  (depends on: ralph-planner, ralph-storage)
└── ralph-storage   (no internal deps)
```

### Module Responsibilities

**ralph-parser** — SQL text in, AST out.
- Tokenizer: produces a stream of typed tokens from raw SQL.
- Parser: recursive-descent parser consumes tokens, produces AST nodes.
- AST types: `Select`, `Insert`, `Update`, `Delete`, `CreateTable`, `DropTable`, expressions, column refs, literals, operators.

**ralph-planner** — AST in, execution plan out.
- Logical plan: relational algebra tree (Scan, Filter, Project, Join, Sort, Limit).
- Physical plan: annotated with access methods (index scan vs. table scan), join strategies.
- Optimizer: rule-based initially; cost-based later.

**ralph-executor** — Plan in, result rows out.
- Volcano/iterator model: each plan node implements `open()`, `next()`, `close()`.
- Expression evaluator: evaluates WHERE clauses, computed columns.
- Result materialization: collects output rows for the caller.

**ralph-storage** — Manages all on-disk state.
- Pager: fixed-size page I/O, buffer pool with LRU eviction.
- B+tree: ordered key-value structure for tables (rowid keys) and indexes.
- File format: header page, interior/leaf pages, freelist.
- WAL: write-ahead log for atomic commits and crash recovery.
- Transactions: begin/commit/rollback with WAL integration.

## Data Model

- A database is a single file on disk.
- The file is divided into fixed-size pages (default 4096 bytes).
- Page 0 is the file header (magic number, page size, schema version, freelist pointer).
- Tables are B+trees keyed by 64-bit rowid.
- Indexes are B+trees keyed by indexed column value(s), with rowid as the value.
- The schema table (sqlite_master equivalent) is stored in page 1.

## On-Disk Layout

```
┌──────────────────────┐
│  Page 0: File Header │  magic, page_size, page_count, freelist_head, schema_root
├──────────────────────┤
│  Page 1: Schema Root │  B+tree root for the schema table
├──────────────────────┤
│  Page 2..N: Data     │  B+tree interior/leaf pages, overflow pages
├──────────────────────┤
│  Freelist pages      │  Linked list of reusable pages
└──────────────────────┘
```

### Page Format (Interior)

```
┌─────────────┬────────────┬─────────────────────────┐
│ page_type   │ cell_count │ right_child_ptr          │
├─────────────┴────────────┴─────────────────────────┤
│ cell_offset_array (cell_count × u16)               │
├────────────────────────────────────────────────────┤
│ free space                                          │
├────────────────────────────────────────────────────┤
│ cells (key + left_child_ptr), growing from end     │
└────────────────────────────────────────────────────┘
```

### Page Format (Leaf)

```
┌─────────────┬────────────┬─────────────────────────┐
│ page_type   │ cell_count │ next_leaf_ptr            │
├─────────────┴────────────┴─────────────────────────┤
│ cell_offset_array (cell_count × u16)               │
├────────────────────────────────────────────────────┤
│ free space                                          │
├────────────────────────────────────────────────────┤
│ cells (key + payload_size + payload), from end     │
└────────────────────────────────────────────────────┘
```

## Pager Strategy

- **Buffer pool**: Fixed number of page-sized frames in memory.
- **Eviction**: LRU with dirty-page tracking.
- **Read path**: Check buffer pool → read from disk → insert into pool.
- **Write path**: Mark page dirty in buffer pool → flush via WAL on commit.
- **Page allocation**: Reuse freelist pages first; extend file if freelist empty.

## B+tree Strategy

- Separate B+trees for each table (keyed by rowid) and each index (keyed by column values).
- Leaf nodes are linked for efficient range scans.
- Split/merge operations maintain balance.
- Target: ~2/3 fill factor after splits.
- Overflow pages for payloads exceeding a fraction of page size.

## Transaction Strategy

- **WAL (Write-Ahead Logging)**:
  - All modifications are first written to a WAL file before the main database file.
  - WAL records: `(page_number, before_image_or_after_image, txn_id, checksum)`.
  - Commit = fsync the WAL with a commit record.
  - Checkpoint = copy WAL pages back to the main database file, then truncate WAL.
- **Isolation**: Single-writer, multiple-reader via WAL.
- **Recovery**: On startup, replay committed WAL records; discard uncommitted.
- **Rollback**: Discard uncommitted WAL records for the aborting transaction.

## Staged Roadmap

### Stage 1: Foundation
- [ ] SQL tokenizer and parser for SELECT/INSERT/CREATE TABLE
- [ ] AST types
- [ ] Basic pager with read/write of 4KB pages
- [ ] File header and page allocation
- [ ] Test harness with sqlite3 oracle

### Stage 2: Storage
- [ ] B+tree insert and point lookup
- [ ] B+tree range scan with leaf linking
- [ ] Schema table (sqlite_master equivalent)
- [ ] Table creation on disk
- [ ] INSERT and simple SELECT execution end-to-end

### Stage 3: Query Execution
- [ ] Volcano iterator model
- [ ] Table scan operator
- [ ] Filter (WHERE) operator
- [ ] Projection operator
- [ ] Expression evaluation (comparisons, arithmetic, string ops)

### Stage 4: Full DML
- [ ] UPDATE execution
- [ ] DELETE execution
- [ ] Secondary indexes (CREATE INDEX, index scan)
- [ ] Query planner: choose index scan vs. table scan

### Stage 5: Transactions
- [ ] WAL file format and write path
- [ ] Commit and checkpoint
- [ ] Crash recovery (WAL replay)
- [ ] BEGIN/COMMIT/ROLLBACK SQL support

### Stage 6: Polish
- [ ] B+tree split and merge
- [ ] Freelist management
- [ ] Overflow pages
- [ ] DROP TABLE / DROP INDEX
- [ ] ORDER BY, LIMIT
- [ ] Aggregate functions (COUNT, SUM, AVG, MIN, MAX)

## Lock-File Protocol

Parallel agents coordinate via lock files in the `current_tasks/` directory.

### Format

Each lock file is named after the task being claimed (e.g., `current_tasks/parser-tokenizer.lock`) and contains:

```
created_at_unix=<unix_timestamp>
agent_id=<agent_identifier>
task=<short description>
```

### Rules

1. **Acquire**: Before starting a task, create a lock file atomically. Use `O_CREAT | O_EXCL` semantics (or equivalent) to avoid races.
2. **Release**: Delete the lock file when the task is complete (committed) or abandoned.
3. **Stale lock detection**: A lock is considered stale if `created_at_unix` is more than **1 hour** old. Any agent may delete a stale lock and re-acquire it.
4. **Best practices**:
   - Use narrow, specific lock names to minimize contention.
   - Check for stale locks before erroring on "already locked."
   - Include `agent_id` so operators can identify which agent holds a lock.
   - Prefer short-lived locks; break large tasks into smaller lockable units.
5. **No nested locks**: An agent should not hold more than one lock at a time to avoid deadlocks.
