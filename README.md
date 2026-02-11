# ralph-sqlite

A SQLite-compatible database engine built from scratch in Rust.

## Purpose

ralph-sqlite is an educational and experimental implementation of a relational database engine modeled after SQLite. It implements SQL parsing, query planning, execution, and a page-based storage engine with B+tree indexes, transactions, and write-ahead logging.

## Architecture Overview

The project is organized as a Cargo workspace with five crates:

```
crates/
├── parser/        SQL tokenizer, parser, and AST definitions
├── planner/       Logical and physical query plan generation
├── executor/      Volcano-style query execution engine
├── storage/       Pager, B+tree, on-disk format, WAL, transactions
└── ralph-sqlite/  Top-level integration crate
```

Data flows through the system as:

```
SQL text → parser → AST → planner → plan → executor ↔ storage → disk
```

## Build / Run / Test

Requires Rust 1.70+ and `sqlite3` (for test oracle).

```bash
# Build
cargo build

# Run all unit tests
cargo test

# Run the test harness (full mode)
./test.sh

# Run the test harness (fast mode — deterministic 10% sample)
./test.sh --fast
```

### Fast mode

Fast mode selects a deterministic 10% subset of tests using md5sum-based hashing with a seed derived from the `AGENT_ID` environment variable. The same seed always produces the same test subset.

## Scope

### In scope
- SQL parsing (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE)
- Single-file database storage (SQLite-compatible page format)
- B+tree tables (rowid) and secondary indexes
- Basic query planning and optimization
- ACID transactions with WAL-based recovery
- Automated testing with sqlite3 as behavioral oracle

### Non-goals
- Network protocol / client-server architecture
- Full SQL standard compliance (CTEs, window functions, etc. are stretch goals)
- Concurrent multi-writer support
- Replication
- Production use

## Lock-File Protocol

See [DESIGN.md](DESIGN.md#lock-file-protocol) for the task lock-file protocol used by parallel agents.
