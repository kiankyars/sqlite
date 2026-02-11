//! Storage engine: pager, B+tree, schema, and on-disk format.
//!
//! This crate provides:
//! - File header management (magic, page size, page count, etc.)
//! - Page cache / buffer pool with LRU eviction (pager)
//! - Page allocation (freelist-pop reuse with file extension fallback)
//! - B+tree: insert, lookup, range scan, splitting
//! - Schema table: metadata for tables and indexes
//!
//! Future additions:
//! - WAL and transaction support

pub mod btree;
pub mod header;
pub mod pager;
pub mod schema;
mod wal;

pub use btree::BTree;
pub use header::FileHeader;
pub use pager::Pager;
pub use schema::Schema;
