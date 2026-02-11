//! Storage engine: pager, B+tree, and on-disk format.
//!
//! This crate provides:
//! - File header management (magic, page size, page count, etc.)
//! - Page cache / buffer pool with LRU eviction (pager)
//! - Page allocation (extend file; freelist reuse planned)
//!
//! Future additions:
//! - B+tree implementation for tables and indexes
//! - WAL and transaction support

pub mod btree;
pub mod header;
pub mod pager;

pub use btree::BTree;
pub use header::FileHeader;
pub use pager::Pager;
