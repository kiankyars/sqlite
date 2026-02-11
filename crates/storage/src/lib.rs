/// Storage engine: pager, B+tree, and on-disk format.
///
/// This crate will contain:
/// - Page cache / buffer pool (pager)
/// - B+tree implementation for tables and indexes
/// - On-disk file format (header, pages, freelists)
/// - WAL and transaction support

pub fn open(_path: &str) -> Result<(), String> {
    Err("storage not yet implemented".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_returns_error() {
        assert!(open("test.db").is_err());
    }
}
