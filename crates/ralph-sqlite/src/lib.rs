/// Top-level integration crate for ralph-sqlite.
///
/// Wires together parser, planner, executor, and storage into a
/// unified database interface.

pub fn version() -> &'static str {
    "0.1.0-bootstrap"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_string() {
        assert_eq!(version(), "0.1.0-bootstrap");
    }
}
