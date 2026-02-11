/// Query planner: transforms parsed AST into a logical/physical query plan.
///
/// This crate will contain:
/// - Logical plan representation
/// - Physical plan representation
/// - Basic cost model / plan selection

pub fn plan() -> Result<(), String> {
    Err("planner not yet implemented".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_returns_error() {
        assert!(plan().is_err());
    }
}
