/// Query executor: evaluates physical plans against storage.
///
/// This crate will contain:
/// - Volcano-style iterator model
/// - Expression evaluation
/// - Result materialization

pub fn execute() -> Result<(), String> {
    Err("executor not yet implemented".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_returns_error() {
        assert!(execute().is_err());
    }
}
