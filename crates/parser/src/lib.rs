/// SQL parser and AST definitions.
///
/// This crate will contain:
/// - Tokenizer/lexer for SQL input
/// - Recursive-descent parser producing an AST
/// - AST node types for SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, etc.

pub fn parse(_input: &str) -> Result<(), String> {
    Err("parser not yet implemented".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_returns_error() {
        assert!(parse("SELECT 1").is_err());
    }
}
