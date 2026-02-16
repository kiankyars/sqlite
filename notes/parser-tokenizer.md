## Parser Tokenizer Handoff (2026-02-11)

Implemented in `crates/parser/src/lib.rs`:

- Public API:
  - `tokenize(input: &str) -> Result<Vec<Token>, LexError>`
  - `Token { kind, start, end }` where spans are byte offsets
  - `TokenKind` covers keywords, identifiers, numeric/string literals, punctuation, and operators
  - `LexError { position, message }`
- Behavior:
  - Keywords are case-insensitive
  - Supports quoted identifiers with escaped `""`
  - Supports string literals with escaped `''`
  - Supports integer and real literals including exponent notation
  - Supports SQL comments: `-- ...` and `/* ... */`
  - Returns position-aware errors for unterminated strings/comments and invalid characters

Unit tests added in the same file cover:
- Basic SELECT tokenization
- Keyword case-insensitivity
- Escaped strings
- Quoted identifiers
- Numeric/operator tokenization
- Comment skipping
- Unterminated string/block-comment errors

Known limitations (acceptable for current milestone):
- Nested block comments are not supported
- Numbers starting with `.` (e.g. `.5`) tokenize as `Dot` + `Integer`
- Parser still returns a stub error; next task should consume `Token` stream for CREATE TABLE / INSERT / SELECT
