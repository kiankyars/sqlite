/// SQL parser and AST definitions.
///
/// This crate currently implements:
/// - SQL tokenization (lexing)
/// - AST node types
/// - A parser for CREATE TABLE, INSERT, and SELECT

pub mod ast;
mod parser;

pub use parser::{parse, ParseError};

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Keyword {
    Select,
    Insert,
    Into,
    Values,
    Create,
    Table,
    Drop,
    Update,
    Delete,
    From,
    Where,
    Set,
    And,
    Or,
    Not,
    Null,
    Primary,
    Key,
    Index,
    On,
    Begin,
    Commit,
    Rollback,
    As,
    Order,
    By,
    Limit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenKind {
    Keyword(Keyword),
    Identifier(String),
    Integer(String),
    Real(String),
    StringLiteral(String),
    Comma,
    Semicolon,
    Dot,
    LeftParen,
    RightParen,
    Star,
    Plus,
    Minus,
    Slash,
    Percent,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    Question,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexError {
    pub position: usize,
    pub message: String,
}

impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lex error at byte {}: {}", self.position, self.message)
    }
}

impl std::error::Error for LexError {}

pub fn tokenize(input: &str) -> Result<Vec<Token>, LexError> {
    Lexer::new(input).tokenize()
}

struct Lexer<'a> {
    input: &'a str,
    chars: std::iter::Peekable<std::str::CharIndices<'a>>,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            chars: input.char_indices().peekable(),
        }
    }

    fn tokenize(mut self) -> Result<Vec<Token>, LexError> {
        let mut tokens = Vec::new();

        while let Some((start, ch)) = self.peek_char() {
            if ch.is_ascii_whitespace() {
                self.next_char();
                continue;
            }

            if ch == '-' && self.peek_nth_char(1).map(|(_, c)| c) == Some('-') {
                self.consume_line_comment();
                continue;
            }

            if ch == '/' && self.peek_nth_char(1).map(|(_, c)| c) == Some('*') {
                self.consume_block_comment(start)?;
                continue;
            }

            if is_identifier_start(ch) {
                tokens.push(self.consume_identifier_or_keyword()?);
                continue;
            }

            if ch.is_ascii_digit() {
                tokens.push(self.consume_number()?);
                continue;
            }

            match ch {
                '\'' => tokens.push(self.consume_string_literal()?),
                '"' => tokens.push(self.consume_quoted_identifier()?),
                ',' => tokens.push(self.single_char_token(TokenKind::Comma)?),
                ';' => tokens.push(self.single_char_token(TokenKind::Semicolon)?),
                '.' => tokens.push(self.single_char_token(TokenKind::Dot)?),
                '(' => tokens.push(self.single_char_token(TokenKind::LeftParen)?),
                ')' => tokens.push(self.single_char_token(TokenKind::RightParen)?),
                '*' => tokens.push(self.single_char_token(TokenKind::Star)?),
                '+' => tokens.push(self.single_char_token(TokenKind::Plus)?),
                '-' => tokens.push(self.single_char_token(TokenKind::Minus)?),
                '/' => tokens.push(self.single_char_token(TokenKind::Slash)?),
                '%' => tokens.push(self.single_char_token(TokenKind::Percent)?),
                '?' => tokens.push(self.single_char_token(TokenKind::Question)?),
                '=' => tokens.push(self.single_char_token(TokenKind::Eq)?),
                '!' => tokens.push(self.consume_bang_operator(start)?),
                '<' => tokens.push(self.consume_lt_operator()?),
                '>' => tokens.push(self.consume_gt_operator()?),
                _ => {
                    return Err(LexError {
                        position: start,
                        message: format!("unexpected character '{}'", ch),
                    });
                }
            }
        }

        Ok(tokens)
    }

    fn peek_char(&mut self) -> Option<(usize, char)> {
        self.chars.peek().copied()
    }

    fn peek_nth_char(&self, n: usize) -> Option<(usize, char)> {
        let mut clone = self.chars.clone();
        clone.nth(n)
    }

    fn next_char(&mut self) -> Option<(usize, char)> {
        self.chars.next()
    }

    fn single_char_token(&mut self, kind: TokenKind) -> Result<Token, LexError> {
        let (start, ch) = self.next_char().ok_or(LexError {
            position: self.input.len(),
            message: "unexpected end of input".to_string(),
        })?;
        Ok(Token {
            kind,
            start,
            end: start + ch.len_utf8(),
        })
    }

    fn consume_identifier_or_keyword(&mut self) -> Result<Token, LexError> {
        let (start, first) = self.next_char().ok_or(LexError {
            position: self.input.len(),
            message: "unexpected end of input".to_string(),
        })?;
        let mut end = start + first.len_utf8();

        while let Some((idx, ch)) = self.peek_char() {
            if is_identifier_continue(ch) {
                self.next_char();
                end = idx + ch.len_utf8();
            } else {
                break;
            }
        }

        let text = &self.input[start..end];
        let kind = match keyword_from_ident(text) {
            Some(keyword) => TokenKind::Keyword(keyword),
            None => TokenKind::Identifier(text.to_string()),
        };

        Ok(Token { kind, start, end })
    }

    fn consume_quoted_identifier(&mut self) -> Result<Token, LexError> {
        let (start, _) = self.next_char().ok_or(LexError {
            position: self.input.len(),
            message: "unexpected end of input".to_string(),
        })?;
        let mut content = String::new();

        while let Some((idx, ch)) = self.next_char() {
            if ch == '"' {
                if self.peek_char().map(|(_, c)| c) == Some('"') {
                    self.next_char();
                    content.push('"');
                    continue;
                }
                return Ok(Token {
                    kind: TokenKind::Identifier(content),
                    start,
                    end: idx + ch.len_utf8(),
                });
            }
            content.push(ch);
        }

        Err(LexError {
            position: start,
            message: "unterminated quoted identifier".to_string(),
        })
    }

    fn consume_number(&mut self) -> Result<Token, LexError> {
        let (start, first) = self.next_char().ok_or(LexError {
            position: self.input.len(),
            message: "unexpected end of input".to_string(),
        })?;
        let mut end = start + first.len_utf8();
        let mut is_real = false;

        while let Some((idx, ch)) = self.peek_char() {
            if ch.is_ascii_digit() {
                self.next_char();
                end = idx + ch.len_utf8();
            } else {
                break;
            }
        }

        if self.peek_char().map(|(_, c)| c) == Some('.')
            && self
                .peek_nth_char(1)
                .map(|(_, c)| c.is_ascii_digit())
                .unwrap_or(false)
        {
            let (dot_idx, dot_ch) = self.next_char().ok_or(LexError {
                position: self.input.len(),
                message: "unexpected end of input".to_string(),
            })?;
            end = dot_idx + dot_ch.len_utf8();
            is_real = true;
            while let Some((idx, ch)) = self.peek_char() {
                if ch.is_ascii_digit() {
                    self.next_char();
                    end = idx + ch.len_utf8();
                } else {
                    break;
                }
            }
        }

        if matches!(self.peek_char().map(|(_, c)| c), Some('e' | 'E')) && self.has_valid_exponent() {
            let (exp_idx, exp_ch) = self.next_char().ok_or(LexError {
                position: self.input.len(),
                message: "unexpected end of input".to_string(),
            })?;
            end = exp_idx + exp_ch.len_utf8();
            is_real = true;

            if matches!(self.peek_char().map(|(_, c)| c), Some('+' | '-')) {
                let (sign_idx, sign_ch) = self.next_char().ok_or(LexError {
                    position: self.input.len(),
                    message: "unexpected end of input".to_string(),
                })?;
                end = sign_idx + sign_ch.len_utf8();
            }

            while let Some((idx, ch)) = self.peek_char() {
                if ch.is_ascii_digit() {
                    self.next_char();
                    end = idx + ch.len_utf8();
                } else {
                    break;
                }
            }
        }

        let text = self.input[start..end].to_string();
        let kind = if is_real {
            TokenKind::Real(text)
        } else {
            TokenKind::Integer(text)
        };
        Ok(Token { kind, start, end })
    }

    fn has_valid_exponent(&self) -> bool {
        match self.peek_nth_char(1).map(|(_, c)| c) {
            Some(ch) if ch.is_ascii_digit() => true,
            Some('+') | Some('-') => self
                .peek_nth_char(2)
                .map(|(_, c)| c.is_ascii_digit())
                .unwrap_or(false),
            _ => false,
        }
    }

    fn consume_string_literal(&mut self) -> Result<Token, LexError> {
        let (start, _) = self.next_char().ok_or(LexError {
            position: self.input.len(),
            message: "unexpected end of input".to_string(),
        })?;
        let mut value = String::new();

        while let Some((idx, ch)) = self.next_char() {
            if ch == '\'' {
                if self.peek_char().map(|(_, c)| c) == Some('\'') {
                    self.next_char();
                    value.push('\'');
                    continue;
                }
                return Ok(Token {
                    kind: TokenKind::StringLiteral(value),
                    start,
                    end: idx + ch.len_utf8(),
                });
            }
            value.push(ch);
        }

        Err(LexError {
            position: start,
            message: "unterminated string literal".to_string(),
        })
    }

    fn consume_bang_operator(&mut self, start: usize) -> Result<Token, LexError> {
        let _ = self.next_char().ok_or(LexError {
            position: self.input.len(),
            message: "unexpected end of input".to_string(),
        })?;

        if self.peek_char().map(|(_, c)| c) == Some('=') {
            let (end_idx, end_ch) = self.next_char().ok_or(LexError {
                position: self.input.len(),
                message: "unexpected end of input".to_string(),
            })?;
            return Ok(Token {
                kind: TokenKind::NotEq,
                start,
                end: end_idx + end_ch.len_utf8(),
            });
        }

        Err(LexError {
            position: start,
            message: "unexpected character '!' (did you mean '!=')".to_string(),
        })
    }

    fn consume_lt_operator(&mut self) -> Result<Token, LexError> {
        let (start, first) = self.next_char().ok_or(LexError {
            position: self.input.len(),
            message: "unexpected end of input".to_string(),
        })?;
        let mut kind = TokenKind::Lt;
        let mut end = start + first.len_utf8();

        if let Some((idx, ch)) = self.peek_char() {
            if ch == '=' {
                self.next_char();
                kind = TokenKind::Lte;
                end = idx + ch.len_utf8();
            } else if ch == '>' {
                self.next_char();
                kind = TokenKind::NotEq;
                end = idx + ch.len_utf8();
            }
        }

        Ok(Token { kind, start, end })
    }

    fn consume_gt_operator(&mut self) -> Result<Token, LexError> {
        let (start, first) = self.next_char().ok_or(LexError {
            position: self.input.len(),
            message: "unexpected end of input".to_string(),
        })?;
        let mut kind = TokenKind::Gt;
        let mut end = start + first.len_utf8();

        if let Some((idx, ch)) = self.peek_char() {
            if ch == '=' {
                self.next_char();
                kind = TokenKind::Gte;
                end = idx + ch.len_utf8();
            }
        }

        Ok(Token { kind, start, end })
    }

    fn consume_line_comment(&mut self) {
        let _ = self.next_char();
        let _ = self.next_char();
        while let Some((_, ch)) = self.next_char() {
            if ch == '\n' {
                break;
            }
        }
    }

    fn consume_block_comment(&mut self, start: usize) -> Result<(), LexError> {
        let _ = self.next_char();
        let _ = self.next_char();
        let mut saw_star = false;

        while let Some((_, ch)) = self.next_char() {
            if saw_star && ch == '/' {
                return Ok(());
            }
            saw_star = ch == '*';
        }

        Err(LexError {
            position: start,
            message: "unterminated block comment".to_string(),
        })
    }
}

fn is_identifier_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_identifier_continue(ch: char) -> bool {
    is_identifier_start(ch) || ch.is_ascii_digit()
}

fn keyword_from_ident(ident: &str) -> Option<Keyword> {
    match ident.to_ascii_uppercase().as_str() {
        "SELECT" => Some(Keyword::Select),
        "INSERT" => Some(Keyword::Insert),
        "INTO" => Some(Keyword::Into),
        "VALUES" => Some(Keyword::Values),
        "CREATE" => Some(Keyword::Create),
        "TABLE" => Some(Keyword::Table),
        "DROP" => Some(Keyword::Drop),
        "UPDATE" => Some(Keyword::Update),
        "DELETE" => Some(Keyword::Delete),
        "FROM" => Some(Keyword::From),
        "WHERE" => Some(Keyword::Where),
        "SET" => Some(Keyword::Set),
        "AND" => Some(Keyword::And),
        "OR" => Some(Keyword::Or),
        "NOT" => Some(Keyword::Not),
        "NULL" => Some(Keyword::Null),
        "PRIMARY" => Some(Keyword::Primary),
        "KEY" => Some(Keyword::Key),
        "INDEX" => Some(Keyword::Index),
        "ON" => Some(Keyword::On),
        "BEGIN" => Some(Keyword::Begin),
        "COMMIT" => Some(Keyword::Commit),
        "ROLLBACK" => Some(Keyword::Rollback),
        "AS" => Some(Keyword::As),
        "ORDER" => Some(Keyword::Order),
        "BY" => Some(Keyword::By),
        "LIMIT" => Some(Keyword::Limit),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{
        BinaryOperator, ColumnDef, CreateTableStatement, Expr, InsertStatement, SelectItem,
        SelectStatement, Statement,
    };

    fn kinds(sql: &str) -> Vec<TokenKind> {
        tokenize(sql)
            .expect("tokenization should succeed")
            .into_iter()
            .map(|t| t.kind)
            .collect()
    }

    #[test]
    fn tokenizes_simple_select() {
        let got = kinds("SELECT id, name FROM users;");
        let want = vec![
            TokenKind::Keyword(Keyword::Select),
            TokenKind::Identifier("id".to_string()),
            TokenKind::Comma,
            TokenKind::Identifier("name".to_string()),
            TokenKind::Keyword(Keyword::From),
            TokenKind::Identifier("users".to_string()),
            TokenKind::Semicolon,
        ];
        assert_eq!(got, want);
    }

    #[test]
    fn treats_keywords_case_insensitively() {
        let got = kinds("sElEcT col FrOm t");
        let want = vec![
            TokenKind::Keyword(Keyword::Select),
            TokenKind::Identifier("col".to_string()),
            TokenKind::Keyword(Keyword::From),
            TokenKind::Identifier("t".to_string()),
        ];
        assert_eq!(got, want);
    }

    #[test]
    fn tokenizes_escaped_string_literal() {
        let got = kinds("INSERT INTO t VALUES('it''s ok')");
        assert_eq!(
            got,
            vec![
                TokenKind::Keyword(Keyword::Insert),
                TokenKind::Keyword(Keyword::Into),
                TokenKind::Identifier("t".to_string()),
                TokenKind::Keyword(Keyword::Values),
                TokenKind::LeftParen,
                TokenKind::StringLiteral("it's ok".to_string()),
                TokenKind::RightParen,
            ]
        );
    }

    #[test]
    fn tokenizes_quoted_identifiers() {
        let got = kinds("SELECT \"first\"\"name\" FROM \"people\"");
        assert_eq!(
            got,
            vec![
                TokenKind::Keyword(Keyword::Select),
                TokenKind::Identifier("first\"name".to_string()),
                TokenKind::Keyword(Keyword::From),
                TokenKind::Identifier("people".to_string()),
            ]
        );
    }

    #[test]
    fn tokenizes_numbers_and_operators() {
        let got = kinds("a=1 AND b<=2.5e+3 OR c<>7 AND d!=8");
        assert_eq!(
            got,
            vec![
                TokenKind::Identifier("a".to_string()),
                TokenKind::Eq,
                TokenKind::Integer("1".to_string()),
                TokenKind::Keyword(Keyword::And),
                TokenKind::Identifier("b".to_string()),
                TokenKind::Lte,
                TokenKind::Real("2.5e+3".to_string()),
                TokenKind::Keyword(Keyword::Or),
                TokenKind::Identifier("c".to_string()),
                TokenKind::NotEq,
                TokenKind::Integer("7".to_string()),
                TokenKind::Keyword(Keyword::And),
                TokenKind::Identifier("d".to_string()),
                TokenKind::NotEq,
                TokenKind::Integer("8".to_string()),
            ]
        );
    }

    #[test]
    fn skips_line_and_block_comments() {
        let got = kinds(
            "SELECT -- comment\nid /* comment block */ FROM users",
        );
        assert_eq!(
            got,
            vec![
                TokenKind::Keyword(Keyword::Select),
                TokenKind::Identifier("id".to_string()),
                TokenKind::Keyword(Keyword::From),
                TokenKind::Identifier("users".to_string()),
            ]
        );
    }

    #[test]
    fn returns_error_for_unterminated_string() {
        let err = tokenize("SELECT 'oops").expect_err("tokenization should fail");
        assert_eq!(err.position, 7);
        assert!(err.message.contains("unterminated string literal"));
    }

    #[test]
    fn returns_error_for_unterminated_block_comment() {
        let err = tokenize("SELECT /*").expect_err("tokenization should fail");
        assert_eq!(err.position, 7);
        assert!(err.message.contains("unterminated block comment"));
    }

    #[test]
    fn parse_create_table() {
        let statement = parse("CREATE TABLE users (id INTEGER, name TEXT);").unwrap();
        assert_eq!(
            statement,
            Statement::CreateTable(CreateTableStatement {
                table_name: "users".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: "INTEGER".to_string(),
                    },
                    ColumnDef {
                        name: "name".to_string(),
                        data_type: "TEXT".to_string(),
                    },
                ],
            })
        );
    }

    #[test]
    fn parse_insert_with_explicit_columns() {
        let statement = parse("INSERT INTO users (id, name) VALUES (1, 'Alice');").unwrap();
        assert_eq!(
            statement,
            Statement::Insert(InsertStatement {
                table_name: "users".to_string(),
                columns: vec!["id".to_string(), "name".to_string()],
                values: vec![Expr::Integer(1), Expr::String("Alice".to_string())],
            })
        );
    }

    #[test]
    fn parse_select_with_arithmetic_and_from() {
        let statement = parse("SELECT 1 + 2, name FROM users;").unwrap();
        assert_eq!(
            statement,
            Statement::Select(SelectStatement {
                projection: vec![
                    SelectItem::Expr(Expr::Binary {
                        left: Box::new(Expr::Integer(1)),
                        op: BinaryOperator::Add,
                        right: Box::new(Expr::Integer(2)),
                    }),
                    SelectItem::Expr(Expr::Identifier("name".to_string())),
                ],
                from: Some("users".to_string()),
            })
        );
    }

    #[test]
    fn parse_select_star() {
        let statement = parse("SELECT * FROM users").unwrap();
        assert_eq!(
            statement,
            Statement::Select(SelectStatement {
                projection: vec![SelectItem::Wildcard],
                from: Some("users".to_string()),
            })
        );
    }

    #[test]
    fn parse_rejects_unsupported_statement() {
        let err = parse("DROP TABLE users;").unwrap_err();
        assert!(err.message().contains("unexpected token"));
    }
}
