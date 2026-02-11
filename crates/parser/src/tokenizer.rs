/// SQL tokenizer (lexer) — converts SQL text into a stream of tokens.

use crate::token::{Keyword, Token};

pub struct Tokenizer<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
        }
    }

    pub fn tokenize(mut self) -> Result<Vec<Token>, String> {
        let mut tokens = Vec::new();
        loop {
            let tok = self.next_token()?;
            if tok == Token::Eof {
                tokens.push(tok);
                break;
            }
            tokens.push(tok);
        }
        Ok(tokens)
    }

    fn peek_byte(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<u8> {
        let b = self.input.get(self.pos).copied()?;
        self.pos += 1;
        Some(b)
    }

    fn skip_whitespace(&mut self) {
        while let Some(b) = self.peek_byte() {
            if b.is_ascii_whitespace() {
                self.pos += 1;
            } else if b == b'-' && self.input.get(self.pos + 1) == Some(&b'-') {
                // Line comment: skip to end of line
                self.pos += 2;
                while let Some(b) = self.peek_byte() {
                    self.pos += 1;
                    if b == b'\n' {
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }

    fn next_token(&mut self) -> Result<Token, String> {
        self.skip_whitespace();

        let b = match self.peek_byte() {
            Some(b) => b,
            None => return Ok(Token::Eof),
        };

        match b {
            b'+' => {
                self.advance();
                Ok(Token::Plus)
            }
            b'-' => {
                self.advance();
                Ok(Token::Minus)
            }
            b'*' => {
                self.advance();
                Ok(Token::Star)
            }
            b'/' => {
                self.advance();
                Ok(Token::Slash)
            }
            b'%' => {
                self.advance();
                Ok(Token::Percent)
            }
            b'(' => {
                self.advance();
                Ok(Token::LeftParen)
            }
            b')' => {
                self.advance();
                Ok(Token::RightParen)
            }
            b',' => {
                self.advance();
                Ok(Token::Comma)
            }
            b';' => {
                self.advance();
                Ok(Token::Semicolon)
            }
            b'.' => {
                self.advance();
                Ok(Token::Dot)
            }
            b'=' => {
                self.advance();
                Ok(Token::Eq)
            }
            b'|' => {
                self.advance();
                if self.peek_byte() == Some(b'|') {
                    self.advance();
                    Ok(Token::Pipe)
                } else {
                    Err("expected '||' for concatenation".into())
                }
            }
            b'<' => {
                self.advance();
                if self.peek_byte() == Some(b'=') {
                    self.advance();
                    Ok(Token::LtEq)
                } else if self.peek_byte() == Some(b'>') {
                    self.advance();
                    Ok(Token::NotEq)
                } else {
                    Ok(Token::Lt)
                }
            }
            b'>' => {
                self.advance();
                if self.peek_byte() == Some(b'=') {
                    self.advance();
                    Ok(Token::GtEq)
                } else {
                    Ok(Token::Gt)
                }
            }
            b'!' => {
                self.advance();
                if self.peek_byte() == Some(b'=') {
                    self.advance();
                    Ok(Token::NotEq)
                } else {
                    Err("expected '!=' after '!'".into())
                }
            }
            b'\'' => self.read_string(),
            b'0'..=b'9' => self.read_number(),
            b if is_ident_start(b) => self.read_ident_or_keyword(),
            other => Err(format!("unexpected character: '{}'", other as char)),
        }
    }

    fn read_string(&mut self) -> Result<Token, String> {
        self.advance(); // consume opening quote
        let mut s = String::new();
        loop {
            match self.advance() {
                None => return Err("unterminated string literal".into()),
                Some(b'\'') => {
                    // Check for escaped single quote ('')
                    if self.peek_byte() == Some(b'\'') {
                        self.advance();
                        s.push('\'');
                    } else {
                        break;
                    }
                }
                Some(b) => s.push(b as char),
            }
        }
        Ok(Token::String(s))
    }

    fn read_number(&mut self) -> Result<Token, String> {
        let start = self.pos;
        while let Some(b) = self.peek_byte() {
            if b.is_ascii_digit() {
                self.advance();
            } else {
                break;
            }
        }

        // Check for decimal point
        if self.peek_byte() == Some(b'.') {
            self.advance();
            while let Some(b) = self.peek_byte() {
                if b.is_ascii_digit() {
                    self.advance();
                } else {
                    break;
                }
            }
            let text = std::str::from_utf8(&self.input[start..self.pos]).unwrap();
            let val: f64 = text
                .parse()
                .map_err(|e| format!("invalid float literal '{}': {}", text, e))?;
            Ok(Token::Float(val))
        } else {
            let text = std::str::from_utf8(&self.input[start..self.pos]).unwrap();
            let val: i64 = text
                .parse()
                .map_err(|e| format!("invalid integer literal '{}': {}", text, e))?;
            Ok(Token::Integer(val))
        }
    }

    fn read_ident_or_keyword(&mut self) -> Result<Token, String> {
        let start = self.pos;
        while let Some(b) = self.peek_byte() {
            if is_ident_part(b) {
                self.advance();
            } else {
                break;
            }
        }
        let text = std::str::from_utf8(&self.input[start..self.pos]).unwrap();
        if let Some(kw) = Keyword::from_str(text) {
            Ok(Token::Keyword(kw))
        } else {
            Ok(Token::Ident(text.to_string()))
        }
    }
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_part(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokenize(input: &str) -> Vec<Token> {
        Tokenizer::new(input).tokenize().unwrap()
    }

    #[test]
    fn test_select_literal() {
        let tokens = tokenize("SELECT 1;");
        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Integer(1),
                Token::Semicolon,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_select_arithmetic() {
        let tokens = tokenize("SELECT 1 + 2;");
        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Integer(1),
                Token::Plus,
                Token::Integer(2),
                Token::Semicolon,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_string_literal() {
        let tokens = tokenize("SELECT 'hello';");
        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Select),
                Token::String("hello".into()),
                Token::Semicolon,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_escaped_string() {
        let tokens = tokenize("SELECT 'it''s';");
        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Select),
                Token::String("it's".into()),
                Token::Semicolon,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_create_table() {
        let tokens = tokenize("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);");
        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Ident("users".into()),
                Token::LeftParen,
                Token::Ident("id".into()),
                Token::Keyword(Keyword::Integer),
                Token::Keyword(Keyword::Primary),
                Token::Keyword(Keyword::Key),
                Token::Comma,
                Token::Ident("name".into()),
                Token::Keyword(Keyword::Text),
                Token::RightParen,
                Token::Semicolon,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_comparison_operators() {
        let tokens = tokenize("a < b <= c > d >= e = f != g <> h");
        assert_eq!(
            tokens,
            vec![
                Token::Ident("a".into()),
                Token::Lt,
                Token::Ident("b".into()),
                Token::LtEq,
                Token::Ident("c".into()),
                Token::Gt,
                Token::Ident("d".into()),
                Token::GtEq,
                Token::Ident("e".into()),
                Token::Eq,
                Token::Ident("f".into()),
                Token::NotEq,
                Token::Ident("g".into()),
                Token::NotEq,
                Token::Ident("h".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_float_literal() {
        let tokens = tokenize("SELECT 3.14;");
        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Float(3.14),
                Token::Semicolon,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_line_comment() {
        let tokens = tokenize("SELECT 1 -- this is a comment\n + 2;");
        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Integer(1),
                Token::Plus,
                Token::Integer(2),
                Token::Semicolon,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_insert() {
        let tokens = tokenize("INSERT INTO t VALUES (1, 'a');");
        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("t".into()),
                Token::Keyword(Keyword::Values),
                Token::LeftParen,
                Token::Integer(1),
                Token::Comma,
                Token::String("a".into()),
                Token::RightParen,
                Token::Semicolon,
                Token::Eof,
            ]
        );
    }
}
