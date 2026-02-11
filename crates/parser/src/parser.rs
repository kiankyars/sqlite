use crate::ast::{
    BinaryOperator, ColumnDef, CreateTableStatement, Expr, InsertStatement, SelectItem,
    SelectStatement, Statement,
};
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    position: usize,
    message: String,
}

impl ParseError {
    fn new(position: usize, message: impl Into<String>) -> Self {
        Self {
            position,
            message: message.into(),
        }
    }

    pub fn position(&self) -> usize {
        self.position
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "parse error at {}: {}", self.position, self.message)
    }
}

impl Error for ParseError {}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Create,
    Table,
    Insert,
    Into,
    Values,
    Select,
    From,
    Identifier(String),
    Integer(i64),
    String(String),
    Comma,
    LParen,
    RParen,
    Semicolon,
    Star,
    Plus,
    Eof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SpannedToken {
    token: Token,
    position: usize,
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

    fn next_token(&mut self) -> Result<SpannedToken, ParseError> {
        self.skip_whitespace();

        let (position, ch) = match self.chars.next() {
            Some(value) => value,
            None => {
                return Ok(SpannedToken {
                    token: Token::Eof,
                    position: self.input.len(),
                });
            }
        };

        let token = match ch {
            ',' => Token::Comma,
            '(' => Token::LParen,
            ')' => Token::RParen,
            ';' => Token::Semicolon,
            '*' => Token::Star,
            '+' => Token::Plus,
            '\'' => Token::String(self.read_string(position)?),
            c if c.is_ascii_alphabetic() || c == '_' => {
                self.read_identifier_or_keyword(position, c)
            }
            c if c.is_ascii_digit() => Token::Integer(self.read_integer(c, position)?),
            _ => {
                return Err(ParseError::new(
                    position,
                    format!("unexpected character '{}'", ch),
                ))
            }
        };

        Ok(SpannedToken { token, position })
    }

    fn skip_whitespace(&mut self) {
        while let Some((_, ch)) = self.chars.peek() {
            if ch.is_whitespace() {
                self.chars.next();
            } else {
                break;
            }
        }
    }

    fn read_identifier_or_keyword(&mut self, start: usize, first: char) -> Token {
        let mut ident = String::with_capacity(8);
        ident.push(first);

        while let Some((_, ch)) = self.chars.peek() {
            if ch.is_ascii_alphanumeric() || *ch == '_' {
                ident.push(*ch);
                self.chars.next();
            } else {
                break;
            }
        }

        let upper = ident.to_ascii_uppercase();
        match upper.as_str() {
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            _ => {
                let _ = start;
                Token::Identifier(ident)
            }
        }
    }

    fn read_integer(&mut self, first: char, position: usize) -> Result<i64, ParseError> {
        let mut num = String::from(first);
        while let Some((_, ch)) = self.chars.peek() {
            if ch.is_ascii_digit() {
                num.push(*ch);
                self.chars.next();
            } else {
                break;
            }
        }

        num.parse::<i64>()
            .map_err(|_| ParseError::new(position, "invalid integer literal"))
    }

    fn read_string(&mut self, position: usize) -> Result<String, ParseError> {
        let mut output = String::new();
        loop {
            let (_, ch) = self
                .chars
                .next()
                .ok_or_else(|| ParseError::new(position, "unterminated string literal"))?;

            if ch == '\'' {
                if let Some((_, '\'')) = self.chars.peek() {
                    output.push('\'');
                    self.chars.next();
                    continue;
                }
                return Ok(output);
            }

            output.push(ch);
        }
    }
}

pub fn parse(input: &str) -> Result<Statement, ParseError> {
    let mut parser = Parser::new(input)?;
    let statement = parser.parse_statement()?;
    parser.consume(Token::Semicolon);
    parser.expect(Token::Eof)?;
    Ok(statement)
}

struct Parser {
    tokens: Vec<SpannedToken>,
    position: usize,
}

impl Parser {
    fn new(input: &str) -> Result<Self, ParseError> {
        let mut lexer = Lexer::new(input);
        let mut tokens = Vec::new();

        loop {
            let token = lexer.next_token()?;
            let is_eof = token.token == Token::Eof;
            tokens.push(token);
            if is_eof {
                break;
            }
        }

        Ok(Self {
            tokens,
            position: 0,
        })
    }

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        match self.peek() {
            Token::Create => self.parse_create_table().map(Statement::CreateTable),
            Token::Insert => self.parse_insert().map(Statement::Insert),
            Token::Select => self.parse_select().map(Statement::Select),
            other => Err(self.error(format!("unexpected token: {:?}", other))),
        }
    }

    fn parse_create_table(&mut self) -> Result<CreateTableStatement, ParseError> {
        self.expect(Token::Create)?;
        self.expect(Token::Table)?;
        let table_name = self.parse_identifier()?;
        self.expect(Token::LParen)?;

        let mut columns = Vec::new();
        loop {
            let name = self.parse_identifier()?;
            let data_type = self.parse_identifier()?;
            columns.push(ColumnDef { name, data_type });

            if !self.consume(Token::Comma) {
                break;
            }
        }

        if columns.is_empty() {
            return Err(self.error("CREATE TABLE requires at least one column"));
        }

        self.expect(Token::RParen)?;

        Ok(CreateTableStatement {
            table_name,
            columns,
        })
    }

    fn parse_insert(&mut self) -> Result<InsertStatement, ParseError> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;
        let table_name = self.parse_identifier()?;

        let columns = if self.consume(Token::LParen) {
            let identifiers = self.parse_identifier_list()?;
            self.expect(Token::RParen)?;
            identifiers
        } else {
            Vec::new()
        };

        self.expect(Token::Values)?;
        self.expect(Token::LParen)?;
        let values = self.parse_expr_list()?;
        self.expect(Token::RParen)?;

        if values.is_empty() {
            return Err(self.error("INSERT VALUES requires at least one value"));
        }

        Ok(InsertStatement {
            table_name,
            columns,
            values,
        })
    }

    fn parse_select(&mut self) -> Result<SelectStatement, ParseError> {
        self.expect(Token::Select)?;

        let projection = if self.consume(Token::Star) {
            vec![SelectItem::Wildcard]
        } else {
            let mut items = Vec::new();
            loop {
                items.push(SelectItem::Expr(self.parse_expr()?));
                if !self.consume(Token::Comma) {
                    break;
                }
            }
            items
        };

        if projection.is_empty() {
            return Err(self.error("SELECT requires at least one projection item"));
        }

        let from = if self.consume(Token::From) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(SelectStatement { projection, from })
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut values = Vec::new();
        loop {
            values.push(self.parse_expr()?);
            if !self.consume(Token::Comma) {
                break;
            }
        }
        Ok(values)
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>, ParseError> {
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_identifier()?);
            if !self.consume(Token::Comma) {
                break;
            }
        }

        if columns.is_empty() {
            return Err(self.error("expected identifier"));
        }

        Ok(columns)
    }

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_primary_expr()?;

        while self.consume(Token::Plus) {
            let rhs = self.parse_primary_expr()?;
            expr = Expr::Binary {
                left: Box::new(expr),
                op: BinaryOperator::Add,
                right: Box::new(rhs),
            };
        }

        Ok(expr)
    }

    fn parse_primary_expr(&mut self) -> Result<Expr, ParseError> {
        match self.peek() {
            Token::Identifier(_) => self.parse_identifier().map(Expr::Identifier),
            Token::Integer(value) => {
                let parsed = *value;
                self.advance();
                Ok(Expr::Integer(parsed))
            }
            Token::String(value) => {
                let parsed = value.clone();
                self.advance();
                Ok(Expr::String(parsed))
            }
            other => Err(self.error(format!("expected expression, found token {:?}", other))),
        }
    }

    fn parse_identifier(&mut self) -> Result<String, ParseError> {
        match self.peek() {
            Token::Identifier(name) => {
                let value = name.clone();
                self.advance();
                Ok(value)
            }
            other => Err(self.error(format!("expected identifier, found token {:?}", other))),
        }
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.position].token
    }

    fn advance(&mut self) {
        if self.position + 1 < self.tokens.len() {
            self.position += 1;
        }
    }

    fn consume(&mut self, expected: Token) -> bool {
        if *self.peek() == expected {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, expected: Token) -> Result<(), ParseError> {
        if self.consume(expected.clone()) {
            Ok(())
        } else {
            Err(self.error(format!(
                "expected token {:?}, found {:?}",
                expected,
                self.peek()
            )))
        }
    }

    fn error(&self, message: impl Into<String>) -> ParseError {
        let pos = self.tokens[self.position].position;
        ParseError::new(pos, message)
    }
}
