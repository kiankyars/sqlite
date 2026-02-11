/// Recursive-descent SQL parser — converts tokens into AST nodes.
use crate::ast::*;
use crate::token::{Keyword, Token};

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    pub fn parse_stmt(&mut self) -> Result<Stmt, String> {
        let stmt = match self.peek() {
            Token::Keyword(Keyword::Select) => Stmt::Select(self.parse_select()?),
            Token::Keyword(Keyword::Insert) => Stmt::Insert(self.parse_insert()?),
            Token::Keyword(Keyword::Create) => self.parse_create()?,
            Token::Keyword(Keyword::Update) => Stmt::Update(self.parse_update()?),
            Token::Keyword(Keyword::Delete) => Stmt::Delete(self.parse_delete()?),
            Token::Keyword(Keyword::Drop) => self.parse_drop()?,
            Token::Keyword(Keyword::Begin) => self.parse_begin()?,
            Token::Keyword(Keyword::Commit) => self.parse_commit()?,
            Token::Keyword(Keyword::Rollback) => self.parse_rollback()?,
            other => return Err(format!("expected statement, found {:?}", other)),
        };
        // Consume optional trailing semicolon
        if self.peek() == &Token::Semicolon {
            self.advance();
        }
        Ok(stmt)
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> &Token {
        let tok = self.tokens.get(self.pos).unwrap_or(&Token::Eof);
        self.pos += 1;
        tok
    }

    fn expect_keyword(&mut self, kw: Keyword) -> Result<(), String> {
        match self.advance() {
            Token::Keyword(k) if *k == kw => Ok(()),
            other => Err(format!("expected {:?}, found {:?}", kw, other)),
        }
    }

    fn expect_token(&mut self, expected: &Token) -> Result<(), String> {
        let tok = self.advance().clone();
        if &tok == expected {
            Ok(())
        } else {
            Err(format!("expected {:?}, found {:?}", expected, tok))
        }
    }

    fn expect_ident(&mut self) -> Result<String, String> {
        match self.advance().clone() {
            Token::Ident(s) => Ok(s),
            other => Err(format!("expected identifier, found {:?}", other)),
        }
    }

    fn at_keyword(&self, kw: Keyword) -> bool {
        matches!(self.peek(), Token::Keyword(k) if *k == kw)
    }

    // ── SELECT ──────────────────────────────────────────────────────────

    fn parse_select(&mut self) -> Result<SelectStmt, String> {
        self.expect_keyword(Keyword::Select)?;

        let columns = self.parse_select_columns()?;

        let from = if self.at_keyword(Keyword::From) {
            self.advance();
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        let where_clause = if self.at_keyword(Keyword::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.at_keyword(Keyword::Group) {
            self.advance();
            self.expect_keyword(Keyword::By)?;
            self.parse_group_by_list()?
        } else {
            Vec::new()
        };

        let having = if self.at_keyword(Keyword::Having) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.at_keyword(Keyword::Order) {
            self.advance();
            self.expect_keyword(Keyword::By)?;
            self.parse_order_by_list()?
        } else {
            Vec::new()
        };

        let limit = if self.at_keyword(Keyword::Limit) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let offset = if self.at_keyword(Keyword::Offset) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(SelectStmt {
            columns,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>, String> {
        let mut cols = Vec::new();
        cols.push(self.parse_select_column()?);
        while self.peek() == &Token::Comma {
            self.advance();
            cols.push(self.parse_select_column()?);
        }
        Ok(cols)
    }

    fn parse_select_column(&mut self) -> Result<SelectColumn, String> {
        if self.peek() == &Token::Star {
            self.advance();
            return Ok(SelectColumn::AllColumns);
        }
        let expr = self.parse_expr()?;
        let alias = if self.at_keyword(Keyword::As) {
            self.advance();
            Some(self.expect_ident()?)
        } else {
            None
        };
        Ok(SelectColumn::Expr { expr, alias })
    }

    fn parse_from_clause(&mut self) -> Result<FromClause, String> {
        let table = self.expect_ident()?;
        let alias = if self.at_keyword(Keyword::As) {
            self.advance();
            Some(self.expect_ident()?)
        } else if matches!(self.peek(), Token::Ident(_)) {
            Some(self.expect_ident()?)
        } else {
            None
        };
        Ok(FromClause { table, alias })
    }

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByItem>, String> {
        let mut items = Vec::new();
        items.push(self.parse_order_by_item()?);
        while self.peek() == &Token::Comma {
            self.advance();
            items.push(self.parse_order_by_item()?);
        }
        Ok(items)
    }

    fn parse_group_by_list(&mut self) -> Result<Vec<Expr>, String> {
        let mut items = Vec::new();
        items.push(self.parse_expr()?);
        while self.peek() == &Token::Comma {
            self.advance();
            items.push(self.parse_expr()?);
        }
        Ok(items)
    }

    fn parse_order_by_item(&mut self) -> Result<OrderByItem, String> {
        let expr = self.parse_expr()?;
        let descending = if self.at_keyword(Keyword::Desc) {
            self.advance();
            true
        } else {
            if self.at_keyword(Keyword::Asc) {
                self.advance();
            }
            false
        };
        Ok(OrderByItem { expr, descending })
    }

    // ── INSERT ──────────────────────────────────────────────────────────

    fn parse_insert(&mut self) -> Result<InsertStmt, String> {
        self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;
        let table = self.expect_ident()?;

        let columns = if self.peek() == &Token::LeftParen {
            // Check if this is a column list or VALUES
            // Peek ahead: if next token after '(' is an ident followed by ',' or ')',
            // treat as column list.
            self.advance(); // consume '('
            let mut cols = Vec::new();
            cols.push(self.expect_ident()?);
            while self.peek() == &Token::Comma {
                self.advance();
                cols.push(self.expect_ident()?);
            }
            self.expect_token(&Token::RightParen)?;
            Some(cols)
        } else {
            None
        };

        self.expect_keyword(Keyword::Values)?;

        let mut values = Vec::new();
        values.push(self.parse_value_row()?);
        while self.peek() == &Token::Comma {
            self.advance();
            values.push(self.parse_value_row()?);
        }

        Ok(InsertStmt {
            table,
            columns,
            values,
        })
    }

    fn parse_value_row(&mut self) -> Result<Vec<Expr>, String> {
        self.expect_token(&Token::LeftParen)?;
        let mut exprs = Vec::new();
        exprs.push(self.parse_expr()?);
        while self.peek() == &Token::Comma {
            self.advance();
            exprs.push(self.parse_expr()?);
        }
        self.expect_token(&Token::RightParen)?;
        Ok(exprs)
    }

    // ── CREATE TABLE ────────────────────────────────────────────────────

    fn parse_create(&mut self) -> Result<Stmt, String> {
        self.expect_keyword(Keyword::Create)?;
        let unique = if self.at_keyword(Keyword::Unique) {
            self.advance();
            true
        } else {
            false
        };

        match self.peek() {
            Token::Keyword(Keyword::Table) => {
                if unique {
                    return Err("UNIQUE is only valid with CREATE INDEX".to_string());
                }
                Ok(Stmt::CreateTable(self.parse_create_table()?))
            }
            Token::Keyword(Keyword::Index) => {
                Ok(Stmt::CreateIndex(self.parse_create_index(unique)?))
            }
            other => Err(format!(
                "expected TABLE or INDEX after CREATE, found {:?}",
                other
            )),
        }
    }

    fn parse_create_table(&mut self) -> Result<CreateTableStmt, String> {
        self.expect_keyword(Keyword::Table)?;

        let if_not_exists = if self.at_keyword(Keyword::If) {
            self.advance();
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        let table = self.expect_ident()?;
        self.expect_token(&Token::LeftParen)?;

        let mut columns = Vec::new();
        columns.push(self.parse_column_def()?);
        while self.peek() == &Token::Comma {
            self.advance();
            columns.push(self.parse_column_def()?);
        }

        self.expect_token(&Token::RightParen)?;

        Ok(CreateTableStmt {
            if_not_exists,
            table,
            columns,
        })
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, String> {
        let name = self.expect_ident()?;

        let type_name = match self.peek() {
            Token::Keyword(Keyword::Integer) => {
                self.advance();
                Some(TypeName::Integer)
            }
            Token::Keyword(Keyword::Text) => {
                self.advance();
                Some(TypeName::Text)
            }
            Token::Keyword(Keyword::Real) => {
                self.advance();
                Some(TypeName::Real)
            }
            Token::Keyword(Keyword::Blob) => {
                self.advance();
                Some(TypeName::Blob)
            }
            _ => None,
        };

        let mut constraints = Vec::new();
        loop {
            match self.peek() {
                Token::Keyword(Keyword::Primary) => {
                    self.advance();
                    self.expect_keyword(Keyword::Key)?;
                    let autoincrement = if self.at_keyword(Keyword::Autoincrement) {
                        self.advance();
                        true
                    } else {
                        false
                    };
                    constraints.push(ColumnConstraint::PrimaryKey { autoincrement });
                }
                Token::Keyword(Keyword::Not) => {
                    self.advance();
                    self.expect_keyword(Keyword::Null)?;
                    constraints.push(ColumnConstraint::NotNull);
                }
                Token::Keyword(Keyword::Unique) => {
                    self.advance();
                    constraints.push(ColumnConstraint::Unique);
                }
                Token::Keyword(Keyword::Default) => {
                    self.advance();
                    let expr = self.parse_primary_expr()?;
                    constraints.push(ColumnConstraint::Default(expr));
                }
                _ => break,
            }
        }

        Ok(ColumnDef {
            name,
            type_name,
            constraints,
        })
    }

    fn parse_create_index(&mut self, unique: bool) -> Result<CreateIndexStmt, String> {
        self.expect_keyword(Keyword::Index)?;

        let if_not_exists = if self.at_keyword(Keyword::If) {
            self.advance();
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        let index = self.expect_ident()?;
        self.expect_keyword(Keyword::On)?;
        let table = self.expect_ident()?;
        self.expect_token(&Token::LeftParen)?;

        let mut columns = Vec::new();
        columns.push(self.expect_ident()?);
        while self.peek() == &Token::Comma {
            self.advance();
            columns.push(self.expect_ident()?);
        }

        self.expect_token(&Token::RightParen)?;
        Ok(CreateIndexStmt {
            if_not_exists,
            unique,
            index,
            table,
            columns,
        })
    }

    // ── UPDATE ──────────────────────────────────────────────────────────

    fn parse_update(&mut self) -> Result<UpdateStmt, String> {
        self.expect_keyword(Keyword::Update)?;
        let table = self.expect_ident()?;
        self.expect_keyword(Keyword::Set)?;

        let mut assignments = Vec::new();
        assignments.push(self.parse_assignment()?);
        while self.peek() == &Token::Comma {
            self.advance();
            assignments.push(self.parse_assignment()?);
        }

        let where_clause = if self.at_keyword(Keyword::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(UpdateStmt {
            table,
            assignments,
            where_clause,
        })
    }

    fn parse_assignment(&mut self) -> Result<Assignment, String> {
        let column = self.expect_ident()?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_expr()?;
        Ok(Assignment { column, value })
    }

    // ── DELETE ──────────────────────────────────────────────────────────

    fn parse_delete(&mut self) -> Result<DeleteStmt, String> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;
        let table = self.expect_ident()?;

        let where_clause = if self.at_keyword(Keyword::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(DeleteStmt {
            table,
            where_clause,
        })
    }

    // ── DROP TABLE / DROP INDEX ────────────────────────────────────────

    fn parse_drop(&mut self) -> Result<Stmt, String> {
        self.expect_keyword(Keyword::Drop)?;
        match self.peek() {
            Token::Keyword(Keyword::Table) => Ok(Stmt::DropTable(self.parse_drop_table()?)),
            Token::Keyword(Keyword::Index) => Ok(Stmt::DropIndex(self.parse_drop_index()?)),
            other => Err(format!(
                "expected TABLE or INDEX after DROP, found {:?}",
                other
            )),
        }
    }

    fn parse_drop_table(&mut self) -> Result<DropTableStmt, String> {
        self.expect_keyword(Keyword::Table)?;
        let if_exists = if self.at_keyword(Keyword::If) {
            self.advance();
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };
        let table = self.expect_ident()?;
        Ok(DropTableStmt { if_exists, table })
    }

    fn parse_drop_index(&mut self) -> Result<DropIndexStmt, String> {
        self.expect_keyword(Keyword::Index)?;
        let if_exists = if self.at_keyword(Keyword::If) {
            self.advance();
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };
        let index = self.expect_ident()?;
        Ok(DropIndexStmt { if_exists, index })
    }

    // ── Transaction control ─────────────────────────────────────────────

    fn parse_begin(&mut self) -> Result<Stmt, String> {
        self.expect_keyword(Keyword::Begin)?;
        if self.at_keyword(Keyword::Transaction) {
            self.advance();
        }
        Ok(Stmt::Begin)
    }

    fn parse_commit(&mut self) -> Result<Stmt, String> {
        self.expect_keyword(Keyword::Commit)?;
        if self.at_keyword(Keyword::Transaction) {
            self.advance();
        }
        Ok(Stmt::Commit)
    }

    fn parse_rollback(&mut self) -> Result<Stmt, String> {
        self.expect_keyword(Keyword::Rollback)?;
        if self.at_keyword(Keyword::Transaction) {
            self.advance();
        }
        Ok(Stmt::Rollback)
    }

    // ── Expression parsing (precedence climbing) ─────────────────────

    fn parse_expr(&mut self) -> Result<Expr, String> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_and_expr()?;
        while self.at_keyword(Keyword::Or) {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_not_expr()?;
        while self.at_keyword(Keyword::And) {
            self.advance();
            let right = self.parse_not_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<Expr, String> {
        if self.at_keyword(Keyword::Not) {
            self.advance();
            let expr = self.parse_not_expr()?;
            Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            })
        } else {
            self.parse_comparison_expr()
        }
    }

    fn parse_comparison_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_addition_expr()?;

        loop {
            let op = match self.peek() {
                Token::Eq => Some(BinaryOperator::Eq),
                Token::NotEq => Some(BinaryOperator::NotEq),
                Token::Lt => Some(BinaryOperator::Lt),
                Token::LtEq => Some(BinaryOperator::LtEq),
                Token::Gt => Some(BinaryOperator::Gt),
                Token::GtEq => Some(BinaryOperator::GtEq),
                Token::Keyword(Keyword::Like) => Some(BinaryOperator::Like),
                Token::Keyword(Keyword::Is) => {
                    self.advance();
                    let negated = if self.at_keyword(Keyword::Not) {
                        self.advance();
                        true
                    } else {
                        false
                    };
                    self.expect_keyword(Keyword::Null)?;
                    left = Expr::IsNull {
                        expr: Box::new(left),
                        negated,
                    };
                    continue;
                }
                Token::Keyword(Keyword::Between) => {
                    self.advance();
                    let low = self.parse_addition_expr()?;
                    self.expect_keyword(Keyword::And)?;
                    let high = self.parse_addition_expr()?;
                    left = Expr::Between {
                        expr: Box::new(left),
                        low: Box::new(low),
                        high: Box::new(high),
                        negated: false,
                    };
                    continue;
                }
                Token::Keyword(Keyword::Not) => {
                    // NOT BETWEEN or NOT IN or NOT LIKE
                    let saved_pos = self.pos;
                    self.advance();
                    match self.peek() {
                        Token::Keyword(Keyword::Between) => {
                            self.advance();
                            let low = self.parse_addition_expr()?;
                            self.expect_keyword(Keyword::And)?;
                            let high = self.parse_addition_expr()?;
                            left = Expr::Between {
                                expr: Box::new(left),
                                low: Box::new(low),
                                high: Box::new(high),
                                negated: true,
                            };
                            continue;
                        }
                        Token::Keyword(Keyword::In) => {
                            self.advance();
                            let list = self.parse_in_list()?;
                            left = Expr::InList {
                                expr: Box::new(left),
                                list,
                                negated: true,
                            };
                            continue;
                        }
                        Token::Keyword(Keyword::Like) => {
                            self.advance();
                            let right = self.parse_addition_expr()?;
                            // NOT LIKE is just a NOT wrapping a LIKE comparison
                            left = Expr::UnaryOp {
                                op: UnaryOperator::Not,
                                expr: Box::new(Expr::BinaryOp {
                                    left: Box::new(left),
                                    op: BinaryOperator::Like,
                                    right: Box::new(right),
                                }),
                            };
                            continue;
                        }
                        _ => {
                            // Not a postfix NOT expression, backtrack
                            self.pos = saved_pos;
                            break;
                        }
                    }
                }
                Token::Keyword(Keyword::In) => {
                    self.advance();
                    let list = self.parse_in_list()?;
                    left = Expr::InList {
                        expr: Box::new(left),
                        list,
                        negated: false,
                    };
                    continue;
                }
                _ => None,
            };

            if let Some(op) = op {
                self.advance();
                let right = self.parse_addition_expr()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    fn parse_in_list(&mut self) -> Result<Vec<Expr>, String> {
        self.expect_token(&Token::LeftParen)?;
        let mut list = Vec::new();
        list.push(self.parse_expr()?);
        while self.peek() == &Token::Comma {
            self.advance();
            list.push(self.parse_expr()?);
        }
        self.expect_token(&Token::RightParen)?;
        Ok(list)
    }

    fn parse_addition_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_multiplication_expr()?;
        loop {
            let op = match self.peek() {
                Token::Plus => BinaryOperator::Add,
                Token::Minus => BinaryOperator::Subtract,
                Token::Pipe => BinaryOperator::Concat,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplication_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_multiplication_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_unary_expr()?;
        loop {
            let op = match self.peek() {
                Token::Star => BinaryOperator::Multiply,
                Token::Slash => BinaryOperator::Divide,
                Token::Percent => BinaryOperator::Modulo,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary_expr(&mut self) -> Result<Expr, String> {
        match self.peek() {
            Token::Minus => {
                self.advance();
                let expr = self.parse_primary_expr()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Negate,
                    expr: Box::new(expr),
                })
            }
            Token::Plus => {
                self.advance();
                self.parse_primary_expr()
            }
            _ => self.parse_primary_expr(),
        }
    }

    fn parse_primary_expr(&mut self) -> Result<Expr, String> {
        match self.peek().clone() {
            Token::Integer(n) => {
                self.advance();
                Ok(Expr::IntegerLiteral(n))
            }
            Token::Float(f) => {
                self.advance();
                Ok(Expr::FloatLiteral(f))
            }
            Token::String(s) => {
                self.advance();
                Ok(Expr::StringLiteral(s))
            }
            Token::Keyword(Keyword::Null) => {
                self.advance();
                Ok(Expr::Null)
            }
            Token::Ident(name) => {
                self.advance();
                // Check for function call: name(...)
                if self.peek() == &Token::LeftParen {
                    self.advance(); // consume '('
                    let mut args = Vec::new();
                    if self.peek() != &Token::RightParen {
                        // Handle COUNT(*) and similar
                        if self.peek() == &Token::Star {
                            self.advance();
                            // Represent * as a special marker — use column ref
                            args.push(Expr::ColumnRef {
                                table: None,
                                column: "*".to_string(),
                            });
                        } else {
                            args.push(self.parse_expr()?);
                            while self.peek() == &Token::Comma {
                                self.advance();
                                args.push(self.parse_expr()?);
                            }
                        }
                    }
                    self.expect_token(&Token::RightParen)?;
                    Ok(Expr::FunctionCall {
                        name: name.to_uppercase(),
                        args,
                    })
                } else if self.peek() == &Token::Dot {
                    // Qualified column: table.column
                    self.advance(); // consume '.'
                    let column = self.expect_ident()?;
                    Ok(Expr::ColumnRef {
                        table: Some(name),
                        column,
                    })
                } else {
                    Ok(Expr::ColumnRef {
                        table: None,
                        column: name,
                    })
                }
            }
            // Handle aggregate keywords as function names (COUNT, SUM, etc.)
            Token::Keyword(
                kw @ (Keyword::Count | Keyword::Sum | Keyword::Avg | Keyword::Min | Keyword::Max),
            ) => {
                self.advance();
                let name = format!("{:?}", kw).to_uppercase();
                self.expect_token(&Token::LeftParen)?;
                let mut args = Vec::new();
                if self.peek() != &Token::RightParen {
                    if self.peek() == &Token::Star {
                        self.advance();
                        args.push(Expr::ColumnRef {
                            table: None,
                            column: "*".to_string(),
                        });
                    } else {
                        args.push(self.parse_expr()?);
                        while self.peek() == &Token::Comma {
                            self.advance();
                            args.push(self.parse_expr()?);
                        }
                    }
                }
                self.expect_token(&Token::RightParen)?;
                Ok(Expr::FunctionCall { name, args })
            }
            Token::LeftParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect_token(&Token::RightParen)?;
                Ok(Expr::Paren(Box::new(expr)))
            }
            other => Err(format!("expected expression, found {:?}", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::Tokenizer;

    fn parse(input: &str) -> Stmt {
        let tokens = Tokenizer::new(input).tokenize().unwrap();
        Parser::new(tokens).parse_stmt().unwrap()
    }

    #[test]
    fn test_select_literal() {
        let stmt = parse("SELECT 1;");
        match stmt {
            Stmt::Select(s) => {
                assert_eq!(s.columns.len(), 1);
                assert!(s.from.is_none());
                match &s.columns[0] {
                    SelectColumn::Expr { expr, alias } => {
                        assert_eq!(*expr, Expr::IntegerLiteral(1));
                        assert!(alias.is_none());
                    }
                    _ => panic!("expected Expr column"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_select_arithmetic() {
        let stmt = parse("SELECT 1 + 2;");
        match stmt {
            Stmt::Select(s) => {
                assert_eq!(s.columns.len(), 1);
                match &s.columns[0] {
                    SelectColumn::Expr { expr, .. } => {
                        assert_eq!(
                            *expr,
                            Expr::BinaryOp {
                                left: Box::new(Expr::IntegerLiteral(1)),
                                op: BinaryOperator::Add,
                                right: Box::new(Expr::IntegerLiteral(2)),
                            }
                        );
                    }
                    _ => panic!("expected Expr column"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_select_string() {
        let stmt = parse("SELECT 'hello';");
        match stmt {
            Stmt::Select(s) => match &s.columns[0] {
                SelectColumn::Expr { expr, .. } => {
                    assert_eq!(*expr, Expr::StringLiteral("hello".into()));
                }
                _ => panic!("expected Expr column"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_select_star_from() {
        let stmt = parse("SELECT * FROM users;");
        match stmt {
            Stmt::Select(s) => {
                assert_eq!(s.columns, vec![SelectColumn::AllColumns]);
                let from = s.from.unwrap();
                assert_eq!(from.table, "users");
                assert!(from.alias.is_none());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_select_columns_where() {
        let stmt = parse("SELECT id, name FROM users WHERE id = 1;");
        match stmt {
            Stmt::Select(s) => {
                assert_eq!(s.columns.len(), 2);
                assert!(s.where_clause.is_some());
                match &s.where_clause.unwrap() {
                    Expr::BinaryOp { left, op, right } => {
                        assert_eq!(
                            **left,
                            Expr::ColumnRef {
                                table: None,
                                column: "id".into(),
                            }
                        );
                        assert_eq!(*op, BinaryOperator::Eq);
                        assert_eq!(**right, Expr::IntegerLiteral(1));
                    }
                    _ => panic!("expected BinaryOp"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_create_table() {
        let stmt = parse(
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, email TEXT UNIQUE);"
        );
        match stmt {
            Stmt::CreateTable(ct) => {
                assert_eq!(ct.table, "users");
                assert!(!ct.if_not_exists);
                assert_eq!(ct.columns.len(), 3);
                assert_eq!(ct.columns[0].name, "id");
                assert_eq!(ct.columns[0].type_name, Some(TypeName::Integer));
                assert_eq!(
                    ct.columns[0].constraints,
                    vec![ColumnConstraint::PrimaryKey {
                        autoincrement: true
                    }]
                );
                assert_eq!(ct.columns[1].name, "name");
                assert_eq!(ct.columns[1].type_name, Some(TypeName::Text));
                assert_eq!(ct.columns[1].constraints, vec![ColumnConstraint::NotNull]);
                assert_eq!(ct.columns[2].name, "email");
                assert_eq!(ct.columns[2].constraints, vec![ColumnConstraint::Unique]);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let stmt = parse("CREATE TABLE IF NOT EXISTS t (x INTEGER);");
        match stmt {
            Stmt::CreateTable(ct) => {
                assert!(ct.if_not_exists);
                assert_eq!(ct.table, "t");
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_create_index() {
        let stmt = parse("CREATE INDEX idx_users_name ON users(name);");
        match stmt {
            Stmt::CreateIndex(ci) => {
                assert!(!ci.unique);
                assert!(!ci.if_not_exists);
                assert_eq!(ci.index, "idx_users_name");
                assert_eq!(ci.table, "users");
                assert_eq!(ci.columns, vec!["name".to_string()]);
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn test_create_unique_index_if_not_exists() {
        let stmt = parse("CREATE UNIQUE INDEX IF NOT EXISTS idx_t_a_b ON t(a, b);");
        match stmt {
            Stmt::CreateIndex(ci) => {
                assert!(ci.unique);
                assert!(ci.if_not_exists);
                assert_eq!(ci.index, "idx_t_a_b");
                assert_eq!(ci.table, "t");
                assert_eq!(ci.columns, vec!["a".to_string(), "b".to_string()]);
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn test_insert() {
        let stmt = parse("INSERT INTO users (name, email) VALUES ('Alice', 'a@b.com');");
        match stmt {
            Stmt::Insert(ins) => {
                assert_eq!(ins.table, "users");
                assert_eq!(
                    ins.columns,
                    Some(vec!["name".to_string(), "email".to_string()])
                );
                assert_eq!(ins.values.len(), 1);
                assert_eq!(ins.values[0].len(), 2);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_insert_no_columns() {
        let stmt = parse("INSERT INTO t VALUES (1, 'hello');");
        match stmt {
            Stmt::Insert(ins) => {
                assert_eq!(ins.table, "t");
                assert!(ins.columns.is_none());
                assert_eq!(ins.values[0].len(), 2);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_insert_multiple_rows() {
        let stmt = parse("INSERT INTO t VALUES (1), (2), (3);");
        match stmt {
            Stmt::Insert(ins) => {
                assert_eq!(ins.values.len(), 3);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_update() {
        let stmt = parse("UPDATE users SET name = 'Bob' WHERE id = 1;");
        match stmt {
            Stmt::Update(upd) => {
                assert_eq!(upd.table, "users");
                assert_eq!(upd.assignments.len(), 1);
                assert_eq!(upd.assignments[0].column, "name");
                assert!(upd.where_clause.is_some());
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn test_delete() {
        let stmt = parse("DELETE FROM users WHERE id = 1;");
        match stmt {
            Stmt::Delete(del) => {
                assert_eq!(del.table, "users");
                assert!(del.where_clause.is_some());
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_delete_all() {
        let stmt = parse("DELETE FROM users;");
        match stmt {
            Stmt::Delete(del) => {
                assert_eq!(del.table, "users");
                assert!(del.where_clause.is_none());
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_drop_table() {
        let stmt = parse("DROP TABLE users;");
        match stmt {
            Stmt::DropTable(dt) => {
                assert_eq!(dt.table, "users");
                assert!(!dt.if_exists);
            }
            _ => panic!("expected DropTable"),
        }
    }

    #[test]
    fn test_drop_table_if_exists() {
        let stmt = parse("DROP TABLE IF EXISTS users;");
        match stmt {
            Stmt::DropTable(dt) => {
                assert_eq!(dt.table, "users");
                assert!(dt.if_exists);
            }
            _ => panic!("expected DropTable"),
        }
    }

    #[test]
    fn test_drop_index() {
        let stmt = parse("DROP INDEX idx_users_name;");
        match stmt {
            Stmt::DropIndex(di) => {
                assert_eq!(di.index, "idx_users_name");
                assert!(!di.if_exists);
            }
            _ => panic!("expected DropIndex"),
        }
    }

    #[test]
    fn test_drop_index_if_exists() {
        let stmt = parse("DROP INDEX IF EXISTS idx_users_name;");
        match stmt {
            Stmt::DropIndex(di) => {
                assert_eq!(di.index, "idx_users_name");
                assert!(di.if_exists);
            }
            _ => panic!("expected DropIndex"),
        }
    }

    #[test]
    fn test_begin_transaction() {
        let stmt = parse("BEGIN TRANSACTION;");
        assert_eq!(stmt, Stmt::Begin);
    }

    #[test]
    fn test_commit_transaction() {
        let stmt = parse("COMMIT TRANSACTION;");
        assert_eq!(stmt, Stmt::Commit);
    }

    #[test]
    fn test_rollback_transaction() {
        let stmt = parse("ROLLBACK;");
        assert_eq!(stmt, Stmt::Rollback);
    }

    #[test]
    fn test_select_with_alias() {
        let stmt = parse("SELECT id AS user_id FROM users;");
        match stmt {
            Stmt::Select(s) => match &s.columns[0] {
                SelectColumn::Expr { alias, .. } => {
                    assert_eq!(alias.as_deref(), Some("user_id"));
                }
                _ => panic!("expected Expr"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_select_order_by_limit() {
        let stmt = parse("SELECT * FROM t ORDER BY id DESC LIMIT 10;");
        match stmt {
            Stmt::Select(s) => {
                assert_eq!(s.order_by.len(), 1);
                assert!(s.order_by[0].descending);
                assert_eq!(s.limit, Some(Expr::IntegerLiteral(10)));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_select_group_by() {
        let stmt = parse("SELECT score, COUNT(*) FROM t GROUP BY score;");
        match stmt {
            Stmt::Select(s) => {
                assert_eq!(s.group_by.len(), 1);
                assert!(s.having.is_none());
                assert!(matches!(
                    s.group_by[0],
                    Expr::ColumnRef {
                        table: None,
                        ref column
                    } if column == "score"
                ));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_select_group_by_having_order_by() {
        let stmt = parse(
            "SELECT score, COUNT(*) FROM t GROUP BY score HAVING COUNT(*) > 1 ORDER BY score ASC;",
        );
        match stmt {
            Stmt::Select(s) => {
                assert_eq!(s.group_by.len(), 1);
                assert!(s.having.is_some());
                assert_eq!(s.order_by.len(), 1);
                assert!(!s.order_by[0].descending);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_select_having_without_group_by() {
        let stmt = parse("SELECT COUNT(*) FROM t HAVING COUNT(*) > 0;");
        match stmt {
            Stmt::Select(s) => {
                assert!(s.group_by.is_empty());
                assert!(s.having.is_some());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_operator_precedence() {
        // 1 + 2 * 3 should parse as 1 + (2 * 3)
        let stmt = parse("SELECT 1 + 2 * 3;");
        match stmt {
            Stmt::Select(s) => match &s.columns[0] {
                SelectColumn::Expr { expr, .. } => {
                    assert_eq!(
                        *expr,
                        Expr::BinaryOp {
                            left: Box::new(Expr::IntegerLiteral(1)),
                            op: BinaryOperator::Add,
                            right: Box::new(Expr::BinaryOp {
                                left: Box::new(Expr::IntegerLiteral(2)),
                                op: BinaryOperator::Multiply,
                                right: Box::new(Expr::IntegerLiteral(3)),
                            }),
                        }
                    );
                }
                _ => panic!("expected Expr"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_complex_where() {
        let stmt = parse("SELECT * FROM t WHERE a = 1 AND b > 2 OR c < 3;");
        match stmt {
            Stmt::Select(s) => {
                // OR has lower precedence than AND, so: (a=1 AND b>2) OR c<3
                assert!(s.where_clause.is_some());
                match &s.where_clause.unwrap() {
                    Expr::BinaryOp { op, .. } => assert_eq!(*op, BinaryOperator::Or),
                    _ => panic!("expected OR at top"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_is_null() {
        let stmt = parse("SELECT * FROM t WHERE x IS NULL;");
        match stmt {
            Stmt::Select(s) => match &s.where_clause.unwrap() {
                Expr::IsNull { negated, .. } => assert!(!negated),
                _ => panic!("expected IsNull"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_is_not_null() {
        let stmt = parse("SELECT * FROM t WHERE x IS NOT NULL;");
        match stmt {
            Stmt::Select(s) => match &s.where_clause.unwrap() {
                Expr::IsNull { negated, .. } => assert!(*negated),
                _ => panic!("expected IsNull"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_between() {
        let stmt = parse("SELECT * FROM t WHERE x BETWEEN 1 AND 10;");
        match stmt {
            Stmt::Select(s) => match &s.where_clause.unwrap() {
                Expr::Between { negated, .. } => assert!(!negated),
                _ => panic!("expected Between"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_in_list() {
        let stmt = parse("SELECT * FROM t WHERE x IN (1, 2, 3);");
        match stmt {
            Stmt::Select(s) => match &s.where_clause.unwrap() {
                Expr::InList { list, negated, .. } => {
                    assert!(!negated);
                    assert_eq!(list.len(), 3);
                }
                _ => panic!("expected InList"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_count_star() {
        let stmt = parse("SELECT COUNT(*) FROM t;");
        match stmt {
            Stmt::Select(s) => match &s.columns[0] {
                SelectColumn::Expr { expr, .. } => match expr {
                    Expr::FunctionCall { name, args } => {
                        assert_eq!(name, "COUNT");
                        assert_eq!(args.len(), 1);
                    }
                    _ => panic!("expected FunctionCall"),
                },
                _ => panic!("expected Expr"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_unary_negate() {
        let stmt = parse("SELECT -1;");
        match stmt {
            Stmt::Select(s) => match &s.columns[0] {
                SelectColumn::Expr { expr, .. } => {
                    assert_eq!(
                        *expr,
                        Expr::UnaryOp {
                            op: UnaryOperator::Negate,
                            expr: Box::new(Expr::IntegerLiteral(1)),
                        }
                    );
                }
                _ => panic!("expected Expr"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parenthesized_expr() {
        let stmt = parse("SELECT (1 + 2) * 3;");
        match stmt {
            Stmt::Select(s) => match &s.columns[0] {
                SelectColumn::Expr { expr, .. } => match expr {
                    Expr::BinaryOp { op, .. } => {
                        assert_eq!(*op, BinaryOperator::Multiply);
                    }
                    _ => panic!("expected BinaryOp"),
                },
                _ => panic!("expected Expr"),
            },
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_qualified_column() {
        let stmt = parse("SELECT t.id FROM t;");
        match stmt {
            Stmt::Select(s) => match &s.columns[0] {
                SelectColumn::Expr { expr, .. } => {
                    assert_eq!(
                        *expr,
                        Expr::ColumnRef {
                            table: Some("t".into()),
                            column: "id".into(),
                        }
                    );
                }
                _ => panic!("expected Expr"),
            },
            _ => panic!("expected Select"),
        }
    }
}
