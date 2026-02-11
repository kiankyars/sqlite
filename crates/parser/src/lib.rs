/// SQL parser and AST definitions for ralph-sqlite.
///
/// Modules:
/// - `token`: Token and keyword type definitions
/// - `ast`: AST node types for SQL statements and expressions
/// - `tokenizer`: SQL lexer producing a token stream
/// - `parser`: Recursive-descent parser producing AST from tokens
pub mod ast;
pub mod parser;
pub mod token;
pub mod tokenizer;

use ast::Stmt;
use parser::Parser;
use tokenizer::Tokenizer;

/// Parse a SQL string into an AST statement.
pub fn parse(input: &str) -> Result<Stmt, String> {
    let tokens = Tokenizer::new(input).tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse_stmt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    #[test]
    fn test_parse_select_literal() {
        let stmt = parse("SELECT 1;").unwrap();
        match stmt {
            Stmt::Select(s) => {
                assert_eq!(s.columns.len(), 1);
                assert!(s.from.is_none());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_create_table() {
        let stmt = parse("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);").unwrap();
        match stmt {
            Stmt::CreateTable(ct) => {
                assert_eq!(ct.table, "t");
                assert_eq!(ct.columns.len(), 2);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let stmt = parse("INSERT INTO t VALUES (1, 'hello');").unwrap();
        match stmt {
            Stmt::Insert(ins) => {
                assert_eq!(ins.table, "t");
                assert_eq!(ins.values[0].len(), 2);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn test_parse_update() {
        let stmt = parse("UPDATE t SET x = 1 WHERE id = 2;").unwrap();
        match stmt {
            Stmt::Update(u) => {
                assert_eq!(u.table, "t");
                assert_eq!(u.assignments.len(), 1);
                assert!(u.where_clause.is_some());
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let stmt = parse("DELETE FROM t WHERE id = 1;").unwrap();
        match stmt {
            Stmt::Delete(d) => {
                assert_eq!(d.table, "t");
                assert!(d.where_clause.is_some());
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_parse_drop_table() {
        let stmt = parse("DROP TABLE IF EXISTS t;").unwrap();
        match stmt {
            Stmt::DropTable(dt) => {
                assert_eq!(dt.table, "t");
                assert!(dt.if_exists);
            }
            _ => panic!("expected DropTable"),
        }
    }

    #[test]
    fn test_parse_drop_index() {
        let stmt = parse("DROP INDEX IF EXISTS idx_t_x;").unwrap();
        match stmt {
            Stmt::DropIndex(di) => {
                assert_eq!(di.index, "idx_t_x");
                assert!(di.if_exists);
            }
            _ => panic!("expected DropIndex"),
        }
    }

    #[test]
    fn test_parse_create_index() {
        let stmt = parse("CREATE INDEX idx_t_x ON t(x);").unwrap();
        match stmt {
            Stmt::CreateIndex(ci) => {
                assert_eq!(ci.index, "idx_t_x");
                assert_eq!(ci.table, "t");
                assert_eq!(ci.columns, vec!["x".to_string()]);
                assert!(!ci.unique);
                assert!(!ci.if_not_exists);
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn test_parse_begin() {
        let stmt = parse("BEGIN;").unwrap();
        assert_eq!(stmt, Stmt::Begin);
    }

    #[test]
    fn test_parse_commit_transaction() {
        let stmt = parse("COMMIT TRANSACTION;").unwrap();
        assert_eq!(stmt, Stmt::Commit);
    }

    #[test]
    fn test_parse_rollback() {
        let stmt = parse("ROLLBACK;").unwrap();
        assert_eq!(stmt, Stmt::Rollback);
    }

    #[test]
    fn test_roundtrip_complex() {
        // A more complex query to verify end-to-end parsing
        let stmt = parse(
            "SELECT id, name, COUNT(*) FROM users WHERE active = 1 AND age > 18 ORDER BY name ASC LIMIT 100;"
        ).unwrap();
        match stmt {
            Stmt::Select(s) => {
                assert_eq!(s.columns.len(), 3);
                assert!(s.from.is_some());
                assert!(s.where_clause.is_some());
                assert_eq!(s.order_by.len(), 1);
                assert!(!s.order_by[0].descending);
                assert_eq!(s.limit, Some(Expr::IntegerLiteral(100)));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_group_by_having() {
        let stmt =
            parse("SELECT score, COUNT(*) FROM users GROUP BY score HAVING COUNT(*) > 1;").unwrap();
        match stmt {
            Stmt::Select(s) => {
                assert_eq!(s.group_by.len(), 1);
                assert!(s.having.is_some());
            }
            _ => panic!("expected Select"),
        }
    }
}
