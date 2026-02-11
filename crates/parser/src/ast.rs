/// AST node definitions for the current stage-1 parser scope.
///
/// The parser currently supports `CREATE TABLE`, `INSERT`, and `SELECT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    CreateTable(CreateTableStatement),
    Insert(InsertStatement),
    Select(SelectStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertStatement {
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStatement {
    pub projection: Vec<SelectItem>,
    pub from: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectItem {
    Wildcard,
    Expr(Expr),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Identifier(String),
    Integer(i64),
    String(String),
    Binary {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    Add,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_table_statement_is_structurally_comparable() {
        let stmt = Statement::CreateTable(CreateTableStatement {
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
        });

        assert_eq!(
            stmt,
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
    fn insert_statement_holds_columns_and_values() {
        let stmt = Statement::Insert(InsertStatement {
            table_name: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            values: vec![Expr::Integer(1), Expr::String("Alice".to_string())],
        });

        let Statement::Insert(insert) = stmt else {
            panic!("expected insert statement");
        };
        assert_eq!(insert.table_name, "users");
        assert_eq!(insert.columns, vec!["id", "name"]);
        assert_eq!(insert.values.len(), 2);
    }

    #[test]
    fn select_statement_represents_projection_and_from() {
        let stmt = Statement::Select(SelectStatement {
            projection: vec![
                SelectItem::Expr(Expr::Identifier("id".to_string())),
                SelectItem::Wildcard,
            ],
            from: Some("users".to_string()),
        });

        let Statement::Select(select) = stmt else {
            panic!("expected select statement");
        };
        assert_eq!(select.projection.len(), 2);
        assert_eq!(select.from, Some("users".to_string()));
    }
}
