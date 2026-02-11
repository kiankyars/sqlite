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
