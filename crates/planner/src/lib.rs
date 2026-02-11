//! Query planner primitives.
//!
//! The current planner scope is intentionally small:
//! - recognize single-table `WHERE` predicates that can use an index
//! - choose between full table scan and index equality lookup

use ralph_parser::ast::{BinaryOperator, Expr, SelectStmt};

/// Plan an access path from an arbitrary WHERE clause.
///
/// This is the general-purpose entry point used by UPDATE, DELETE, and any
/// statement that needs to decide between a full table scan and an index lookup.
pub fn plan_where(
    where_clause: Option<&Expr>,
    table_name: &str,
    indexes: &[IndexInfo],
) -> AccessPath {
    where_clause
        .and_then(|expr| choose_index_access(expr, table_name, indexes))
        .unwrap_or(AccessPath::TableScan)
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexInfo {
    pub name: String,
    pub table: String,
    pub column: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AccessPath {
    TableScan,
    IndexEq {
        index_name: String,
        column: String,
        value_expr: Expr,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectPlan {
    pub access_path: AccessPath,
}

pub fn plan_select(stmt: &SelectStmt, table_name: &str, indexes: &[IndexInfo]) -> SelectPlan {
    let access_path = stmt
        .where_clause
        .as_ref()
        .and_then(|expr| choose_index_access(expr, table_name, indexes))
        .unwrap_or(AccessPath::TableScan);
    SelectPlan { access_path }
}

fn choose_index_access(expr: &Expr, table_name: &str, indexes: &[IndexInfo]) -> Option<AccessPath> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => choose_index_access(left, table_name, indexes)
            .or_else(|| choose_index_access(right, table_name, indexes)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => plan_index_eq(left, right, table_name, indexes)
            .or_else(|| plan_index_eq(right, left, table_name, indexes)),
        _ => None,
    }
}

fn plan_index_eq(
    lhs: &Expr,
    rhs: &Expr,
    table_name: &str,
    indexes: &[IndexInfo],
) -> Option<AccessPath> {
    let (col_table, col_name) = match lhs {
        Expr::ColumnRef { table, column } => (table.as_deref(), column.as_str()),
        _ => return None,
    };

    if let Some(qualifier) = col_table {
        if !qualifier.eq_ignore_ascii_case(table_name) {
            return None;
        }
    }

    if expr_contains_column_ref(rhs) {
        return None;
    }

    let index = indexes.iter().find(|idx| {
        idx.table.eq_ignore_ascii_case(table_name) && idx.column.eq_ignore_ascii_case(col_name)
    })?;

    Some(AccessPath::IndexEq {
        index_name: index.name.clone(),
        column: index.column.clone(),
        value_expr: rhs.clone(),
    })
}

fn expr_contains_column_ref(expr: &Expr) -> bool {
    match expr {
        Expr::ColumnRef { .. } => true,
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_column_ref(left) || expr_contains_column_ref(right)
        }
        Expr::UnaryOp { expr, .. } => expr_contains_column_ref(expr),
        Expr::IsNull { expr, .. } => expr_contains_column_ref(expr),
        Expr::FunctionCall { args, .. } => args.iter().any(expr_contains_column_ref),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_column_ref(expr)
                || expr_contains_column_ref(low)
                || expr_contains_column_ref(high)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_column_ref(expr) || list.iter().any(expr_contains_column_ref)
        }
        Expr::Paren(inner) => expr_contains_column_ref(inner),
        Expr::IntegerLiteral(_) | Expr::FloatLiteral(_) | Expr::StringLiteral(_) | Expr::Null => {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ralph_parser::ast::{FromClause, SelectColumn};

    fn parse_select(sql: &str) -> SelectStmt {
        match ralph_parser::parse(sql).unwrap() {
            ralph_parser::ast::Stmt::Select(stmt) => stmt,
            other => panic!("expected SELECT, got: {other:?}"),
        }
    }

    fn default_indexes() -> Vec<IndexInfo> {
        vec![
            IndexInfo {
                name: "idx_t_score".to_string(),
                table: "t".to_string(),
                column: "score".to_string(),
            },
            IndexInfo {
                name: "idx_t_age".to_string(),
                table: "t".to_string(),
                column: "age".to_string(),
            },
        ]
    }

    #[test]
    fn chooses_table_scan_without_where() {
        let stmt = SelectStmt {
            columns: vec![SelectColumn::AllColumns],
            from: Some(FromClause {
                table: "t".to_string(),
                alias: None,
            }),
            where_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        let plan = plan_select(&stmt, "t", &default_indexes());
        assert_eq!(plan.access_path, AccessPath::TableScan);
    }

    #[test]
    fn chooses_index_for_equality_predicate() {
        let stmt = parse_select("SELECT * FROM t WHERE score = 42;");
        let plan = plan_select(&stmt, "t", &default_indexes());
        assert_eq!(
            plan.access_path,
            AccessPath::IndexEq {
                index_name: "idx_t_score".to_string(),
                column: "score".to_string(),
                value_expr: Expr::IntegerLiteral(42),
            }
        );
    }

    #[test]
    fn chooses_index_for_reversed_equality_predicate() {
        let stmt = parse_select("SELECT * FROM t WHERE 42 = score;");
        let plan = plan_select(&stmt, "t", &default_indexes());
        assert_eq!(
            plan.access_path,
            AccessPath::IndexEq {
                index_name: "idx_t_score".to_string(),
                column: "score".to_string(),
                value_expr: Expr::IntegerLiteral(42),
            }
        );
    }

    #[test]
    fn chooses_index_inside_and_predicate() {
        let stmt = parse_select("SELECT * FROM t WHERE age = 9 AND score > 1;");
        let plan = plan_select(&stmt, "t", &default_indexes());
        assert_eq!(
            plan.access_path,
            AccessPath::IndexEq {
                index_name: "idx_t_age".to_string(),
                column: "age".to_string(),
                value_expr: Expr::IntegerLiteral(9),
            }
        );
    }

    #[test]
    fn falls_back_when_rhs_uses_columns() {
        let stmt = parse_select("SELECT * FROM t WHERE score = age;");
        let plan = plan_select(&stmt, "t", &default_indexes());
        assert_eq!(plan.access_path, AccessPath::TableScan);
    }

    #[test]
    fn falls_back_when_column_has_no_index() {
        let stmt = parse_select("SELECT * FROM t WHERE id = 1;");
        let plan = plan_select(&stmt, "t", &default_indexes());
        assert_eq!(plan.access_path, AccessPath::TableScan);
    }

    fn parse_where(sql: &str) -> Option<Expr> {
        let stmt = parse_select(sql);
        stmt.where_clause
    }

    #[test]
    fn plan_where_returns_table_scan_without_where() {
        let path = plan_where(None, "t", &default_indexes());
        assert_eq!(path, AccessPath::TableScan);
    }

    #[test]
    fn plan_where_chooses_index_for_equality() {
        let where_expr = parse_where("SELECT * FROM t WHERE score = 42;");
        let path = plan_where(where_expr.as_ref(), "t", &default_indexes());
        assert_eq!(
            path,
            AccessPath::IndexEq {
                index_name: "idx_t_score".to_string(),
                column: "score".to_string(),
                value_expr: Expr::IntegerLiteral(42),
            }
        );
    }

    #[test]
    fn plan_where_falls_back_for_non_indexed_column() {
        let where_expr = parse_where("SELECT * FROM t WHERE id = 1;");
        let path = plan_where(where_expr.as_ref(), "t", &default_indexes());
        assert_eq!(path, AccessPath::TableScan);
    }
}
