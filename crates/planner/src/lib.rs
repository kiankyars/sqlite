//! Query planner primitives.
//!
//! The current planner scope is intentionally small:
//! - recognize single-table `WHERE` predicates that can use an index
//! - choose between full table scan and index-driven lookup

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
    IndexRange {
        index_name: String,
        column: String,
        lower: Option<RangeBound>,
        upper: Option<RangeBound>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RangeBound {
    pub value_expr: Expr,
    pub inclusive: bool,
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
    choose_index_eq_access(expr, table_name, indexes)
        .or_else(|| choose_index_range_access(expr, table_name, indexes))
}

fn choose_index_eq_access(
    expr: &Expr,
    table_name: &str,
    indexes: &[IndexInfo],
) -> Option<AccessPath> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => choose_index_eq_access(left, table_name, indexes)
            .or_else(|| choose_index_eq_access(right, table_name, indexes)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => plan_index_eq(left, right, table_name, indexes)
            .or_else(|| plan_index_eq(right, left, table_name, indexes)),
        _ => None,
    }
}

fn choose_index_range_access(
    expr: &Expr,
    table_name: &str,
    indexes: &[IndexInfo],
) -> Option<AccessPath> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => choose_index_range_access(left, table_name, indexes)
            .or_else(|| choose_index_range_access(right, table_name, indexes)),
        Expr::BinaryOp { left, op, right } => {
            plan_index_range_binary(left, *op, right, table_name, indexes).or_else(|| {
                invert_comparison(*op).and_then(|inverted| {
                    plan_index_range_binary(right, inverted, left, table_name, indexes)
                })
            })
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            if *negated {
                return None;
            }
            let (col_table, col_name) = match expr.as_ref() {
                Expr::ColumnRef { table, column } => (table.as_deref(), column.as_str()),
                _ => return None,
            };
            if let Some(qualifier) = col_table {
                if !qualifier.eq_ignore_ascii_case(table_name) {
                    return None;
                }
            }
            if expr_contains_column_ref(low) || expr_contains_column_ref(high) {
                return None;
            }
            let index = find_matching_index(table_name, col_name, indexes)?;
            Some(AccessPath::IndexRange {
                index_name: index.name.clone(),
                column: index.column.clone(),
                lower: Some(RangeBound {
                    value_expr: low.as_ref().clone(),
                    inclusive: true,
                }),
                upper: Some(RangeBound {
                    value_expr: high.as_ref().clone(),
                    inclusive: true,
                }),
            })
        }
        _ => None,
    }
}

fn plan_index_range_binary(
    lhs: &Expr,
    op: BinaryOperator,
    rhs: &Expr,
    table_name: &str,
    indexes: &[IndexInfo],
) -> Option<AccessPath> {
    match op {
        BinaryOperator::Lt | BinaryOperator::LtEq | BinaryOperator::Gt | BinaryOperator::GtEq => {}
        _ => return None,
    }

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

    let index = find_matching_index(table_name, col_name, indexes)?;

    let (lower, upper) = match op {
        BinaryOperator::Gt => (
            Some(RangeBound {
                value_expr: rhs.clone(),
                inclusive: false,
            }),
            None,
        ),
        BinaryOperator::GtEq => (
            Some(RangeBound {
                value_expr: rhs.clone(),
                inclusive: true,
            }),
            None,
        ),
        BinaryOperator::Lt => (
            None,
            Some(RangeBound {
                value_expr: rhs.clone(),
                inclusive: false,
            }),
        ),
        BinaryOperator::LtEq => (
            None,
            Some(RangeBound {
                value_expr: rhs.clone(),
                inclusive: true,
            }),
        ),
        _ => return None,
    };

    Some(AccessPath::IndexRange {
        index_name: index.name.clone(),
        column: index.column.clone(),
        lower,
        upper,
    })
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

    let index = find_matching_index(table_name, col_name, indexes)?;

    Some(AccessPath::IndexEq {
        index_name: index.name.clone(),
        column: index.column.clone(),
        value_expr: rhs.clone(),
    })
}

fn find_matching_index<'a>(
    table_name: &str,
    col_name: &str,
    indexes: &'a [IndexInfo],
) -> Option<&'a IndexInfo> {
    indexes.iter().find(|idx| {
        idx.table.eq_ignore_ascii_case(table_name) && idx.column.eq_ignore_ascii_case(col_name)
    })
}

fn invert_comparison(op: BinaryOperator) -> Option<BinaryOperator> {
    match op {
        BinaryOperator::Lt => Some(BinaryOperator::Gt),
        BinaryOperator::LtEq => Some(BinaryOperator::GtEq),
        BinaryOperator::Gt => Some(BinaryOperator::Lt),
        BinaryOperator::GtEq => Some(BinaryOperator::LtEq),
        _ => None,
    }
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
                joins: Vec::new(),
            }),
            where_clause: None,
            group_by: Vec::new(),
            having: None,
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
    fn chooses_index_range_for_greater_than_predicate() {
        let stmt = parse_select("SELECT * FROM t WHERE score > 10;");
        let plan = plan_select(&stmt, "t", &default_indexes());
        assert_eq!(
            plan.access_path,
            AccessPath::IndexRange {
                index_name: "idx_t_score".to_string(),
                column: "score".to_string(),
                lower: Some(RangeBound {
                    value_expr: Expr::IntegerLiteral(10),
                    inclusive: false,
                }),
                upper: None,
            }
        );
    }

    #[test]
    fn chooses_index_range_for_reversed_comparison_predicate() {
        let stmt = parse_select("SELECT * FROM t WHERE 100 <= score;");
        let plan = plan_select(&stmt, "t", &default_indexes());
        assert_eq!(
            plan.access_path,
            AccessPath::IndexRange {
                index_name: "idx_t_score".to_string(),
                column: "score".to_string(),
                lower: Some(RangeBound {
                    value_expr: Expr::IntegerLiteral(100),
                    inclusive: true,
                }),
                upper: None,
            }
        );
    }

    #[test]
    fn chooses_index_range_for_between_predicate() {
        let stmt = parse_select("SELECT * FROM t WHERE score BETWEEN 3 AND 7;");
        let plan = plan_select(&stmt, "t", &default_indexes());
        assert_eq!(
            plan.access_path,
            AccessPath::IndexRange {
                index_name: "idx_t_score".to_string(),
                column: "score".to_string(),
                lower: Some(RangeBound {
                    value_expr: Expr::IntegerLiteral(3),
                    inclusive: true,
                }),
                upper: Some(RangeBound {
                    value_expr: Expr::IntegerLiteral(7),
                    inclusive: true,
                }),
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

    #[test]
    fn plan_where_chooses_index_for_range_predicate() {
        let where_expr = parse_where("SELECT * FROM t WHERE score <= 99;");
        let path = plan_where(where_expr.as_ref(), "t", &default_indexes());
        assert_eq!(
            path,
            AccessPath::IndexRange {
                index_name: "idx_t_score".to_string(),
                column: "score".to_string(),
                lower: None,
                upper: Some(RangeBound {
                    value_expr: Expr::IntegerLiteral(99),
                    inclusive: true,
                }),
            }
        );
    }
}
