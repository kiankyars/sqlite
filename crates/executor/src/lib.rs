/// Query executor: evaluates physical plans against storage.
///
/// This module implements a minimal Volcano-style iterator model with
/// `Scan`, `Filter`, and `Project` operators.
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use ralph_parser::ast::{BinaryOperator, Expr, UnaryOperator};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

pub type Row = Vec<Value>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorError {
    message: String,
}

impl ExecutorError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for ExecutorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for ExecutorError {}

pub type ExecResult<T> = Result<T, ExecutorError>;

pub trait Operator {
    fn open(&mut self) -> ExecResult<()>;
    fn next(&mut self) -> ExecResult<Option<Row>>;
    fn close(&mut self) -> ExecResult<()>;
}

pub type Predicate = Arc<dyn Fn(&Row) -> ExecResult<bool> + Send + Sync + 'static>;
pub type Projection = Arc<dyn Fn(&Row) -> ExecResult<Row> + Send + Sync + 'static>;

pub struct Scan {
    rows: Vec<Row>,
    cursor: usize,
    is_open: bool,
}

impl Scan {
    pub fn new(rows: Vec<Row>) -> Self {
        Self {
            rows,
            cursor: 0,
            is_open: false,
        }
    }
}

impl Operator for Scan {
    fn open(&mut self) -> ExecResult<()> {
        self.cursor = 0;
        self.is_open = true;
        Ok(())
    }

    fn next(&mut self) -> ExecResult<Option<Row>> {
        if !self.is_open {
            return Err(ExecutorError::new("operator is not open"));
        }
        if self.cursor >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.cursor].clone();
        self.cursor += 1;
        Ok(Some(row))
    }

    fn close(&mut self) -> ExecResult<()> {
        self.is_open = false;
        self.cursor = 0;
        Ok(())
    }
}

pub struct Filter {
    input: Box<dyn Operator>,
    predicate: Predicate,
    is_open: bool,
}

impl Filter {
    pub fn new<F>(input: Box<dyn Operator>, predicate: F) -> Self
    where
        F: Fn(&Row) -> ExecResult<bool> + Send + Sync + 'static,
    {
        Self {
            input,
            predicate: Arc::new(predicate),
            is_open: false,
        }
    }

    pub fn from_expr(input: Box<dyn Operator>, predicate: Expr, columns: Vec<String>) -> Self {
        Self::new(input, move |row| {
            let value = eval_expr(&predicate, Some((row, columns.as_slice())))?;
            Ok(is_truthy(&value))
        })
    }
}

impl Operator for Filter {
    fn open(&mut self) -> ExecResult<()> {
        self.input.open()?;
        self.is_open = true;
        Ok(())
    }

    fn next(&mut self) -> ExecResult<Option<Row>> {
        if !self.is_open {
            return Err(ExecutorError::new("operator is not open"));
        }
        loop {
            let Some(row) = self.input.next()? else {
                return Ok(None);
            };
            if (self.predicate)(&row)? {
                return Ok(Some(row));
            }
        }
    }

    fn close(&mut self) -> ExecResult<()> {
        if self.is_open {
            self.input.close()?;
        }
        self.is_open = false;
        Ok(())
    }
}

pub struct Project {
    input: Box<dyn Operator>,
    projection: Projection,
    is_open: bool,
}

impl Project {
    pub fn new<F>(input: Box<dyn Operator>, projection: F) -> Self
    where
        F: Fn(&Row) -> ExecResult<Row> + Send + Sync + 'static,
    {
        Self {
            input,
            projection: Arc::new(projection),
            is_open: false,
        }
    }

    pub fn from_exprs(input: Box<dyn Operator>, expressions: Vec<Expr>, columns: Vec<String>) -> Self {
        Self::new(input, move |row| {
            expressions
                .iter()
                .map(|expr| eval_expr(expr, Some((row, columns.as_slice()))))
                .collect()
        })
    }
}

impl Operator for Project {
    fn open(&mut self) -> ExecResult<()> {
        self.input.open()?;
        self.is_open = true;
        Ok(())
    }

    fn next(&mut self) -> ExecResult<Option<Row>> {
        if !self.is_open {
            return Err(ExecutorError::new("operator is not open"));
        }
        let Some(row) = self.input.next()? else {
            return Ok(None);
        };
        Ok(Some((self.projection)(&row)?))
    }

    fn close(&mut self) -> ExecResult<()> {
        if self.is_open {
            self.input.close()?;
        }
        self.is_open = false;
        Ok(())
    }
}

pub fn execute(mut root: Box<dyn Operator>) -> ExecResult<Vec<Row>> {
    root.open()?;
    let mut rows = Vec::new();
    while let Some(row) = root.next()? {
        rows.push(row);
    }
    root.close()?;
    Ok(rows)
}

pub fn eval_expr(expr: &Expr, row_ctx: Option<(&Row, &[String])>) -> ExecResult<Value> {
    match expr {
        Expr::IntegerLiteral(i) => Ok(Value::Integer(*i)),
        Expr::FloatLiteral(f) => Ok(Value::Real(*f)),
        Expr::StringLiteral(s) => Ok(Value::Text(s.clone())),
        Expr::Null => Ok(Value::Null),
        Expr::Paren(inner) => eval_expr(inner, row_ctx),
        Expr::ColumnRef { table, column } => {
            if table.is_some() {
                return Err(ExecutorError::new(
                    "table-qualified column references are not supported yet",
                ));
            }
            let (row, columns) = row_ctx
                .ok_or_else(|| ExecutorError::new("column reference requires row context"))?;
            if column == "*" {
                return Err(ExecutorError::new(
                    "'*' cannot be used as a scalar expression",
                ));
            }
            let idx = columns
                .iter()
                .position(|name| name.eq_ignore_ascii_case(column))
                .ok_or_else(|| ExecutorError::new(format!("unknown column '{column}'")))?;
            row.get(idx)
                .cloned()
                .ok_or_else(|| ExecutorError::new(format!("row is missing column '{column}'")))
        }
        Expr::UnaryOp { op, expr } => {
            let value = eval_expr(expr, row_ctx)?;
            match op {
                UnaryOperator::Negate => match value {
                    Value::Integer(i) => Ok(Value::Integer(-i)),
                    Value::Real(f) => Ok(Value::Real(-f)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ExecutorError::new("cannot negate non-numeric value")),
                },
                UnaryOperator::Not => Ok(Value::Integer((!is_truthy(&value)) as i64)),
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let lhs = eval_expr(left, row_ctx)?;
            let rhs = eval_expr(right, row_ctx)?;
            eval_binary_op(&lhs, *op, &rhs)
        }
        Expr::IsNull { expr, negated } => {
            let value = eval_expr(expr, row_ctx)?;
            let is_null = matches!(value, Value::Null);
            Ok(Value::Integer((if *negated { !is_null } else { is_null }) as i64))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let value = eval_expr(expr, row_ctx)?;
            let low_value = eval_expr(low, row_ctx)?;
            let high_value = eval_expr(high, row_ctx)?;
            let ge_low = compare_values(&value, &low_value)
                .map(|ord| ord >= std::cmp::Ordering::Equal)?;
            let le_high = compare_values(&value, &high_value)
                .map(|ord| ord <= std::cmp::Ordering::Equal)?;
            let between = ge_low && le_high;
            Ok(Value::Integer((if *negated { !between } else { between }) as i64))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let value = eval_expr(expr, row_ctx)?;
            let mut found = false;
            for item in list {
                let candidate = eval_expr(item, row_ctx)?;
                if values_equal(&value, &candidate) {
                    found = true;
                    break;
                }
            }
            Ok(Value::Integer((if *negated { !found } else { found }) as i64))
        }
        Expr::FunctionCall { name, .. } => Err(ExecutorError::new(format!(
            "function '{name}' is not supported yet"
        ))),
    }
}

fn eval_binary_op(lhs: &Value, op: BinaryOperator, rhs: &Value) -> ExecResult<Value> {
    use BinaryOperator::*;

    match op {
        Add | Subtract | Multiply | Divide | Modulo => eval_numeric_binary(lhs, op, rhs),
        Eq => Ok(Value::Integer(values_equal(lhs, rhs) as i64)),
        NotEq => Ok(Value::Integer((!values_equal(lhs, rhs)) as i64)),
        Lt => compare_values(lhs, rhs).map(|ord| Value::Integer((ord == std::cmp::Ordering::Less) as i64)),
        LtEq => compare_values(lhs, rhs).map(|ord| {
            Value::Integer((ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal) as i64)
        }),
        Gt => {
            compare_values(lhs, rhs).map(|ord| Value::Integer((ord == std::cmp::Ordering::Greater) as i64))
        }
        GtEq => compare_values(lhs, rhs).map(|ord| {
            Value::Integer((ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal) as i64)
        }),
        And => Ok(Value::Integer((is_truthy(lhs) && is_truthy(rhs)) as i64)),
        Or => Ok(Value::Integer((is_truthy(lhs) || is_truthy(rhs)) as i64)),
        Like => {
            let haystack = value_to_string(lhs);
            let needle = value_to_string(rhs).replace('%', "");
            Ok(Value::Integer(haystack.contains(&needle) as i64))
        }
        Concat => Ok(Value::Text(format!(
            "{}{}",
            value_to_string(lhs),
            value_to_string(rhs)
        ))),
    }
}

fn eval_numeric_binary(lhs: &Value, op: BinaryOperator, rhs: &Value) -> ExecResult<Value> {
    let (left, right, as_integer) = numeric_operands(lhs, rhs)?;
    let out = match op {
        BinaryOperator::Add => left + right,
        BinaryOperator::Subtract => left - right,
        BinaryOperator::Multiply => left * right,
        BinaryOperator::Divide => {
            if right == 0.0 {
                return Err(ExecutorError::new("division by zero"));
            }
            left / right
        }
        BinaryOperator::Modulo => {
            if right == 0.0 {
                return Err(ExecutorError::new("modulo by zero"));
            }
            left % right
        }
        _ => unreachable!("non-arithmetic operator passed to eval_numeric_binary"),
    };
    if as_integer {
        Ok(Value::Integer(out as i64))
    } else {
        Ok(Value::Real(out))
    }
}

fn numeric_operands(lhs: &Value, rhs: &Value) -> ExecResult<(f64, f64, bool)> {
    let left = value_to_f64(lhs)?;
    let right = value_to_f64(rhs)?;
    let both_int = matches!(lhs, Value::Integer(_)) && matches!(rhs, Value::Integer(_));
    Ok((left, right, both_int))
}

fn value_to_f64(value: &Value) -> ExecResult<f64> {
    match value {
        Value::Integer(i) => Ok(*i as f64),
        Value::Real(f) => Ok(*f),
        Value::Null => Ok(0.0),
        Value::Text(_) => Err(ExecutorError::new("expected numeric value")),
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => f.to_string(),
        Value::Text(s) => s.clone(),
    }
}

fn values_equal(lhs: &Value, rhs: &Value) -> bool {
    match (lhs, rhs) {
        (Value::Null, Value::Null) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::Real(a), Value::Real(b)) => a == b,
        (Value::Integer(a), Value::Real(b)) => (*a as f64) == *b,
        (Value::Real(a), Value::Integer(b)) => *a == (*b as f64),
        (Value::Text(a), Value::Text(b)) => a == b,
        _ => false,
    }
}

fn compare_values(lhs: &Value, rhs: &Value) -> ExecResult<std::cmp::Ordering> {
    match (lhs, rhs) {
        (Value::Integer(a), Value::Integer(b)) => Ok(a.cmp(b)),
        (Value::Real(a), Value::Real(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| ExecutorError::new("cannot compare NaN values")),
        (Value::Integer(a), Value::Real(b)) => (*a as f64)
            .partial_cmp(b)
            .ok_or_else(|| ExecutorError::new("cannot compare NaN values")),
        (Value::Real(a), Value::Integer(b)) => a
            .partial_cmp(&(*b as f64))
            .ok_or_else(|| ExecutorError::new("cannot compare NaN values")),
        (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),
        (Value::Null, Value::Null) => Ok(std::cmp::Ordering::Equal),
        _ => Err(ExecutorError::new("cannot compare values of different types")),
    }
}

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Integer(i) => *i != 0,
        Value::Real(f) => *f != 0.0,
        Value::Text(s) => !s.is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int(v: i64) -> Value {
        Value::Integer(v)
    }

    fn col(name: &str) -> Expr {
        Expr::ColumnRef {
            table: None,
            column: name.to_string(),
        }
    }

    fn bin(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    #[test]
    fn scan_emits_rows_in_order() {
        let mut scan = Scan::new(vec![vec![int(1)], vec![int(2)]]);
        scan.open().unwrap();
        assert_eq!(scan.next().unwrap(), Some(vec![int(1)]));
        assert_eq!(scan.next().unwrap(), Some(vec![int(2)]));
        assert_eq!(scan.next().unwrap(), None);
        scan.close().unwrap();
    }

    #[test]
    fn scan_next_before_open_errors() {
        let mut scan = Scan::new(vec![vec![int(1)]]);
        let err = scan.next().unwrap_err();
        assert_eq!(err.to_string(), "operator is not open");
    }

    #[test]
    fn filter_selects_only_matching_rows() {
        let scan = Scan::new(vec![vec![int(1)], vec![int(2)], vec![int(3)]]);
        let root = Filter::new(Box::new(scan), |row| match row[0] {
            Value::Integer(v) => Ok(v % 2 == 1),
            _ => Ok(false),
        });

        let out = execute(Box::new(root)).unwrap();
        assert_eq!(out, vec![vec![int(1)], vec![int(3)]]);
    }

    #[test]
    fn project_transforms_rows() {
        let scan = Scan::new(vec![vec![int(2)], vec![int(4)]]);
        let root = Project::new(Box::new(scan), |row| match row[0] {
            Value::Integer(v) => Ok(vec![int(v * 10)]),
            _ => Err(ExecutorError::new("expected integer")),
        });

        let out = execute(Box::new(root)).unwrap();
        assert_eq!(out, vec![vec![int(20)], vec![int(40)]]);
    }

    #[test]
    fn scan_filter_project_pipeline() {
        let scan = Scan::new(vec![
            vec![int(1), int(10)],
            vec![int(2), int(20)],
            vec![int(3), int(30)],
        ]);
        let filter = Filter::new(Box::new(scan), |row| match row[0] {
            Value::Integer(v) => Ok(v >= 2),
            _ => Ok(false),
        });
        let project = Project::new(Box::new(filter), |row| match (&row[0], &row[1]) {
            (Value::Integer(id), Value::Integer(score)) => Ok(vec![int(*id), int(*score + 1)]),
            _ => Err(ExecutorError::new("expected integer columns")),
        });

        let out = execute(Box::new(project)).unwrap();
        assert_eq!(out, vec![vec![int(2), int(21)], vec![int(3), int(31)]]);
    }

    #[test]
    fn predicate_error_is_returned() {
        let scan = Scan::new(vec![vec![int(1)]]);
        let root = Filter::new(Box::new(scan), |_row| {
            Err(ExecutorError::new("predicate failure"))
        });
        let err = execute(Box::new(root)).unwrap_err();
        assert_eq!(err.to_string(), "predicate failure");
    }

    #[test]
    fn eval_expr_handles_arithmetic_and_boolean_ops() {
        let expr = bin(
            bin(Expr::IntegerLiteral(7), BinaryOperator::Subtract, Expr::IntegerLiteral(2)),
            BinaryOperator::Eq,
            Expr::IntegerLiteral(5),
        );

        assert_eq!(eval_expr(&expr, None).unwrap(), int(1));
    }

    #[test]
    fn eval_expr_resolves_columns_from_row_context() {
        let row = vec![int(3), int(4)];
        let columns = vec!["a".to_string(), "b".to_string()];
        let expr = bin(col("a"), BinaryOperator::Multiply, col("b"));

        assert_eq!(
            eval_expr(&expr, Some((&row, columns.as_slice()))).unwrap(),
            int(12)
        );
    }

    #[test]
    fn filter_from_expr_applies_sql_predicate() {
        let scan = Scan::new(vec![vec![int(1), int(10)], vec![int(2), int(20)]]);
        let predicate = bin(col("id"), BinaryOperator::Gt, Expr::IntegerLiteral(1));
        let filter = Filter::from_expr(
            Box::new(scan),
            predicate,
            vec!["id".to_string(), "score".to_string()],
        );

        let out = execute(Box::new(filter)).unwrap();
        assert_eq!(out, vec![vec![int(2), int(20)]]);
    }

    #[test]
    fn project_from_exprs_materializes_expression_outputs() {
        let scan = Scan::new(vec![vec![int(2), int(20)]]);
        let projections = vec![
            col("id"),
            bin(col("score"), BinaryOperator::Add, Expr::IntegerLiteral(1)),
        ];
        let project = Project::from_exprs(
            Box::new(scan),
            projections,
            vec!["id".to_string(), "score".to_string()],
        );

        let out = execute(Box::new(project)).unwrap();
        assert_eq!(out, vec![vec![int(2), int(21)]]);
    }

    #[test]
    fn eval_expr_errors_on_unknown_column() {
        let row = vec![int(1)];
        let columns = vec!["known".to_string()];
        let err = eval_expr(&col("missing"), Some((&row, columns.as_slice()))).unwrap_err();
        assert_eq!(err.to_string(), "unknown column 'missing'");
    }
}
