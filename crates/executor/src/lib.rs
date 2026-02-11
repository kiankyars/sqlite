/// Query executor: evaluates physical plans against storage.
///
/// This module implements a minimal Volcano-style iterator model with
/// `Scan`, `Filter`, and `Project` operators.
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use ralph_parser::ast::{BinaryOperator, Expr, UnaryOperator};
use ralph_storage::pager::PageNum;
use ralph_storage::{BTree, Pager};

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

impl From<std::io::Error> for ExecutorError {
    fn from(err: std::io::Error) -> Self {
        Self::new(format!("io error: {err}"))
    }
}

impl From<String> for ExecutorError {
    fn from(err: String) -> Self {
        Self::new(err)
    }
}

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

pub struct TableScan<'a> {
    pager: &'a mut Pager,
    root_page: PageNum,
    rows: Vec<Row>,
    cursor: usize,
    is_open: bool,
}

impl<'a> TableScan<'a> {
    pub fn new(pager: &'a mut Pager, root_page: PageNum) -> Self {
        Self {
            pager,
            root_page,
            rows: Vec::new(),
            cursor: 0,
            is_open: false,
        }
    }
}

impl<'a> Operator for TableScan<'a> {
    fn open(&mut self) -> ExecResult<()> {
        let mut btree = BTree::new(self.pager, self.root_page);
        let entries = btree.scan_all()?;
        self.rows = entries
            .into_iter()
            .map(|entry| decode_row(&entry.payload))
            .collect::<Result<_, _>>()?;
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
        self.rows.clear();
        self.cursor = 0;
        Ok(())
    }
}

pub struct IndexEqScan<'a> {
    pager: &'a mut Pager,
    index_root: PageNum,
    table_root: PageNum,
    value: Value,
    rows: Vec<Row>,
    cursor: usize,
    is_open: bool,
}

impl<'a> IndexEqScan<'a> {
    pub fn new(
        pager: &'a mut Pager,
        index_root: PageNum,
        table_root: PageNum,
        value: Value,
    ) -> Self {
        Self {
            pager,
            index_root,
            table_root,
            value,
            rows: Vec::new(),
            cursor: 0,
            is_open: false,
        }
    }
}

impl<'a> Operator for IndexEqScan<'a> {
    fn open(&mut self) -> ExecResult<()> {
        let key = index_key_for_value(&self.value)?;

        // 1. Scan Index
        let rowids = {
            let mut index_tree = BTree::new(self.pager, self.index_root);
            match index_tree.lookup(key)? {
                Some(payload) => {
                    let buckets = decode_index_payload(&payload)?;
                    buckets
                        .into_iter()
                        .find(|b| values_equal(&b.value, &self.value))
                        .map(|b| b.rowids)
                        .unwrap_or_default()
                }
                None => Vec::new(),
            }
        };

        // 2. Fetch from Table
        let mut table_tree = BTree::new(self.pager, self.table_root);
        self.rows = Vec::with_capacity(rowids.len());
        for rowid in rowids {
            if let Some(payload) = table_tree.lookup(rowid)? {
                let row = decode_row(&payload)?;
                self.rows.push(row);
            }
        }

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
        self.rows.clear();
        self.cursor = 0;
        Ok(())
    }
}

pub struct Filter<'a> {
    input: Box<dyn Operator + 'a>,
    predicate: Predicate,
    is_open: bool,
}

impl<'a> Filter<'a> {
    pub fn new<F>(input: Box<dyn Operator + 'a>, predicate: F) -> Self
    where
        F: Fn(&Row) -> ExecResult<bool> + Send + Sync + 'static,
    {
        Self {
            input,
            predicate: Arc::new(predicate),
            is_open: false,
        }
    }

    pub fn from_expr(input: Box<dyn Operator + 'a>, predicate: Expr, columns: Vec<String>) -> Self {
        Self::new(input, move |row| {
            let value = eval_expr(&predicate, Some((row, columns.as_slice())))?;
            Ok(is_truthy(&value))
        })
    }
}

impl<'a> Operator for Filter<'a> {
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

pub struct Project<'a> {
    input: Box<dyn Operator + 'a>,
    projection: Projection,
    is_open: bool,
}

impl<'a> Project<'a> {
    pub fn new<F>(input: Box<dyn Operator + 'a>, projection: F) -> Self
    where
        F: Fn(&Row) -> ExecResult<Row> + Send + Sync + 'static,
    {
        Self {
            input,
            projection: Arc::new(projection),
            is_open: false,
        }
    }

    pub fn from_exprs(
        input: Box<dyn Operator + 'a>,
        expressions: Vec<Expr>,
        columns: Vec<String>,
    ) -> Self {
        Self::new(input, move |row| {
            expressions
                .iter()
                .map(|expr| eval_expr(expr, Some((row, columns.as_slice()))))
                .collect()
        })
    }
}

impl<'a> Operator for Project<'a> {
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

pub fn execute<'a>(mut root: Box<dyn Operator + 'a>) -> ExecResult<Vec<Row>> {
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
            Ok(Value::Integer(
                (if *negated { !is_null } else { is_null }) as i64,
            ))
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
            let ge_low =
                compare_values(&value, &low_value).map(|ord| ord >= std::cmp::Ordering::Equal)?;
            let le_high =
                compare_values(&value, &high_value).map(|ord| ord <= std::cmp::Ordering::Equal)?;
            let between = ge_low && le_high;
            Ok(Value::Integer(
                (if *negated { !between } else { between }) as i64,
            ))
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
            Ok(Value::Integer(
                (if *negated { !found } else { found }) as i64,
            ))
        }
        Expr::FunctionCall { name, args } => {
            let mut values = Vec::with_capacity(args.len());
            for arg in args {
                values.push(eval_expr(arg, row_ctx)?);
            }
            eval_scalar_function(name, &values)
        }
    }
}

pub fn eval_scalar_function(name: &str, args: &[Value]) -> ExecResult<Value> {
    if name.eq_ignore_ascii_case("LENGTH") {
        expect_arg_count(name, args, 1)?;
        if matches!(args[0], Value::Null) {
            return Ok(Value::Null);
        }
        return Ok(Value::Integer(
            value_to_string(&args[0]).chars().count() as i64
        ));
    }

    if name.eq_ignore_ascii_case("UPPER") {
        expect_arg_count(name, args, 1)?;
        if matches!(args[0], Value::Null) {
            return Ok(Value::Null);
        }
        return Ok(Value::Text(value_to_string(&args[0]).to_ascii_uppercase()));
    }

    if name.eq_ignore_ascii_case("LOWER") {
        expect_arg_count(name, args, 1)?;
        if matches!(args[0], Value::Null) {
            return Ok(Value::Null);
        }
        return Ok(Value::Text(value_to_string(&args[0]).to_ascii_lowercase()));
    }

    if name.eq_ignore_ascii_case("TYPEOF") {
        expect_arg_count(name, args, 1)?;
        let kind = match args[0] {
            Value::Null => "null",
            Value::Integer(_) => "integer",
            Value::Real(_) => "real",
            Value::Text(_) => "text",
        };
        return Ok(Value::Text(kind.to_string()));
    }

    if name.eq_ignore_ascii_case("ABS") {
        expect_arg_count(name, args, 1)?;
        return match args[0] {
            Value::Null => Ok(Value::Null),
            Value::Integer(i) => i
                .checked_abs()
                .map(Value::Integer)
                .ok_or_else(|| ExecutorError::new("integer overflow in ABS()")),
            Value::Real(f) => Ok(Value::Real(f.abs())),
            Value::Text(_) => Err(ExecutorError::new("ABS() expects a numeric value")),
        };
    }

    if name.eq_ignore_ascii_case("COALESCE") {
        if args.is_empty() {
            return Err(ExecutorError::new(
                "COALESCE() expects at least one argument",
            ));
        }
        for arg in args {
            if !matches!(arg, Value::Null) {
                return Ok(arg.clone());
            }
        }
        return Ok(Value::Null);
    }

    if name.eq_ignore_ascii_case("IFNULL") {
        expect_arg_count(name, args, 2)?;
        if matches!(args[0], Value::Null) {
            return Ok(args[1].clone());
        }
        return Ok(args[0].clone());
    }

    if name.eq_ignore_ascii_case("NULLIF") {
        expect_arg_count(name, args, 2)?;
        if values_equal(&args[0], &args[1]) {
            return Ok(Value::Null);
        }
        return Ok(args[0].clone());
    }

    if name.eq_ignore_ascii_case("SUBSTR") {
        if args.len() != 2 && args.len() != 3 {
            return Err(ExecutorError::new(
                "SUBSTR() expects exactly two or three arguments",
            ));
        }
        if matches!(args[0], Value::Null)
            || matches!(args[1], Value::Null)
            || (args.len() == 3 && matches!(args[2], Value::Null))
        {
            return Ok(Value::Null);
        }
        let source = value_to_string(&args[0]);
        let start = value_to_i64(&args[1])?;
        let length = if args.len() == 3 {
            Some(value_to_i64(&args[2])?)
        } else {
            None
        };
        return Ok(Value::Text(sql_substr(&source, start, length)));
    }

    if name.eq_ignore_ascii_case("INSTR") {
        expect_arg_count(name, args, 2)?;
        if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
            return Ok(Value::Null);
        }
        let haystack = value_to_string(&args[0]);
        let needle = value_to_string(&args[1]);
        if needle.is_empty() {
            return Ok(Value::Integer(1));
        }
        if let Some(byte_idx) = haystack.find(&needle) {
            return Ok(Value::Integer(
                haystack[..byte_idx].chars().count() as i64 + 1,
            ));
        }
        return Ok(Value::Integer(0));
    }

    if name.eq_ignore_ascii_case("REPLACE") {
        expect_arg_count(name, args, 3)?;
        if matches!(args[0], Value::Null)
            || matches!(args[1], Value::Null)
            || matches!(args[2], Value::Null)
        {
            return Ok(Value::Null);
        }
        let source = value_to_string(&args[0]);
        let from = value_to_string(&args[1]);
        let to = value_to_string(&args[2]);
        if from.is_empty() {
            return Ok(Value::Text(source));
        }
        return Ok(Value::Text(source.replace(&from, &to)));
    }

    if name.eq_ignore_ascii_case("TRIM") {
        return eval_trim_function(name, args, TrimDirection::Both);
    }
    if name.eq_ignore_ascii_case("LTRIM") {
        return eval_trim_function(name, args, TrimDirection::Left);
    }
    if name.eq_ignore_ascii_case("RTRIM") {
        return eval_trim_function(name, args, TrimDirection::Right);
    }

    Err(ExecutorError::new(format!(
        "function '{name}' is not supported yet"
    )))
}

#[derive(Clone, Copy)]
enum TrimDirection {
    Left,
    Right,
    Both,
}

fn eval_trim_function(name: &str, args: &[Value], direction: TrimDirection) -> ExecResult<Value> {
    if args.len() != 1 && args.len() != 2 {
        return Err(ExecutorError::new(format!(
            "{name}() expects one or two arguments"
        )));
    }
    if matches!(args[0], Value::Null) || (args.len() == 2 && matches!(args[1], Value::Null)) {
        return Ok(Value::Null);
    }

    let source = value_to_string(&args[0]);
    let trim_chars: Vec<char> = if args.len() == 2 {
        value_to_string(&args[1]).chars().collect()
    } else {
        vec![' ']
    };
    let input: Vec<char> = source.chars().collect();
    let mut start = 0usize;
    let mut end = input.len();

    if matches!(direction, TrimDirection::Left | TrimDirection::Both) {
        while start < end && trim_chars.contains(&input[start]) {
            start += 1;
        }
    }
    if matches!(direction, TrimDirection::Right | TrimDirection::Both) {
        while end > start && trim_chars.contains(&input[end - 1]) {
            end -= 1;
        }
    }

    Ok(Value::Text(input[start..end].iter().collect()))
}

fn expect_arg_count(name: &str, args: &[Value], expected: usize) -> ExecResult<()> {
    if args.len() == expected {
        Ok(())
    } else {
        Err(ExecutorError::new(format!(
            "{name}() expects exactly {expected} argument{}",
            if expected == 1 { "" } else { "s" }
        )))
    }
}

fn value_to_i64(value: &Value) -> ExecResult<i64> {
    match value {
        Value::Integer(i) => Ok(*i),
        Value::Real(f) => Ok(*f as i64),
        Value::Null => Ok(0),
        Value::Text(_) => Err(ExecutorError::new("expected integer value")),
    }
}

fn sql_substr(source: &str, start: i64, length: Option<i64>) -> String {
    let chars: Vec<char> = source.chars().collect();
    let char_len = chars.len() as i64;
    if char_len == 0 {
        return String::new();
    }

    let mut start_idx = if start > 0 {
        start - 1
    } else if start < 0 {
        char_len + start
    } else {
        0
    };
    start_idx = start_idx.clamp(0, char_len);

    let (begin, end) = match length {
        Some(len) if len < 0 => {
            let end = start_idx;
            let begin = (start_idx + len).clamp(0, end);
            (begin, end)
        }
        Some(len) => {
            let end = (start_idx + len).clamp(start_idx, char_len);
            (start_idx, end)
        }
        None => (start_idx, char_len),
    };

    chars[begin as usize..end as usize].iter().collect()
}

fn eval_binary_op(lhs: &Value, op: BinaryOperator, rhs: &Value) -> ExecResult<Value> {
    use BinaryOperator::*;

    match op {
        Add | Subtract | Multiply | Divide | Modulo => eval_numeric_binary(lhs, op, rhs),
        Eq => Ok(Value::Integer(values_equal(lhs, rhs) as i64)),
        NotEq => Ok(Value::Integer((!values_equal(lhs, rhs)) as i64)),
        Lt => compare_values(lhs, rhs)
            .map(|ord| Value::Integer((ord == std::cmp::Ordering::Less) as i64)),
        LtEq => compare_values(lhs, rhs).map(|ord| {
            Value::Integer(
                (ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal) as i64,
            )
        }),
        Gt => compare_values(lhs, rhs)
            .map(|ord| Value::Integer((ord == std::cmp::Ordering::Greater) as i64)),
        GtEq => compare_values(lhs, rhs).map(|ord| {
            Value::Integer(
                (ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal) as i64,
            )
        }),
        And => Ok(Value::Integer((is_truthy(lhs) && is_truthy(rhs)) as i64)),
        Or => Ok(Value::Integer((is_truthy(lhs) || is_truthy(rhs)) as i64)),
        Like => {
            if matches!(lhs, Value::Null) || matches!(rhs, Value::Null) {
                return Ok(Value::Null);
            }
            let haystack = value_to_string(lhs);
            let pattern = value_to_string(rhs);
            Ok(Value::Integer(sql_like_match(&haystack, &pattern) as i64))
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

/// SQL LIKE pattern matching (case-insensitive for ASCII, per SQLite default).
///
/// `%` matches any sequence of zero or more characters.
/// `_` matches any single character.
pub fn sql_like_match(haystack: &str, pattern: &str) -> bool {
    let h: Vec<char> = haystack.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    like_dp(&h, &p)
}

fn like_dp(h: &[char], p: &[char]) -> bool {
    let (hn, pn) = (h.len(), p.len());
    // dp[j] = true means p[0..j] matches h[0..i] for the current i
    let mut dp = vec![false; pn + 1];
    dp[0] = true;
    // Leading '%' chars can match empty string
    for j in 0..pn {
        if p[j] == '%' {
            dp[j + 1] = dp[j];
        } else {
            break;
        }
    }
    for i in 0..hn {
        let mut new_dp = vec![false; pn + 1];
        for j in 0..pn {
            if p[j] == '%' {
                // '%' matches zero chars (new_dp[j]) or one more char (dp[j+1])
                new_dp[j + 1] = new_dp[j] || dp[j + 1];
            } else if p[j] == '_' {
                new_dp[j + 1] = dp[j];
            } else {
                new_dp[j + 1] = dp[j] && p[j].eq_ignore_ascii_case(&h[i]);
            }
        }
        dp = new_dp;
    }
    dp[pn]
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
        _ => Err(ExecutorError::new(
            "cannot compare values of different types",
        )),
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

// ─── Encoding / Decoding Helpers ──────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct IndexBucket {
    pub value: Value,
    pub rowids: Vec<i64>,
}

pub const TAG_NULL: u8 = 0;
pub const TAG_INTEGER: u8 = 1;
pub const TAG_REAL: u8 = 2;
pub const TAG_TEXT: u8 = 3;

pub fn decode_row(payload: &[u8]) -> ExecResult<Vec<Value>> {
    if payload.len() < 4 {
        return Err(ExecutorError::new("row payload too small"));
    }
    let mut offset = 0usize;
    let col_count = read_u32(payload, &mut offset)? as usize;
    let mut row = Vec::with_capacity(col_count);

    for _ in 0..col_count {
        row.push(decode_value(payload, &mut offset)?);
    }

    Ok(row)
}

pub fn decode_index_payload(payload: &[u8]) -> ExecResult<Vec<IndexBucket>> {
    if payload.len() < 4 {
        return Err(ExecutorError::new("index payload too small"));
    }

    let mut offset = 0usize;
    let bucket_count = read_u32(payload, &mut offset)? as usize;
    let mut buckets = Vec::with_capacity(bucket_count);
    for _ in 0..bucket_count {
        let value = decode_value(payload, &mut offset)?;
        let row_count = read_u32(payload, &mut offset)? as usize;
        let mut rowids = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            rowids.push(read_i64(payload, &mut offset)?);
        }
        buckets.push(IndexBucket { value, rowids });
    }

    Ok(buckets)
}

pub fn index_key_for_value(value: &Value) -> ExecResult<i64> {
    if let Some(ordered) = ordered_index_key_for_value(value) {
        return Ok(ordered);
    }

    let mut encoded = Vec::new();
    encode_value(value, &mut encoded)?;
    let hash = fnv1a64(&encoded);
    Ok(i64::from_be_bytes(hash.to_be_bytes()))
}

/// Returns an order-preserving B+tree key for values that support true range
/// seeks. Non-orderable values return `None` and should use hash-based index
/// probing.
pub fn ordered_index_key_for_value(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(i) => Some(ordered_numeric_key(*i as f64)),
        Value::Real(f) if !f.is_nan() => Some(ordered_numeric_key(*f)),
        Value::Text(text) => Some(ordered_text_key(text)),
        _ => None,
    }
}

pub fn encode_value(value: &Value, out: &mut Vec<u8>) -> ExecResult<()> {
    match value {
        Value::Null => out.push(TAG_NULL),
        Value::Integer(i) => {
            out.push(TAG_INTEGER);
            out.extend_from_slice(&i.to_be_bytes());
        }
        Value::Real(f) => {
            out.push(TAG_REAL);
            out.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        Value::Text(s) => {
            let len: u32 = s
                .len()
                .try_into()
                .map_err(|_| ExecutorError::new("string value too large"))?;
            out.push(TAG_TEXT);
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(s.as_bytes());
        }
    }
    Ok(())
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    let mut hash = OFFSET_BASIS;
    for b in bytes {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn ordered_numeric_key(value: f64) -> i64 {
    let bits = value.to_bits();
    let sortable_u64 = if bits & (1u64 << 63) != 0 {
        !bits
    } else {
        bits ^ (1u64 << 63)
    };
    sortable_u64_to_i64(sortable_u64)
}

fn ordered_text_key(value: &str) -> i64 {
    // Keep the first 7 bytes exact, and encode byte 8 with a one-bit overlap
    // channel sourced from byte 9. This preserves non-decreasing ordering and
    // adds limited post-8th-byte discrimination for long shared prefixes.
    let bytes = value.as_bytes();
    let mut prefix = [0u8; 7];
    let copy_len = bytes.len().min(prefix.len());
    prefix[..copy_len].copy_from_slice(&bytes[..copy_len]);

    let b8 = bytes.get(7).copied().unwrap_or(0);
    let b9 = bytes.get(8).copied().unwrap_or(0);
    let tail_bit = u8::from(b9 >= 0x70);

    let low_byte = if b8 == 0 {
        0
    } else {
        b8.saturating_sub(1).saturating_add(tail_bit)
    };

    let mut sortable_bytes = [0u8; 8];
    sortable_bytes[..7].copy_from_slice(&prefix);
    sortable_bytes[7] = low_byte;
    let sortable_u64 = u64::from_be_bytes(sortable_bytes);

    sortable_u64_to_i64(sortable_u64)
}

fn sortable_u64_to_i64(sortable_u64: u64) -> i64 {
    let sortable_i64 = sortable_u64 ^ (1u64 << 63);
    i64::from_be_bytes(sortable_i64.to_be_bytes())
}

pub fn decode_value(buf: &[u8], offset: &mut usize) -> ExecResult<Value> {
    let tag = *buf
        .get(*offset)
        .ok_or_else(|| ExecutorError::new("payload truncated while reading value tag"))?;
    *offset += 1;
    match tag {
        TAG_NULL => Ok(Value::Null),
        TAG_INTEGER => Ok(Value::Integer(read_i64(buf, offset)?)),
        TAG_REAL => Ok(Value::Real(f64::from_bits(read_u64(buf, offset)?))),
        TAG_TEXT => {
            let len = read_u32(buf, offset)? as usize;
            let end = *offset + len;
            if end > buf.len() {
                return Err(ExecutorError::new("payload text out of bounds"));
            }
            let s = std::str::from_utf8(&buf[*offset..end])
                .map_err(|e| ExecutorError::new(format!("invalid utf-8 text in payload: {e}")))?;
            *offset = end;
            Ok(Value::Text(s.to_string()))
        }
        other => Err(ExecutorError::new(format!(
            "unknown value tag in payload: {other}"
        ))),
    }
}

fn read_u32(buf: &[u8], offset: &mut usize) -> ExecResult<u32> {
    let end = *offset + 4;
    if end > buf.len() {
        return Err(ExecutorError::new("payload truncated while reading u32"));
    }
    let value = u32::from_be_bytes(buf[*offset..end].try_into().unwrap());
    *offset = end;
    Ok(value)
}

fn read_u64(buf: &[u8], offset: &mut usize) -> ExecResult<u64> {
    let end = *offset + 8;
    if end > buf.len() {
        return Err(ExecutorError::new("payload truncated while reading u64"));
    }
    let value = u64::from_be_bytes(buf[*offset..end].try_into().unwrap());
    *offset = end;
    Ok(value)
}

fn read_i64(buf: &[u8], offset: &mut usize) -> ExecResult<i64> {
    let end = *offset + 8;
    if end > buf.len() {
        return Err(ExecutorError::new("payload truncated while reading i64"));
    }
    let value = i64::from_be_bytes(buf[*offset..end].try_into().unwrap());
    *offset = end;
    Ok(value)
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

    fn call(name: &str, args: Vec<Expr>) -> Expr {
        Expr::FunctionCall {
            name: name.to_string(),
            args,
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
            bin(
                Expr::IntegerLiteral(7),
                BinaryOperator::Subtract,
                Expr::IntegerLiteral(2),
            ),
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

    #[test]
    fn eval_expr_supports_scalar_functions() {
        assert_eq!(
            eval_expr(
                &call("LENGTH", vec![Expr::StringLiteral("hello".to_string())]),
                None
            )
            .unwrap(),
            int(5)
        );
        assert_eq!(
            eval_expr(
                &call("UPPER", vec![Expr::StringLiteral("MiXeD".to_string())]),
                None
            )
            .unwrap(),
            Value::Text("MIXED".to_string())
        );
        assert_eq!(
            eval_expr(
                &call(
                    "COALESCE",
                    vec![Expr::Null, Expr::IntegerLiteral(7), Expr::IntegerLiteral(8)]
                ),
                None
            )
            .unwrap(),
            int(7)
        );
        assert_eq!(
            eval_expr(
                &call(
                    "SUBSTR",
                    vec![
                        Expr::StringLiteral("alphabet".to_string()),
                        Expr::IntegerLiteral(3),
                        Expr::IntegerLiteral(3)
                    ]
                ),
                None
            )
            .unwrap(),
            Value::Text("pha".to_string())
        );
        assert_eq!(
            eval_expr(
                &call(
                    "TRIM",
                    vec![
                        Expr::StringLiteral("..hello..".to_string()),
                        Expr::StringLiteral(".".to_string())
                    ]
                ),
                None
            )
            .unwrap(),
            Value::Text("hello".to_string())
        );
    }

    #[test]
    fn eval_expr_scalar_function_errors_for_unsupported_name() {
        let err = eval_expr(&call("DOES_NOT_EXIST", vec![]), None).unwrap_err();
        assert_eq!(
            err.to_string(),
            "function 'DOES_NOT_EXIST' is not supported yet"
        );
    }

    #[test]
    fn ordered_index_key_is_monotonic_for_numeric_values() {
        let k1 = ordered_index_key_for_value(&Value::Integer(-10)).unwrap();
        let k2 = ordered_index_key_for_value(&Value::Real(0.5)).unwrap();
        let k3 = ordered_index_key_for_value(&Value::Integer(42)).unwrap();
        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn ordered_index_key_is_monotonic_for_text_values() {
        let ka = ordered_index_key_for_value(&Value::Text("apple".to_string())).unwrap();
        let kb = ordered_index_key_for_value(&Value::Text("banana".to_string())).unwrap();
        let kc = ordered_index_key_for_value(&Value::Text("banana~".to_string())).unwrap();
        assert!(ka < kb);
        assert!(kb < kc);
    }

    #[test]
    fn ordered_index_key_distinguishes_some_long_text_suffixes_beyond_eight_bytes() {
        let k1 = ordered_index_key_for_value(&Value::Text("abcdefgh1".to_string())).unwrap();
        let k2 = ordered_index_key_for_value(&Value::Text("abcdefghz".to_string())).unwrap();
        assert!(k1 < k2);
    }

    #[test]
    fn ordered_index_key_is_non_decreasing_for_sorted_text_series() {
        let values = vec![
            "",
            "a",
            "ab",
            "abc",
            "abcdefgh",
            "abcdefgh0",
            "abcdefgh2",
            "abcdefghz",
            "abcdefgi0",
            "b",
        ];

        let mut prev = None;
        for text in values {
            let key = ordered_index_key_for_value(&Value::Text(text.to_string())).unwrap();
            if let Some(prev_key) = prev {
                assert!(prev_key <= key, "expected non-decreasing keys");
            }
            prev = Some(key);
        }
    }

    // ── LIKE pattern matching tests ──────────────────────────────────

    #[test]
    fn like_exact_match() {
        assert!(sql_like_match("hello", "hello"));
        assert!(!sql_like_match("hello", "world"));
    }

    #[test]
    fn like_case_insensitive() {
        assert!(sql_like_match("Hello", "hello"));
        assert!(sql_like_match("hello", "HELLO"));
        assert!(sql_like_match("HeLLo", "hEllO"));
    }

    #[test]
    fn like_percent_wildcard() {
        assert!(sql_like_match("hello world", "hello%"));
        assert!(sql_like_match("hello world", "%world"));
        assert!(sql_like_match("hello world", "%lo wo%"));
        assert!(sql_like_match("hello world", "%"));
        assert!(sql_like_match("", "%"));
        assert!(!sql_like_match("hello", "hello%world"));
    }

    #[test]
    fn like_underscore_wildcard() {
        assert!(sql_like_match("a", "_"));
        assert!(sql_like_match("abc", "a_c"));
        assert!(!sql_like_match("abc", "a_"));
        assert!(sql_like_match("abc", "___"));
        assert!(!sql_like_match("ab", "___"));
    }

    #[test]
    fn like_combined_wildcards() {
        assert!(sql_like_match("apple", "a___e"));
        assert!(sql_like_match("abcde", "a%e"));
        assert!(sql_like_match("ae", "a%e"));
        assert!(sql_like_match("apple pie", "a%_e"));
        assert!(sql_like_match("apple", "%pp%"));
    }

    #[test]
    fn like_empty_patterns() {
        assert!(sql_like_match("", ""));
        assert!(!sql_like_match("a", ""));
        assert!(!sql_like_match("", "_"));
        assert!(sql_like_match("", "%"));
    }

    #[test]
    fn like_null_operands_return_null() {
        let null = Value::Null;
        let text = Value::Text("abc".to_string());
        let pat = Value::Text("%".to_string());
        assert_eq!(
            eval_binary_op(&null, BinaryOperator::Like, &pat).unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_binary_op(&text, BinaryOperator::Like, &null).unwrap(),
            Value::Null
        );
    }
}
