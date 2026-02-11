/// Top-level integration crate for ralph-sqlite.
///
/// This crate provides a minimal embedded database API that parses SQL
/// statements and executes a small supported subset against pager + B+tree
/// storage.
use std::collections::{HashMap, HashSet};
use std::path::Path;

use ralph_parser::ast::{
    Assignment, BinaryOperator, CreateTableStmt, DeleteStmt, Expr, InsertStmt, SelectColumn,
    SelectStmt, Stmt, UnaryOperator, UpdateStmt,
};
use ralph_storage::pager::PageNum;
use ralph_storage::{BTree, Pager};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecuteResult {
    CreateTable,
    Insert { rows_affected: usize },
    Update { rows_affected: usize },
    Delete { rows_affected: usize },
    Select(QueryResult),
}

#[derive(Debug, Clone)]
struct TableMeta {
    name: String,
    columns: Vec<String>,
    root_page: PageNum,
}

pub struct Database {
    pager: Pager,
    tables: HashMap<String, TableMeta>,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let pager = Pager::open(path).map_err(|e| format!("open database: {e}"))?;
        Ok(Self {
            pager,
            tables: HashMap::new(),
        })
    }

    pub fn execute(&mut self, sql: &str) -> Result<ExecuteResult, String> {
        let stmt = ralph_parser::parse(sql).map_err(|e| format!("parse error: {e}"))?;
        match stmt {
            Stmt::CreateTable(create_stmt) => self.execute_create_table(create_stmt),
            Stmt::Insert(insert_stmt) => self.execute_insert(insert_stmt),
            Stmt::Update(update_stmt) => self.execute_update(update_stmt),
            Stmt::Delete(delete_stmt) => self.execute_delete(delete_stmt),
            Stmt::Select(select_stmt) => self.execute_select(select_stmt),
            other => Err(format!("statement not supported yet: {other:?}")),
        }
    }

    fn execute_create_table(&mut self, stmt: CreateTableStmt) -> Result<ExecuteResult, String> {
        let table_key = normalize_identifier(&stmt.table);
        if self.tables.contains_key(&table_key) {
            if stmt.if_not_exists {
                return Ok(ExecuteResult::CreateTable);
            }
            return Err(format!("table '{}' already exists", stmt.table));
        }

        if stmt.columns.is_empty() {
            return Err("CREATE TABLE requires at least one column".to_string());
        }

        let columns: Vec<String> = stmt.columns.into_iter().map(|c| c.name).collect();
        let root_page = BTree::create(&mut self.pager).map_err(|e| format!("create table: {e}"))?;
        self.tables.insert(
            table_key,
            TableMeta {
                name: stmt.table,
                columns,
                root_page,
            },
        );
        self.pager
            .flush_all()
            .map_err(|e| format!("flush create table: {e}"))?;
        Ok(ExecuteResult::CreateTable)
    }

    fn execute_insert(&mut self, stmt: InsertStmt) -> Result<ExecuteResult, String> {
        let table_key = normalize_identifier(&stmt.table);
        let meta = self
            .tables
            .get(&table_key)
            .cloned()
            .ok_or_else(|| format!("no such table '{}'", stmt.table))?;

        let target_columns = resolve_insert_columns(&meta, stmt.columns.as_ref())?;
        let mut encoded_rows = Vec::with_capacity(stmt.values.len());
        for expr_row in &stmt.values {
            if expr_row.len() != target_columns.len() {
                return Err(format!(
                    "INSERT row has {} values but expected {}",
                    expr_row.len(),
                    target_columns.len()
                ));
            }

            let mut row = vec![Value::Null; meta.columns.len()];
            for (expr, col_idx) in expr_row.iter().zip(target_columns.iter().copied()) {
                row[col_idx] = eval_expr(expr, None)?;
            }
            encoded_rows.push(encode_row(&row)?);
        }

        let rows_affected = encoded_rows.len();
        let mut tree = BTree::new(&mut self.pager, meta.root_page);
        let existing = tree.scan_all().map_err(|e| format!("scan table: {e}"))?;
        let mut next_rowid = existing.last().map(|e| e.key + 1).unwrap_or(1);

        for row in encoded_rows {
            tree.insert(next_rowid, &row)
                .map_err(|e| format!("insert row: {e}"))?;
            next_rowid += 1;
        }

        self.pager
            .flush_all()
            .map_err(|e| format!("flush insert: {e}"))?;

        Ok(ExecuteResult::Insert { rows_affected })
    }

    fn execute_update(&mut self, stmt: UpdateStmt) -> Result<ExecuteResult, String> {
        let table_key = normalize_identifier(&stmt.table);
        let meta = self
            .tables
            .get(&table_key)
            .cloned()
            .ok_or_else(|| format!("no such table '{}'", stmt.table))?;
        let assignments = resolve_update_assignments(&meta, &stmt.assignments)?;

        let mut tree = BTree::new(&mut self.pager, meta.root_page);
        let entries = tree.scan_all().map_err(|e| format!("scan table: {e}"))?;
        let mut rows_affected = 0usize;

        for entry in entries {
            let original_row = decode_table_row(&meta, &entry.payload)?;
            if !where_clause_matches(&meta, &original_row, stmt.where_clause.as_ref())? {
                continue;
            }

            // UPDATE assignments are evaluated against the original row.
            let mut evaluated_assignments = Vec::with_capacity(assignments.len());
            for (col_idx, expr) in &assignments {
                let value = eval_expr(expr, Some((&meta, &original_row)))?;
                evaluated_assignments.push((*col_idx, value));
            }

            let mut updated_row = original_row;
            for (col_idx, value) in evaluated_assignments {
                updated_row[col_idx] = value;
            }

            let encoded = encode_row(&updated_row)?;
            tree.insert(entry.key, &encoded)
                .map_err(|e| format!("update row: {e}"))?;
            rows_affected += 1;
        }

        self.pager
            .flush_all()
            .map_err(|e| format!("flush update: {e}"))?;

        Ok(ExecuteResult::Update { rows_affected })
    }

    fn execute_delete(&mut self, stmt: DeleteStmt) -> Result<ExecuteResult, String> {
        let table_key = normalize_identifier(&stmt.table);
        let meta = self
            .tables
            .get(&table_key)
            .cloned()
            .ok_or_else(|| format!("no such table '{}'", stmt.table))?;

        let mut tree = BTree::new(&mut self.pager, meta.root_page);
        let entries = tree.scan_all().map_err(|e| format!("scan table: {e}"))?;
        let mut rows_affected = 0usize;

        for entry in entries {
            let row = decode_table_row(&meta, &entry.payload)?;
            if !where_clause_matches(&meta, &row, stmt.where_clause.as_ref())? {
                continue;
            }

            let deleted = tree
                .delete(entry.key)
                .map_err(|e| format!("delete row: {e}"))?;
            if deleted {
                rows_affected += 1;
            }
        }

        self.pager
            .flush_all()
            .map_err(|e| format!("flush delete: {e}"))?;

        Ok(ExecuteResult::Delete { rows_affected })
    }

    fn execute_select(&mut self, stmt: SelectStmt) -> Result<ExecuteResult, String> {
        if !stmt.order_by.is_empty() {
            return Err("ORDER BY is not supported yet".to_string());
        }

        let mut rows = if let Some(from) = &stmt.from {
            let table_key = normalize_identifier(&from.table);
            let meta = self
                .tables
                .get(&table_key)
                .cloned()
                .ok_or_else(|| format!("no such table '{}'", from.table))?;
            let mut tree = BTree::new(&mut self.pager, meta.root_page);
            let entries = tree.scan_all().map_err(|e| format!("scan table: {e}"))?;

            let mut projected_rows = Vec::new();
            for entry in entries {
                let decoded = decode_table_row(&meta, &entry.payload)?;
                if !where_clause_matches(&meta, &decoded, stmt.where_clause.as_ref())? {
                    continue;
                }

                projected_rows.push(project_row(&stmt.columns, &meta, &decoded)?);
            }
            projected_rows
        } else {
            if stmt
                .columns
                .iter()
                .any(|col| matches!(col, SelectColumn::AllColumns))
            {
                return Err("SELECT * without FROM is not supported".to_string());
            }

            if let Some(where_expr) = &stmt.where_clause {
                let predicate = eval_expr(where_expr, None)?;
                if !is_truthy(&predicate) {
                    Vec::new()
                } else {
                    vec![project_row_no_from(&stmt.columns)?]
                }
            } else {
                vec![project_row_no_from(&stmt.columns)?]
            }
        };

        let offset = eval_optional_usize_expr(stmt.offset.as_ref())?;
        if offset > 0 {
            if offset >= rows.len() {
                rows.clear();
            } else {
                rows = rows.into_iter().skip(offset).collect();
            }
        }

        if let Some(limit) = eval_optional_limit_expr(stmt.limit.as_ref())? {
            rows.truncate(limit);
        }

        let columns = if let Some(from) = &stmt.from {
            let table_key = normalize_identifier(&from.table);
            let meta = self
                .tables
                .get(&table_key)
                .ok_or_else(|| format!("no such table '{}'", from.table))?;
            select_output_columns(&stmt.columns, Some(meta))
        } else {
            select_output_columns(&stmt.columns, None)
        }?;

        Ok(ExecuteResult::Select(QueryResult { columns, rows }))
    }
}

fn resolve_insert_columns(
    meta: &TableMeta,
    columns: Option<&Vec<String>>,
) -> Result<Vec<usize>, String> {
    let mut result = Vec::new();
    if let Some(cols) = columns {
        let mut seen = HashSet::new();
        for col in cols {
            let idx = find_column_index(meta, col)
                .ok_or_else(|| format!("unknown column '{}' in table '{}'", col, meta.name))?;
            if !seen.insert(idx) {
                return Err(format!("duplicate column '{}' in INSERT", col));
            }
            result.push(idx);
        }
    } else {
        result.extend(0..meta.columns.len());
    }
    Ok(result)
}

fn resolve_update_assignments(
    meta: &TableMeta,
    assignments: &[Assignment],
) -> Result<Vec<(usize, Expr)>, String> {
    let mut resolved = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let col_idx = find_column_index(meta, &assignment.column).ok_or_else(|| {
            format!(
                "unknown column '{}' in table '{}'",
                assignment.column, meta.name
            )
        })?;
        resolved.push((col_idx, assignment.value.clone()));
    }
    Ok(resolved)
}

fn find_column_index(meta: &TableMeta, column: &str) -> Option<usize> {
    meta.columns
        .iter()
        .position(|c| c.eq_ignore_ascii_case(column))
}

fn decode_table_row(meta: &TableMeta, payload: &[u8]) -> Result<Vec<Value>, String> {
    let row = decode_row(payload)?;
    if row.len() != meta.columns.len() {
        return Err(format!(
            "row column count {} does not match table schema {}",
            row.len(),
            meta.columns.len()
        ));
    }
    Ok(row)
}

fn where_clause_matches(
    meta: &TableMeta,
    row: &[Value],
    where_clause: Option<&Expr>,
) -> Result<bool, String> {
    if let Some(where_expr) = where_clause {
        let predicate = eval_expr(where_expr, Some((meta, row)))?;
        Ok(is_truthy(&predicate))
    } else {
        Ok(true)
    }
}

fn project_row(
    columns: &[SelectColumn],
    meta: &TableMeta,
    row: &[Value],
) -> Result<Vec<Value>, String> {
    let mut projected = Vec::new();
    for column in columns {
        match column {
            SelectColumn::AllColumns => projected.extend_from_slice(row),
            SelectColumn::Expr { expr, .. } => projected.push(eval_expr(expr, Some((meta, row)))?),
        }
    }
    Ok(projected)
}

fn project_row_no_from(columns: &[SelectColumn]) -> Result<Vec<Value>, String> {
    let mut projected = Vec::new();
    for column in columns {
        match column {
            SelectColumn::AllColumns => {
                return Err("SELECT * without FROM is not supported".to_string());
            }
            SelectColumn::Expr { expr, .. } => projected.push(eval_expr(expr, None)?),
        }
    }
    Ok(projected)
}

fn select_output_columns(
    columns: &[SelectColumn],
    meta: Option<&TableMeta>,
) -> Result<Vec<String>, String> {
    let mut names = Vec::new();
    for (idx, col) in columns.iter().enumerate() {
        match col {
            SelectColumn::AllColumns => {
                let meta = meta.ok_or_else(|| "SELECT * requires FROM".to_string())?;
                names.extend(meta.columns.iter().cloned());
            }
            SelectColumn::Expr { expr, alias } => {
                if let Some(alias) = alias {
                    names.push(alias.clone());
                } else {
                    names.push(default_expr_name(expr, idx));
                }
            }
        }
    }
    Ok(names)
}

fn default_expr_name(expr: &Expr, idx: usize) -> String {
    match expr {
        Expr::ColumnRef { column, .. } => column.clone(),
        _ => format!("expr{}", idx + 1),
    }
}

fn eval_optional_limit_expr(expr: Option<&Expr>) -> Result<Option<usize>, String> {
    expr.map(eval_usize_expr).transpose()
}

fn eval_optional_usize_expr(expr: Option<&Expr>) -> Result<usize, String> {
    Ok(match expr {
        Some(e) => eval_usize_expr(e)?,
        None => 0,
    })
}

fn eval_usize_expr(expr: &Expr) -> Result<usize, String> {
    let value = eval_expr(expr, None)?;
    match value {
        Value::Integer(i) if i >= 0 => Ok(i as usize),
        Value::Integer(_) => Err("LIMIT/OFFSET cannot be negative".to_string()),
        _ => Err("LIMIT/OFFSET must evaluate to an integer".to_string()),
    }
}

fn eval_expr(expr: &Expr, row_ctx: Option<(&TableMeta, &[Value])>) -> Result<Value, String> {
    match expr {
        Expr::IntegerLiteral(i) => Ok(Value::Integer(*i)),
        Expr::FloatLiteral(f) => Ok(Value::Real(*f)),
        Expr::StringLiteral(s) => Ok(Value::Text(s.clone())),
        Expr::Null => Ok(Value::Null),
        Expr::Paren(inner) => eval_expr(inner, row_ctx),
        Expr::ColumnRef { table, column } => {
            let (meta, row) =
                row_ctx.ok_or_else(|| "column reference requires a table row".to_string())?;
            if let Some(table_name) = table {
                if !meta.name.eq_ignore_ascii_case(table_name) {
                    return Err(format!(
                        "unknown table qualifier '{}' for table '{}'",
                        table_name, meta.name
                    ));
                }
            }
            if column == "*" {
                return Err("'*' cannot be used as a scalar expression".to_string());
            }
            let col_idx = find_column_index(meta, column)
                .ok_or_else(|| format!("unknown column '{}' in table '{}'", column, meta.name))?;
            Ok(row[col_idx].clone())
        }
        Expr::UnaryOp { op, expr } => {
            let v = eval_expr(expr, row_ctx)?;
            match op {
                UnaryOperator::Negate => match v {
                    Value::Integer(i) => Ok(Value::Integer(-i)),
                    Value::Real(f) => Ok(Value::Real(-f)),
                    Value::Null => Ok(Value::Null),
                    _ => Err("cannot negate non-numeric value".to_string()),
                },
                UnaryOperator::Not => Ok(Value::Integer((!is_truthy(&v)) as i64)),
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let lhs = eval_expr(left, row_ctx)?;
            let rhs = eval_expr(right, row_ctx)?;
            eval_binary_op(&lhs, *op, &rhs)
        }
        Expr::IsNull { expr, negated } => {
            let v = eval_expr(expr, row_ctx)?;
            let is_null = matches!(v, Value::Null);
            let result = if *negated { !is_null } else { is_null };
            Ok(Value::Integer(result as i64))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let v = eval_expr(expr, row_ctx)?;
            let low_v = eval_expr(low, row_ctx)?;
            let high_v = eval_expr(high, row_ctx)?;
            let ge_low = compare_values(&v, &low_v).map(|ord| ord >= std::cmp::Ordering::Equal)?;
            let le_high =
                compare_values(&v, &high_v).map(|ord| ord <= std::cmp::Ordering::Equal)?;
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
        Expr::FunctionCall { name, .. } => Err(format!("function '{name}' is not supported yet")),
    }
}

fn eval_binary_op(lhs: &Value, op: BinaryOperator, rhs: &Value) -> Result<Value, String> {
    use BinaryOperator::*;

    match op {
        Add | Subtract | Multiply | Divide | Modulo => eval_numeric_binary(lhs, op, rhs),
        Eq => Ok(Value::Integer(values_equal(lhs, rhs) as i64)),
        NotEq => Ok(Value::Integer((!values_equal(lhs, rhs)) as i64)),
        Lt => {
            compare_values(lhs, rhs).map(|o| Value::Integer((o == std::cmp::Ordering::Less) as i64))
        }
        LtEq => compare_values(lhs, rhs).map(|o| {
            Value::Integer((o == std::cmp::Ordering::Less || o == std::cmp::Ordering::Equal) as i64)
        }),
        Gt => compare_values(lhs, rhs)
            .map(|o| Value::Integer((o == std::cmp::Ordering::Greater) as i64)),
        GtEq => compare_values(lhs, rhs).map(|o| {
            Value::Integer(
                (o == std::cmp::Ordering::Greater || o == std::cmp::Ordering::Equal) as i64,
            )
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

fn eval_numeric_binary(lhs: &Value, op: BinaryOperator, rhs: &Value) -> Result<Value, String> {
    let (l, r, as_integer) = numeric_operands(lhs, rhs)?;
    let out = match op {
        BinaryOperator::Add => l + r,
        BinaryOperator::Subtract => l - r,
        BinaryOperator::Multiply => l * r,
        BinaryOperator::Divide => {
            if r == 0.0 {
                return Err("division by zero".to_string());
            }
            l / r
        }
        BinaryOperator::Modulo => {
            if r == 0.0 {
                return Err("modulo by zero".to_string());
            }
            l % r
        }
        _ => unreachable!("non-arithmetic operator passed to eval_numeric_binary"),
    };
    if as_integer {
        Ok(Value::Integer(out as i64))
    } else {
        Ok(Value::Real(out))
    }
}

fn numeric_operands(lhs: &Value, rhs: &Value) -> Result<(f64, f64, bool), String> {
    let l = value_to_f64(lhs)?;
    let r = value_to_f64(rhs)?;
    let both_int = matches!(lhs, Value::Integer(_)) && matches!(rhs, Value::Integer(_));
    Ok((l, r, both_int))
}

fn value_to_f64(v: &Value) -> Result<f64, String> {
    match v {
        Value::Integer(i) => Ok(*i as f64),
        Value::Real(f) => Ok(*f),
        Value::Null => Ok(0.0),
        Value::Text(_) => Err("expected numeric value".to_string()),
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
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

fn compare_values(lhs: &Value, rhs: &Value) -> Result<std::cmp::Ordering, String> {
    match (lhs, rhs) {
        (Value::Integer(a), Value::Integer(b)) => Ok(a.cmp(b)),
        (Value::Real(a), Value::Real(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| "cannot compare NaN values".to_string()),
        (Value::Integer(a), Value::Real(b)) => (*a as f64)
            .partial_cmp(b)
            .ok_or_else(|| "cannot compare NaN values".to_string()),
        (Value::Real(a), Value::Integer(b)) => a
            .partial_cmp(&(*b as f64))
            .ok_or_else(|| "cannot compare NaN values".to_string()),
        (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),
        (Value::Null, Value::Null) => Ok(std::cmp::Ordering::Equal),
        _ => Err("cannot compare values of different types".to_string()),
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

fn normalize_identifier(ident: &str) -> String {
    ident.to_ascii_lowercase()
}

const TAG_NULL: u8 = 0;
const TAG_INTEGER: u8 = 1;
const TAG_REAL: u8 = 2;
const TAG_TEXT: u8 = 3;

fn encode_row(row: &[Value]) -> Result<Vec<u8>, String> {
    let col_count: u32 = row
        .len()
        .try_into()
        .map_err(|_| "row has too many columns".to_string())?;

    let mut out = Vec::new();
    out.extend_from_slice(&col_count.to_be_bytes());
    for value in row {
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
                    .map_err(|_| "string value too large".to_string())?;
                out.push(TAG_TEXT);
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(s.as_bytes());
            }
        }
    }
    Ok(out)
}

fn decode_row(payload: &[u8]) -> Result<Vec<Value>, String> {
    if payload.len() < 4 {
        return Err("row payload too small".to_string());
    }
    let mut offset = 0usize;
    let col_count = read_u32(payload, &mut offset)? as usize;
    let mut row = Vec::with_capacity(col_count);

    for _ in 0..col_count {
        let tag = *payload
            .get(offset)
            .ok_or_else(|| "row payload truncated".to_string())?;
        offset += 1;

        match tag {
            TAG_NULL => row.push(Value::Null),
            TAG_INTEGER => {
                let i = read_i64(payload, &mut offset)?;
                row.push(Value::Integer(i));
            }
            TAG_REAL => {
                let bits = read_u64(payload, &mut offset)?;
                row.push(Value::Real(f64::from_bits(bits)));
            }
            TAG_TEXT => {
                let len = read_u32(payload, &mut offset)? as usize;
                let end = offset + len;
                if end > payload.len() {
                    return Err("row payload text out of bounds".to_string());
                }
                let s = std::str::from_utf8(&payload[offset..end])
                    .map_err(|e| format!("invalid utf-8 text in row payload: {e}"))?;
                row.push(Value::Text(s.to_string()));
                offset = end;
            }
            other => return Err(format!("unknown value tag in row payload: {other}")),
        }
    }

    Ok(row)
}

fn read_u32(buf: &[u8], offset: &mut usize) -> Result<u32, String> {
    let end = *offset + 4;
    if end > buf.len() {
        return Err("payload truncated while reading u32".to_string());
    }
    let value = u32::from_be_bytes(buf[*offset..end].try_into().unwrap());
    *offset = end;
    Ok(value)
}

fn read_u64(buf: &[u8], offset: &mut usize) -> Result<u64, String> {
    let end = *offset + 8;
    if end > buf.len() {
        return Err("payload truncated while reading u64".to_string());
    }
    let value = u64::from_be_bytes(buf[*offset..end].try_into().unwrap());
    *offset = end;
    Ok(value)
}

fn read_i64(buf: &[u8], offset: &mut usize) -> Result<i64, String> {
    let end = *offset + 8;
    if end > buf.len() {
        return Err("payload truncated while reading i64".to_string());
    }
    let value = i64::from_be_bytes(buf[*offset..end].try_into().unwrap());
    *offset = end;
    Ok(value)
}

pub fn version() -> &'static str {
    "0.1.0-bootstrap"
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    fn temp_db_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("ralph_sqlite_tests");
        fs::create_dir_all(&dir).ok();
        let pid = std::process::id();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        dir.join(format!("{name}_{pid}_{ts}.db"))
    }

    fn cleanup(path: &Path) {
        fs::remove_file(path).ok();
    }

    #[test]
    fn version_string() {
        assert_eq!(version(), "0.1.0-bootstrap");
    }

    #[test]
    fn create_insert_select_roundtrip() {
        let path = temp_db_path("roundtrip");
        let mut db = Database::open(&path).unwrap();

        let create = db
            .execute("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        assert_eq!(create, ExecuteResult::CreateTable);

        let insert = db
            .execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob');")
            .unwrap();
        assert_eq!(insert, ExecuteResult::Insert { rows_affected: 2 });

        let result = db.execute("SELECT id, name FROM users;").unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(q.columns, vec!["id".to_string(), "name".to_string()]);
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(1), Value::Text("alice".to_string())],
                        vec![Value::Integer(2), Value::Text("bob".to_string())],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn insert_with_column_list_fills_missing_with_null() {
        let path = temp_db_path("column_list");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (a INTEGER, b TEXT, c INTEGER);")
            .unwrap();
        db.execute("INSERT INTO t (b, a) VALUES ('x', 10);")
            .unwrap();

        let result = db.execute("SELECT * FROM t;").unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![
                        Value::Integer(10),
                        Value::Text("x".to_string()),
                        Value::Null
                    ]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_literal_without_from() {
        let path = temp_db_path("literal_select");
        let mut db = Database::open(&path).unwrap();

        let result = db.execute("SELECT 1 + 2, 'ok';").unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![Value::Integer(3), Value::Text("ok".to_string())]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn update_with_where_updates_matching_rows() {
        let path = temp_db_path("update_with_where");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, name TEXT, score INTEGER);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'cara', 30);")
            .unwrap();

        let result = db
            .execute("UPDATE users SET score = score + 5, name = 'updated' WHERE id >= 2;")
            .unwrap();
        assert_eq!(result, ExecuteResult::Update { rows_affected: 2 });

        let selected = db.execute("SELECT id, name, score FROM users;").unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![
                            Value::Integer(1),
                            Value::Text("alice".to_string()),
                            Value::Integer(10)
                        ],
                        vec![
                            Value::Integer(2),
                            Value::Text("updated".to_string()),
                            Value::Integer(25)
                        ],
                        vec![
                            Value::Integer(3),
                            Value::Text("updated".to_string()),
                            Value::Integer(35)
                        ],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn delete_with_where_removes_matching_rows() {
        let path = temp_db_path("delete_with_where");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2), (3), (4);")
            .unwrap();

        let deleted = db.execute("DELETE FROM t WHERE id >= 3;").unwrap();
        assert_eq!(deleted, ExecuteResult::Delete { rows_affected: 2 });

        let remaining = db.execute("SELECT id FROM t;").unwrap();
        match remaining {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn update_and_delete_without_where_affect_all_rows() {
        let path = temp_db_path("update_delete_all_rows");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (v INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2), (3);").unwrap();

        let updated = db.execute("UPDATE t SET v = v * 2;").unwrap();
        assert_eq!(updated, ExecuteResult::Update { rows_affected: 3 });

        let deleted = db.execute("DELETE FROM t;").unwrap();
        assert_eq!(deleted, ExecuteResult::Delete { rows_affected: 3 });

        let remaining = db.execute("SELECT * FROM t;").unwrap();
        match remaining {
            ExecuteResult::Select(q) => assert!(q.rows.is_empty()),
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }
}
