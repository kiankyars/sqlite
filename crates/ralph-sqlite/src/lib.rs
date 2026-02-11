/// Top-level integration crate for ralph-sqlite.
///
/// This crate provides a minimal embedded database API that parses SQL
/// statements and executes a small supported subset against pager + B+tree
/// storage.
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use ralph_parser::ast::{
    Assignment, BinaryOperator, CreateIndexStmt, CreateTableStmt, DeleteStmt, Expr, InsertStmt,
    OrderByItem, SelectColumn, SelectStmt, Stmt, UnaryOperator, UpdateStmt,
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
    Begin,
    Commit,
    Rollback,
    CreateTable,
    CreateIndex,
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

#[derive(Debug, Clone)]
struct IndexMeta {
    table_key: String,
    table_name: String,
    column: String,
    column_idx: usize,
    root_page: PageNum,
}

#[derive(Debug, Clone)]
struct IndexBucket {
    value: Value,
    rowids: Vec<i64>,
}

#[derive(Debug, Clone)]
struct TransactionSnapshot {
    tables: HashMap<String, TableMeta>,
    indexes: HashMap<String, IndexMeta>,
}

pub struct Database {
    db_path: PathBuf,
    pager: Pager,
    tables: HashMap<String, TableMeta>,
    indexes: HashMap<String, IndexMeta>,
    in_explicit_txn: bool,
    tx_snapshot: Option<TransactionSnapshot>,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let db_path = path.as_ref().to_path_buf();
        let pager = Pager::open(&db_path).map_err(|e| format!("open database: {e}"))?;
        Ok(Self {
            db_path,
            pager,
            tables: HashMap::new(),
            indexes: HashMap::new(),
            in_explicit_txn: false,
            tx_snapshot: None,
        })
    }

    pub fn execute(&mut self, sql: &str) -> Result<ExecuteResult, String> {
        let stmt = ralph_parser::parse(sql).map_err(|e| format!("parse error: {e}"))?;
        match stmt {
            Stmt::Begin => self.execute_begin(),
            Stmt::Commit => self.execute_commit(),
            Stmt::Rollback => self.execute_rollback(),
            Stmt::CreateTable(create_stmt) => self.execute_create_table(create_stmt),
            Stmt::CreateIndex(create_stmt) => self.execute_create_index(create_stmt),
            Stmt::Insert(insert_stmt) => self.execute_insert(insert_stmt),
            Stmt::Update(update_stmt) => self.execute_update(update_stmt),
            Stmt::Delete(delete_stmt) => self.execute_delete(delete_stmt),
            Stmt::Select(select_stmt) => self.execute_select(select_stmt),
            other => Err(format!("statement not supported yet: {other:?}")),
        }
    }

    fn execute_begin(&mut self) -> Result<ExecuteResult, String> {
        if self.in_explicit_txn {
            return Err("cannot BEGIN: transaction already active".to_string());
        }
        self.tx_snapshot = Some(TransactionSnapshot {
            tables: self.tables.clone(),
            indexes: self.indexes.clone(),
        });
        self.in_explicit_txn = true;
        Ok(ExecuteResult::Begin)
    }

    fn execute_commit(&mut self) -> Result<ExecuteResult, String> {
        if !self.in_explicit_txn {
            return Err("cannot COMMIT: no active transaction".to_string());
        }
        self.pager
            .commit()
            .map_err(|e| format!("commit transaction: {e}"))?;
        self.in_explicit_txn = false;
        self.tx_snapshot = None;
        Ok(ExecuteResult::Commit)
    }

    fn execute_rollback(&mut self) -> Result<ExecuteResult, String> {
        if !self.in_explicit_txn {
            return Err("cannot ROLLBACK: no active transaction".to_string());
        }

        let snapshot = self
            .tx_snapshot
            .as_ref()
            .cloned()
            .ok_or_else(|| "cannot ROLLBACK: transaction snapshot missing".to_string())?;
        let reopened = Pager::open(&self.db_path)
            .map_err(|e| format!("rollback transaction: reopen pager: {e}"))?;

        self.pager = reopened;
        self.tables = snapshot.tables;
        self.indexes = snapshot.indexes;
        self.in_explicit_txn = false;
        self.tx_snapshot = None;
        Ok(ExecuteResult::Rollback)
    }

    fn commit_if_autocommit(&mut self, context: &str) -> Result<(), String> {
        if self.in_explicit_txn {
            return Ok(());
        }
        self.pager
            .commit()
            .map_err(|e| format!("commit {context}: {e}"))
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
        self.commit_if_autocommit("create table")?;
        Ok(ExecuteResult::CreateTable)
    }

    fn execute_create_index(&mut self, stmt: CreateIndexStmt) -> Result<ExecuteResult, String> {
        if stmt.unique {
            return Err("UNIQUE indexes are not supported yet".to_string());
        }
        if stmt.columns.len() != 1 {
            return Err("only single-column indexes are supported yet".to_string());
        }

        let index_key = normalize_identifier(&stmt.index);
        if self.indexes.contains_key(&index_key) {
            if stmt.if_not_exists {
                return Ok(ExecuteResult::CreateIndex);
            }
            return Err(format!("index '{}' already exists", stmt.index));
        }

        let table_key = normalize_identifier(&stmt.table);
        let table_meta = self
            .tables
            .get(&table_key)
            .cloned()
            .ok_or_else(|| format!("no such table '{}'", stmt.table))?;

        let column = stmt.columns[0].clone();
        let column_idx = find_column_index(&table_meta, &column)
            .ok_or_else(|| format!("unknown column '{}' in table '{}'", column, table_meta.name))?;

        let root_page = BTree::create(&mut self.pager).map_err(|e| format!("create index: {e}"))?;
        let mut table_tree = BTree::new(&mut self.pager, table_meta.root_page);
        let table_entries = table_tree
            .scan_all()
            .map_err(|e| format!("scan table for index build: {e}"))?;
        drop(table_tree);

        let index_meta = IndexMeta {
            table_key: table_key.clone(),
            table_name: table_meta.name.clone(),
            column,
            column_idx,
            root_page,
        };
        for entry in table_entries {
            let row = decode_row(&entry.payload)?;
            if row.len() != table_meta.columns.len() {
                return Err(format!(
                    "row column count {} does not match table schema {}",
                    row.len(),
                    table_meta.columns.len()
                ));
            }
            self.index_insert_row(&index_meta, entry.key, &row)?;
        }

        self.indexes.insert(index_key, index_meta);
        self.commit_if_autocommit("create index")?;
        Ok(ExecuteResult::CreateIndex)
    }

    fn execute_insert(&mut self, stmt: InsertStmt) -> Result<ExecuteResult, String> {
        let table_key = normalize_identifier(&stmt.table);
        let meta = self
            .tables
            .get(&table_key)
            .cloned()
            .ok_or_else(|| format!("no such table '{}'", stmt.table))?;

        let target_columns = resolve_insert_columns(&meta, stmt.columns.as_ref())?;
        let mut evaluated_rows = Vec::with_capacity(stmt.values.len());
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
            evaluated_rows.push(row);
        }

        let rows_affected = evaluated_rows.len();
        let mut table_tree = BTree::new(&mut self.pager, meta.root_page);
        let existing = table_tree
            .scan_all()
            .map_err(|e| format!("scan table: {e}"))?;
        let mut next_rowid = existing.last().map(|e| e.key + 1).unwrap_or(1);
        let mut inserted_rows = Vec::with_capacity(evaluated_rows.len());

        for row in evaluated_rows {
            let encoded = encode_row(&row)?;
            table_tree
                .insert(next_rowid, &encoded)
                .map_err(|e| format!("insert row: {e}"))?;
            inserted_rows.push((next_rowid, row));
            next_rowid += 1;
        }
        drop(table_tree);

        let table_indexes = self.indexes_for_table(&table_key);
        for (rowid, row) in inserted_rows {
            for index_meta in &table_indexes {
                self.index_insert_row(index_meta, rowid, &row)?;
            }
        }

        self.commit_if_autocommit("insert")?;

        Ok(ExecuteResult::Insert { rows_affected })
    }

    fn indexes_for_table(&self, table_key: &str) -> Vec<IndexMeta> {
        self.indexes
            .values()
            .filter(|idx| idx.table_key == table_key)
            .cloned()
            .collect()
    }

    fn index_insert_row(
        &mut self,
        index_meta: &IndexMeta,
        rowid: i64,
        row: &[Value],
    ) -> Result<(), String> {
        let value = row.get(index_meta.column_idx).ok_or_else(|| {
            format!(
                "row missing indexed column '{}' for index on '{}'",
                index_meta.column, index_meta.table_name
            )
        })?;

        let key = index_key_for_value(value)?;
        let mut tree = BTree::new(&mut self.pager, index_meta.root_page);
        let mut buckets = match tree
            .lookup(key)
            .map_err(|e| format!("lookup index entry: {e}"))?
        {
            Some(payload) => decode_index_payload(&payload)?,
            None => Vec::new(),
        };

        if let Some(existing) = buckets.iter_mut().find(|b| values_equal(&b.value, value)) {
            if !existing.rowids.contains(&rowid) {
                existing.rowids.push(rowid);
            }
        } else {
            buckets.push(IndexBucket {
                value: value.clone(),
                rowids: vec![rowid],
            });
        }

        let encoded = encode_index_payload(&buckets)?;
        tree.insert(key, &encoded)
            .map_err(|e| format!("insert index entry: {e}"))?;
        Ok(())
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

        self.commit_if_autocommit("update")?;

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

        self.commit_if_autocommit("delete")?;

        Ok(ExecuteResult::Delete { rows_affected })
    }

    fn execute_select(&mut self, stmt: SelectStmt) -> Result<ExecuteResult, String> {
        let table_meta = if let Some(from) = &stmt.from {
            let table_key = normalize_identifier(&from.table);
            Some(
                self.tables
                    .get(&table_key)
                    .cloned()
                    .ok_or_else(|| format!("no such table '{}'", from.table))?,
            )
        } else {
            None
        };

        let mut rows_with_order_keys = if let Some(meta) = table_meta.as_ref() {
            let mut tree = BTree::new(&mut self.pager, meta.root_page);
            let entries = tree.scan_all().map_err(|e| format!("scan table: {e}"))?;
            let mut rows = Vec::new();
            for entry in entries {
                let decoded = decode_table_row(meta, &entry.payload)?;
                if !where_clause_matches(meta, &decoded, stmt.where_clause.as_ref())? {
                    continue;
                }
                let projected = project_row(&stmt.columns, meta, &decoded)?;
                let order_keys = evaluate_order_by_keys(&stmt.order_by, Some((meta, &decoded)))?;
                rows.push((projected, order_keys));
            }
            rows
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
                    vec![(
                        project_row_no_from(&stmt.columns)?,
                        evaluate_order_by_keys(&stmt.order_by, None)?,
                    )]
                }
            } else {
                vec![(
                    project_row_no_from(&stmt.columns)?,
                    evaluate_order_by_keys(&stmt.order_by, None)?,
                )]
            }
        };

        if !stmt.order_by.is_empty() {
            rows_with_order_keys.sort_by(|(_, left_keys), (_, right_keys)| {
                compare_order_keys(left_keys, right_keys, &stmt.order_by)
            });
        }

        let mut rows: Vec<Vec<Value>> = rows_with_order_keys
            .into_iter()
            .map(|(row, _)| row)
            .collect();

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

        let columns = select_output_columns(&stmt.columns, table_meta.as_ref())?;

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

fn evaluate_order_by_keys(
    order_by: &[OrderByItem],
    row_ctx: Option<(&TableMeta, &[Value])>,
) -> Result<Vec<Value>, String> {
    let mut out = Vec::with_capacity(order_by.len());
    for item in order_by {
        out.push(eval_expr(&item.expr, row_ctx)?);
    }
    Ok(out)
}

fn compare_order_keys(
    left_keys: &[Value],
    right_keys: &[Value],
    order_by: &[OrderByItem],
) -> std::cmp::Ordering {
    debug_assert_eq!(left_keys.len(), order_by.len());
    debug_assert_eq!(right_keys.len(), order_by.len());

    for ((left, right), item) in left_keys.iter().zip(right_keys.iter()).zip(order_by.iter()) {
        let mut ord = compare_sort_values(left, right);
        if item.descending {
            ord = ord.reverse();
        }
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_sort_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        _ if is_numeric(left) && is_numeric(right) => value_to_f64(left)
            .and_then(|lv| {
                value_to_f64(right)
                    .map(|rv| lv.partial_cmp(&rv).unwrap_or(std::cmp::Ordering::Equal))
            })
            .unwrap_or(std::cmp::Ordering::Equal),
        _ => sort_type_rank(left).cmp(&sort_type_rank(right)),
    }
}

fn sort_type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Integer(_) | Value::Real(_) => 1,
        Value::Text(_) => 2,
    }
}

fn is_numeric(v: &Value) -> bool {
    matches!(v, Value::Integer(_) | Value::Real(_))
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
        encode_value(value, &mut out)?;
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
        row.push(decode_value(payload, &mut offset)?);
    }

    Ok(row)
}

fn encode_index_payload(buckets: &[IndexBucket]) -> Result<Vec<u8>, String> {
    let bucket_count: u32 = buckets
        .len()
        .try_into()
        .map_err(|_| "too many index buckets".to_string())?;
    let mut out = Vec::new();
    out.extend_from_slice(&bucket_count.to_be_bytes());

    for bucket in buckets {
        encode_value(&bucket.value, &mut out)?;
        let row_count: u32 = bucket
            .rowids
            .len()
            .try_into()
            .map_err(|_| "too many rowids in index bucket".to_string())?;
        out.extend_from_slice(&row_count.to_be_bytes());
        for rowid in &bucket.rowids {
            out.extend_from_slice(&rowid.to_be_bytes());
        }
    }

    Ok(out)
}

fn decode_index_payload(payload: &[u8]) -> Result<Vec<IndexBucket>, String> {
    if payload.len() < 4 {
        return Err("index payload too small".to_string());
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

fn index_key_for_value(value: &Value) -> Result<i64, String> {
    let mut encoded = Vec::new();
    encode_value(value, &mut encoded)?;
    let hash = fnv1a64(&encoded);
    Ok(i64::from_be_bytes(hash.to_be_bytes()))
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

fn encode_value(value: &Value, out: &mut Vec<u8>) -> Result<(), String> {
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
    Ok(())
}

fn decode_value(buf: &[u8], offset: &mut usize) -> Result<Value, String> {
    let tag = *buf
        .get(*offset)
        .ok_or_else(|| "payload truncated while reading value tag".to_string())?;
    *offset += 1;
    match tag {
        TAG_NULL => Ok(Value::Null),
        TAG_INTEGER => Ok(Value::Integer(read_i64(buf, offset)?)),
        TAG_REAL => Ok(Value::Real(f64::from_bits(read_u64(buf, offset)?))),
        TAG_TEXT => {
            let len = read_u32(buf, offset)? as usize;
            let end = *offset + len;
            if end > buf.len() {
                return Err("payload text out of bounds".to_string());
            }
            let s = std::str::from_utf8(&buf[*offset..end])
                .map_err(|e| format!("invalid utf-8 text in payload: {e}"))?;
            *offset = end;
            Ok(Value::Text(s.to_string()))
        }
        other => Err(format!("unknown value tag in payload: {other}")),
    }
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
    use std::ffi::OsString;
    use std::fs;
    use std::path::{Path, PathBuf};

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
        fs::remove_file(wal_path(path)).ok();
    }

    fn wal_path(path: &Path) -> PathBuf {
        let mut wal_name: OsString = path.as_os_str().to_os_string();
        wal_name.push("-wal");
        PathBuf::from(wal_name)
    }

    fn indexed_rowids(db: &mut Database, index_name: &str, value: &Value) -> Vec<i64> {
        let idx_key = normalize_identifier(index_name);
        let index_meta = db.indexes.get(&idx_key).unwrap().clone();
        let key = index_key_for_value(value).unwrap();
        let mut index_tree = BTree::new(&mut db.pager, index_meta.root_page);
        let payload = index_tree.lookup(key).unwrap().unwrap();
        let buckets = decode_index_payload(&payload).unwrap();
        buckets
            .into_iter()
            .find(|bucket| values_equal(&bucket.value, value))
            .map(|bucket| bucket.rowids)
            .unwrap_or_default()
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
    fn select_order_by_non_projected_column_desc() {
        let path = temp_db_path("order_by_non_projected");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (2, 'bob'), (1, 'alice'), (3, 'cara');")
            .unwrap();

        let result = db
            .execute("SELECT name FROM users ORDER BY id DESC;")
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Text("cara".to_string())],
                        vec![Value::Text("bob".to_string())],
                        vec![Value::Text("alice".to_string())],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_order_by_expression_with_limit_and_offset() {
        let path = temp_db_path("order_by_expr_limit_offset");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (v INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (3), (2), (5);")
            .unwrap();

        let result = db
            .execute("SELECT v FROM t ORDER BY v * 2 DESC LIMIT 2 OFFSET 1;")
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![Value::Integer(3)], vec![Value::Integer(2)]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_order_by_nulls_and_secondary_key() {
        let path = temp_db_path("order_by_nulls");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER, score INTEGER);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, NULL), (2, 10), (3, NULL), (4, 5);")
            .unwrap();

        let asc = db
            .execute("SELECT id, score FROM t ORDER BY score ASC, id DESC;")
            .unwrap();
        match asc {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(3), Value::Null],
                        vec![Value::Integer(1), Value::Null],
                        vec![Value::Integer(4), Value::Integer(5)],
                        vec![Value::Integer(2), Value::Integer(10)],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        let desc = db
            .execute("SELECT id, score FROM t ORDER BY score DESC, id ASC;")
            .unwrap();
        match desc {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(2), Value::Integer(10)],
                        vec![Value::Integer(4), Value::Integer(5)],
                        vec![Value::Integer(1), Value::Null],
                        vec![Value::Integer(3), Value::Null],
                    ]
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
    fn create_index_backfills_existing_rows() {
        let path = temp_db_path("index_backfill");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER, score INTEGER);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 7), (2, 9), (3, 7);")
            .unwrap();

        let result = db.execute("CREATE INDEX idx_t_score ON t(score);").unwrap();
        assert_eq!(result, ExecuteResult::CreateIndex);

        assert_eq!(
            indexed_rowids(&mut db, "idx_t_score", &Value::Integer(7)),
            vec![1, 3]
        );
        assert_eq!(
            indexed_rowids(&mut db, "idx_t_score", &Value::Integer(9)),
            vec![2]
        );

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
    fn insert_updates_secondary_index() {
        let path = temp_db_path("index_insert_maintenance");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, age INTEGER);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_age ON users(age);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 30), (2, 30), (3, 42);")
            .unwrap();

        assert_eq!(
            indexed_rowids(&mut db, "idx_users_age", &Value::Integer(30)),
            vec![1, 2]
        );
        assert_eq!(
            indexed_rowids(&mut db, "idx_users_age", &Value::Integer(42)),
            vec![3]
        );

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

    #[test]
    fn explicit_transaction_delays_wal_until_commit() {
        let path = temp_db_path("txn_delay_wal");
        let mut db = Database::open(&path).unwrap();

        let wal_len_before = fs::metadata(wal_path(&path)).unwrap().len();

        assert_eq!(db.execute("BEGIN;").unwrap(), ExecuteResult::Begin);
        assert_eq!(
            db.execute("CREATE TABLE t (id INTEGER);").unwrap(),
            ExecuteResult::CreateTable
        );
        assert_eq!(
            db.execute("INSERT INTO t VALUES (1);").unwrap(),
            ExecuteResult::Insert { rows_affected: 1 }
        );
        let wal_len_during_txn = fs::metadata(wal_path(&path)).unwrap().len();
        assert_eq!(wal_len_during_txn, wal_len_before);

        assert_eq!(db.execute("COMMIT;").unwrap(), ExecuteResult::Commit);
        let wal_len_after_commit = fs::metadata(wal_path(&path)).unwrap().len();
        assert!(wal_len_after_commit > wal_len_before);

        let rows = db.execute("SELECT id FROM t;").unwrap();
        match rows {
            ExecuteResult::Select(q) => assert_eq!(q.rows, vec![vec![Value::Integer(1)]]),
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn rollback_discards_uncommitted_transaction_changes() {
        let path = temp_db_path("txn_rollback");
        let mut db = Database::open(&path).unwrap();

        assert_eq!(
            db.execute("BEGIN TRANSACTION;").unwrap(),
            ExecuteResult::Begin
        );
        db.execute("CREATE TABLE t (id INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2);").unwrap();
        assert_eq!(
            db.execute("ROLLBACK TRANSACTION;").unwrap(),
            ExecuteResult::Rollback
        );

        let err = db.execute("SELECT * FROM t;").unwrap_err();
        assert!(err.contains("no such table"));

        cleanup(&path);
    }

    #[test]
    fn transaction_state_errors_are_reported() {
        let path = temp_db_path("txn_state_errors");
        let mut db = Database::open(&path).unwrap();

        let commit_err = db.execute("COMMIT;").unwrap_err();
        assert!(commit_err.contains("no active transaction"));

        let rollback_err = db.execute("ROLLBACK;").unwrap_err();
        assert!(rollback_err.contains("no active transaction"));

        assert_eq!(db.execute("BEGIN;").unwrap(), ExecuteResult::Begin);
        let nested_begin_err = db.execute("BEGIN;").unwrap_err();
        assert!(nested_begin_err.contains("already active"));

        cleanup(&path);
    }
}
