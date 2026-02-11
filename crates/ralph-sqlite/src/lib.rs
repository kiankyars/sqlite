/// Top-level integration crate for ralph-sqlite.
///
/// This crate provides a minimal embedded database API that parses SQL
/// statements and executes a small supported subset against pager + B+tree
/// storage.
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use ralph_executor::{
    self, decode_index_payload, decode_row, encode_value, index_key_for_value,
    ordered_index_key_for_value, Filter, IndexBucket, IndexEqScan, Operator, TableScan, Value,
};
use ralph_parser::ast::{
    Assignment, BinaryOperator, CreateIndexStmt, CreateTableStmt, DeleteStmt, DropIndexStmt,
    DropTableStmt, Expr, InsertStmt, OrderByItem, SelectColumn, SelectStmt, Stmt, TypeName,
    UnaryOperator, UpdateStmt,
};
use ralph_planner::{plan_select, plan_where, AccessPath, IndexInfo};
use ralph_storage::pager::PageNum;
use ralph_storage::{BTree, Pager, Schema};

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
    DropTable,
    DropIndex,
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
    columns: Vec<String>,
    column_indices: Vec<usize>,
    root_page: PageNum,
    unique: bool,
}

#[derive(Debug, Clone)]
struct TransactionSnapshot {
    tables: HashMap<String, TableMeta>,
    indexes: HashMap<String, IndexMeta>,
}

#[derive(Debug, Clone)]
struct GroupState {
    key: Vec<Value>,
    rows: Vec<Vec<Value>>,
    scalar_row_count: usize,
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
        let mut pager = Pager::open(&db_path).map_err(|e| format!("open database: {e}"))?;
        if pager.header().schema_root == 0 {
            Schema::initialize(&mut pager).map_err(|e| format!("initialize schema: {e}"))?;
        }
        let (tables, indexes) = load_catalogs(&mut pager)?;

        Ok(Self {
            db_path,
            pager,
            tables,
            indexes,
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
            Stmt::CreateTable(create_stmt) => self.execute_create_table(create_stmt, sql),
            Stmt::CreateIndex(create_stmt) => self.execute_create_index(create_stmt, sql),
            Stmt::DropTable(drop_stmt) => self.execute_drop_table(drop_stmt),
            Stmt::DropIndex(drop_stmt) => self.execute_drop_index(drop_stmt),
            Stmt::Insert(insert_stmt) => self.execute_insert(insert_stmt),
            Stmt::Update(update_stmt) => self.execute_update(update_stmt),
            Stmt::Delete(delete_stmt) => self.execute_delete(delete_stmt),
            Stmt::Select(select_stmt) => self.execute_select(select_stmt),
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

    fn execute_create_table(
        &mut self,
        stmt: CreateTableStmt,
        original_sql: &str,
    ) -> Result<ExecuteResult, String> {
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

        let columns: Vec<String> = stmt.columns.iter().map(|c| c.name.clone()).collect();
        let schema_columns: Vec<(String, String)> = stmt
            .columns
            .iter()
            .map(|column| {
                (
                    column.name.clone(),
                    type_name_to_sql(column.type_name.as_ref()),
                )
            })
            .collect();

        let root_page = Schema::create_table(
            &mut self.pager,
            &stmt.table,
            &schema_columns,
            original_sql.trim(),
        )
        .map_err(|e| format!("create table: {e}"))?;
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

    fn execute_create_index(
        &mut self,
        stmt: CreateIndexStmt,
        original_sql: &str,
    ) -> Result<ExecuteResult, String> {
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

        let mut seen_column_indexes = HashSet::new();
        let mut indexed_columns = Vec::with_capacity(stmt.columns.len());
        for column in &stmt.columns {
            let column_idx = find_column_index(&table_meta, column).ok_or_else(|| {
                format!("unknown column '{}' in table '{}'", column, table_meta.name)
            })?;
            if !seen_column_indexes.insert(column_idx) {
                return Err(format!("duplicate column '{}' in index definition", column));
            }
            indexed_columns.push((column.clone(), column_idx));
        }
        let schema_columns: Vec<(String, u32)> = indexed_columns
            .iter()
            .map(|(name, idx)| (name.clone(), *idx as u32))
            .collect();

        let root_page = Schema::create_index(
            &mut self.pager,
            &stmt.index,
            &table_meta.name,
            &schema_columns,
            original_sql.trim(),
        )
        .map_err(|e| format!("create index: {e}"))?;
        let mut table_tree = BTree::new(&mut self.pager, table_meta.root_page);
        let table_entries = table_tree
            .scan_all()
            .map_err(|e| format!("scan table for index build: {e}"))?;
        drop(table_tree);

        let mut decoded_rows = Vec::with_capacity(table_entries.len());
        for entry in table_entries {
            let row = decode_row(&entry.payload).map_err(|e| e.to_string())?;
            if row.len() != table_meta.columns.len() {
                return Err(format!(
                    "row column count {} does not match table schema {}",
                    row.len(),
                    table_meta.columns.len()
                ));
            }
            decoded_rows.push((entry.key, row));
        }

        let index_columns: Vec<String> = indexed_columns
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        let index_column_indices: Vec<usize> =
            indexed_columns.iter().map(|(_, idx)| *idx).collect();

        if stmt.unique {
            validate_unique_index_backfill_rows(
                &table_meta.name,
                &index_columns,
                &index_column_indices,
                decoded_rows.iter().map(|(_, row)| row),
            )?;
        }

        let index_meta = IndexMeta {
            table_key: table_key.clone(),
            table_name: table_meta.name.clone(),
            columns: index_columns,
            column_indices: index_column_indices,
            root_page,
            unique: stmt.unique,
        };
        for (rowid, row) in decoded_rows {
            self.index_insert_row(&index_meta, rowid, &row)?;
        }

        self.indexes.insert(index_key, index_meta);
        self.commit_if_autocommit("create index")?;
        Ok(ExecuteResult::CreateIndex)
    }

    fn execute_drop_table(&mut self, stmt: DropTableStmt) -> Result<ExecuteResult, String> {
        let table_key = normalize_identifier(&stmt.table);
        let table_meta = match self.tables.get(&table_key).cloned() {
            Some(meta) => meta,
            None => {
                if stmt.if_exists {
                    return Ok(ExecuteResult::DropTable);
                }
                return Err(format!("no such table '{}'", stmt.table));
            }
        };

        let table_indexes: Vec<(String, IndexMeta)> = self
            .indexes
            .iter()
            .filter(|(_, idx)| idx.table_key == table_key)
            .map(|(name, idx)| (name.clone(), idx.clone()))
            .collect();

        for (index_key, _) in &table_indexes {
            let dropped = Schema::drop_index(&mut self.pager, index_key)
                .map_err(|e| format!("drop index schema entry '{}': {e}", index_key))?;
            let dropped = dropped.ok_or_else(|| {
                format!(
                    "drop table '{}': index schema entry '{}' not found",
                    table_meta.name, index_key
                )
            })?;
            BTree::reclaim_tree(&mut self.pager, dropped.root_page)
                .map_err(|e| format!("drop index '{}': reclaim pages: {e}", dropped.name))?;
            self.indexes.remove(index_key);
        }

        let dropped_table = Schema::drop_table(&mut self.pager, &table_meta.name)
            .map_err(|e| format!("drop table schema entry '{}': {e}", table_meta.name))?;
        let dropped_table = dropped_table.ok_or_else(|| {
            format!(
                "drop table '{}': table schema entry not found",
                table_meta.name
            )
        })?;
        BTree::reclaim_tree(&mut self.pager, dropped_table.root_page)
            .map_err(|e| format!("drop table '{}': reclaim pages: {e}", dropped_table.name))?;

        self.tables.remove(&table_key);
        self.commit_if_autocommit("drop table")?;
        Ok(ExecuteResult::DropTable)
    }

    fn execute_drop_index(&mut self, stmt: DropIndexStmt) -> Result<ExecuteResult, String> {
        let index_key = normalize_identifier(&stmt.index);
        let index_meta = match self.indexes.get(&index_key).cloned() {
            Some(meta) => meta,
            None => {
                if stmt.if_exists {
                    return Ok(ExecuteResult::DropIndex);
                }
                return Err(format!("no such index '{}'", stmt.index));
            }
        };

        let dropped_index = Schema::drop_index(&mut self.pager, &stmt.index)
            .map_err(|e| format!("drop index schema entry '{}': {e}", stmt.index))?;
        let dropped_index = dropped_index
            .ok_or_else(|| format!("drop index '{}': index schema entry not found", stmt.index))?;
        BTree::reclaim_tree(&mut self.pager, dropped_index.root_page)
            .map_err(|e| format!("drop index '{}': reclaim pages: {e}", dropped_index.name))?;

        self.indexes.remove(&index_key);
        debug_assert_eq!(
            index_meta.root_page, dropped_index.root_page,
            "in-memory and schema root pages diverged for dropped index '{}'",
            stmt.index
        );
        self.commit_if_autocommit("drop index")?;
        Ok(ExecuteResult::DropIndex)
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

        let table_indexes = self.indexes_for_table(&table_key);
        self.validate_unique_constraints_for_insert_rows(&table_indexes, &evaluated_rows)?;

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
        let indexed_values = index_values_from_row(index_meta, row)?;
        let (key, bucket_value) = index_key_and_bucket_value(&indexed_values)?;
        let mut tree = BTree::new(&mut self.pager, index_meta.root_page);
        let mut buckets = match tree
            .lookup(key)
            .map_err(|e| format!("lookup index entry: {e}"))?
        {
            Some(payload) => decode_index_payload(&payload).map_err(|e| e.to_string())?,
            None => Vec::new(),
        };

        if let Some(existing) = buckets
            .iter_mut()
            .find(|b| values_equal(&b.value, &bucket_value))
        {
            if index_meta.unique
                && !index_value_contains_null(&indexed_values)
                && existing
                    .rowids
                    .iter()
                    .any(|existing_rowid| *existing_rowid != rowid)
            {
                return Err(unique_constraint_error(
                    &index_meta.table_name,
                    &index_meta.columns,
                ));
            }
            if !existing.rowids.contains(&rowid) {
                existing.rowids.push(rowid);
            }
        } else {
            buckets.push(IndexBucket {
                value: bucket_value,
                rowids: vec![rowid],
            });
        }

        let encoded = encode_index_payload(&buckets)?;
        tree.insert(key, &encoded)
            .map_err(|e| format!("insert index entry: {e}"))?;
        Ok(())
    }

    fn index_delete_row(
        &mut self,
        index_meta: &IndexMeta,
        rowid: i64,
        row: &[Value],
    ) -> Result<(), String> {
        let indexed_values = index_values_from_row(index_meta, row)?;
        let (key, bucket_value) = index_key_and_bucket_value(&indexed_values)?;
        let mut tree = BTree::new(&mut self.pager, index_meta.root_page);
        let Some(payload) = tree
            .lookup(key)
            .map_err(|e| format!("lookup index entry: {e}"))?
        else {
            return Ok(());
        };

        let mut buckets = decode_index_payload(&payload).map_err(|e| e.to_string())?;
        if let Some(bucket) = buckets
            .iter_mut()
            .find(|bucket| values_equal(&bucket.value, &bucket_value))
        {
            bucket.rowids.retain(|candidate| *candidate != rowid);
        }
        buckets.retain(|bucket| !bucket.rowids.is_empty());

        if buckets.is_empty() {
            tree.delete(key)
                .map_err(|e| format!("delete index entry: {e}"))?;
        } else {
            let encoded = encode_index_payload(&buckets)?;
            tree.insert(key, &encoded)
                .map_err(|e| format!("update index entry: {e}"))?;
        }
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
        let table_indexes = self.indexes_for_table(&table_key);

        let planner_indexes = self.planner_indexes_for_table(&table_key);
        let access_path = plan_where(stmt.where_clause.as_ref(), &meta.name, &planner_indexes);
        let entries = self.read_candidate_entries(&meta, &access_path)?;
        let mut planned_updates = Vec::new();

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

            let mut updated_row = original_row.clone();
            for (col_idx, value) in evaluated_assignments {
                updated_row[col_idx] = value;
            }

            planned_updates.push((entry.key, original_row, updated_row));
        }

        self.validate_unique_constraints_for_updates(&table_indexes, &planned_updates)?;
        let rows_affected = planned_updates.len();

        for (rowid, original_row, updated_row) in planned_updates {
            for index_meta in &table_indexes {
                self.index_delete_row(index_meta, rowid, &original_row)?;
            }

            let encoded = encode_row(&updated_row)?;
            {
                let mut tree = BTree::new(&mut self.pager, meta.root_page);
                tree.insert(rowid, &encoded)
                    .map_err(|e| format!("update row: {e}"))?;
            }

            for index_meta in &table_indexes {
                self.index_insert_row(index_meta, rowid, &updated_row)?;
            }
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
        let table_indexes = self.indexes_for_table(&table_key);

        let planner_indexes = self.planner_indexes_for_table(&table_key);
        let access_path = plan_where(stmt.where_clause.as_ref(), &meta.name, &planner_indexes);
        let entries = self.read_candidate_entries(&meta, &access_path)?;
        let mut rows_affected = 0usize;

        for entry in entries {
            let row = decode_table_row(&meta, &entry.payload)?;
            if !where_clause_matches(&meta, &row, stmt.where_clause.as_ref())? {
                continue;
            }

            for index_meta in &table_indexes {
                self.index_delete_row(index_meta, entry.key, &row)?;
            }

            let deleted = {
                let mut tree = BTree::new(&mut self.pager, meta.root_page);
                tree.delete(entry.key)
                    .map_err(|e| format!("delete row: {e}"))?
            };
            if deleted {
                rows_affected += 1;
            }
        }

        self.commit_if_autocommit("delete")?;

        Ok(ExecuteResult::Delete { rows_affected })
    }

    fn execute_select(&mut self, stmt: SelectStmt) -> Result<ExecuteResult, String> {
        let aggregate_select = select_uses_aggregates(&stmt);
        let aggregate_having = stmt
            .having
            .as_ref()
            .map(expr_contains_aggregate)
            .unwrap_or(false);

        if let Some(where_expr) = stmt.where_clause.as_ref() {
            if expr_contains_aggregate(where_expr) {
                return Err("aggregate functions are not allowed in WHERE".to_string());
            }
        }
        if stmt.group_by.iter().any(expr_contains_aggregate) {
            return Err("aggregate functions are not allowed in GROUP BY".to_string());
        }

        // Dispatch to join-specific path when FROM has joins
        if let Some(from) = &stmt.from {
            if !from.joins.is_empty() {
                return self.execute_select_join(stmt);
            }
        }

        let table_ctx = if let Some(from) = &stmt.from {
            let table_key = normalize_identifier(&from.table);
            let table_meta = self
                .tables
                .get(&table_key)
                .cloned()
                .ok_or_else(|| format!("no such table '{}'", from.table))?;
            Some((table_key, table_meta))
        } else {
            None
        };
        let table_meta = table_ctx.as_ref().map(|(_, meta)| meta);
        let access_path = if let Some((table_key, meta)) = table_ctx.as_ref() {
            let planner_indexes = self.planner_indexes_for_table(table_key);
            plan_select(&stmt, &meta.name, &planner_indexes).access_path
        } else {
            AccessPath::TableScan
        };

        let mut rows_with_order_keys = if !stmt.group_by.is_empty() {
            let groups = if let Some(meta) = table_meta {
                let filtered_rows =
                    self.read_rows_for_select(meta, stmt.where_clause.as_ref(), &access_path)?;
                let mut groups = Vec::new();
                for row in filtered_rows {
                    let key = evaluate_group_by_key(&stmt.group_by, Some((meta, row.as_slice())))?;
                    if let Some(existing) = groups
                        .iter_mut()
                        .find(|candidate: &&mut GroupState| group_keys_equal(&candidate.key, &key))
                    {
                        existing.rows.push(row);
                    } else {
                        groups.push(GroupState {
                            key,
                            rows: vec![row],
                            scalar_row_count: 0,
                        });
                    }
                }
                groups
            } else {
                if stmt
                    .columns
                    .iter()
                    .any(|col| matches!(col, SelectColumn::AllColumns))
                {
                    return Err("SELECT * without FROM is not supported".to_string());
                }

                let scalar_row_count = if let Some(where_expr) = &stmt.where_clause {
                    let predicate = eval_expr(where_expr, None)?;
                    if !is_truthy(&predicate) {
                        0
                    } else {
                        1
                    }
                } else {
                    1
                };

                if scalar_row_count == 0 {
                    Vec::new()
                } else {
                    vec![GroupState {
                        key: evaluate_group_by_key(&stmt.group_by, None)?,
                        rows: Vec::new(),
                        scalar_row_count,
                    }]
                }
            };

            let mut rows = Vec::with_capacity(groups.len());
            for group in &groups {
                let representative_row = group.rows.first().map(|row| row.as_slice());
                if let Some(having_expr) = stmt.having.as_ref() {
                    let predicate = eval_grouped_expr(
                        having_expr,
                        table_meta,
                        &group.rows,
                        group.scalar_row_count,
                        representative_row,
                    )?;
                    if !is_truthy(&predicate) {
                        continue;
                    }
                }

                let projected = project_grouped_row(
                    &stmt.columns,
                    table_meta,
                    &group.rows,
                    group.scalar_row_count,
                    representative_row,
                )?;
                let order_keys = evaluate_grouped_order_by_keys(
                    &stmt.order_by,
                    table_meta,
                    &group.rows,
                    group.scalar_row_count,
                    representative_row,
                )?;
                rows.push((projected, order_keys));
            }
            rows
        } else if let Some(meta) = table_meta {
            let filtered_rows =
                self.read_rows_for_select(meta, stmt.where_clause.as_ref(), &access_path)?;
            let aggregate_query = aggregate_select || aggregate_having;

            if stmt.having.is_some() && !aggregate_query {
                return Err("HAVING clause on a non-aggregate query".to_string());
            }

            if aggregate_query {
                let include_row = if let Some(having_expr) = stmt.having.as_ref() {
                    let predicate =
                        eval_aggregate_expr(having_expr, table_meta, &filtered_rows, 0)?;
                    is_truthy(&predicate)
                } else {
                    true
                };

                if include_row {
                    vec![(
                        project_aggregate_row(&stmt.columns, table_meta, &filtered_rows, 0)?,
                        evaluate_aggregate_order_by_keys(
                            &stmt.order_by,
                            table_meta,
                            &filtered_rows,
                            0,
                        )?,
                    )]
                } else {
                    Vec::new()
                }
            } else {
                let mut rows = Vec::with_capacity(filtered_rows.len());
                for decoded in &filtered_rows {
                    let projected = project_row(&stmt.columns, meta, decoded)?;
                    let order_keys = evaluate_order_by_keys(&stmt.order_by, Some((meta, decoded)))?;
                    rows.push((projected, order_keys));
                }
                rows
            }
        } else {
            if stmt
                .columns
                .iter()
                .any(|col| matches!(col, SelectColumn::AllColumns))
            {
                return Err("SELECT * without FROM is not supported".to_string());
            }

            let scalar_row_count = if let Some(where_expr) = &stmt.where_clause {
                let predicate = eval_expr(where_expr, None)?;
                if !is_truthy(&predicate) {
                    0
                } else {
                    1
                }
            } else {
                1
            };

            let aggregate_query = aggregate_select || aggregate_having;
            if stmt.having.is_some() && !aggregate_query {
                return Err("HAVING clause on a non-aggregate query".to_string());
            }

            if aggregate_query {
                let include_row = if let Some(having_expr) = stmt.having.as_ref() {
                    let predicate = eval_aggregate_expr(having_expr, None, &[], scalar_row_count)?;
                    is_truthy(&predicate)
                } else {
                    true
                };

                if include_row {
                    vec![(
                        project_aggregate_row(&stmt.columns, None, &[], scalar_row_count)?,
                        evaluate_aggregate_order_by_keys(
                            &stmt.order_by,
                            None,
                            &[],
                            scalar_row_count,
                        )?,
                    )]
                } else {
                    Vec::new()
                }
            } else if scalar_row_count == 0 {
                Vec::new()
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

        let columns = select_output_columns(&stmt.columns, table_meta)?;

        Ok(ExecuteResult::Select(QueryResult { columns, rows }))
    }

    /// Read candidate B+tree entries using the given access path.
    ///
    /// When an index-driven path is available, only rowids selected from the
    /// index are fetched from the table B+tree (instead of a full scan). The
    /// caller is still responsible for applying the full WHERE predicate on the
    /// returned entries because the index may over-select (hash collisions, AND
    /// predicates with additional terms, etc.).
    fn read_candidate_entries(
        &mut self,
        meta: &TableMeta,
        access_path: &AccessPath,
    ) -> Result<Vec<ralph_storage::btree::Entry>, String> {
        match access_path {
            AccessPath::TableScan => {
                let mut tree = BTree::new(&mut self.pager, meta.root_page);
                tree.scan_all().map_err(|e| format!("scan table: {e}"))
            }
            AccessPath::IndexEq {
                index_name,
                value_exprs,
                ..
            } => {
                let index_meta = self.resolve_index_meta(index_name)?;
                let mut values = Vec::with_capacity(value_exprs.len());
                for value_expr in value_exprs {
                    values.push(eval_expr(value_expr, None)?);
                }
                let rowids = self.index_eq_rowids(&index_meta, &values)?;
                self.lookup_table_entries_by_rowids(meta.root_page, rowids)
            }
            AccessPath::IndexRange {
                index_name,
                lower,
                upper,
                ..
            } => {
                let index_meta = self.resolve_index_meta(index_name)?;
                let lower_bound = match lower {
                    Some(bound) => Some((eval_expr(&bound.value_expr, None)?, bound.inclusive)),
                    None => None,
                };
                let upper_bound = match upper {
                    Some(bound) => Some((eval_expr(&bound.value_expr, None)?, bound.inclusive)),
                    None => None,
                };

                let rowids = self.index_range_rowids(
                    index_meta.root_page,
                    lower_bound
                        .as_ref()
                        .map(|(value, inclusive)| (value, *inclusive)),
                    upper_bound
                        .as_ref()
                        .map(|(value, inclusive)| (value, *inclusive)),
                )?;
                self.lookup_table_entries_by_rowids(meta.root_page, rowids)
            }
            AccessPath::IndexOr { branches } => {
                let mut entries = Vec::new();
                let mut seen_rowids = HashSet::new();
                for branch in branches {
                    let branch_entries = self.read_candidate_entries(meta, branch)?;
                    for entry in branch_entries {
                        if seen_rowids.insert(entry.key) {
                            entries.push(entry);
                        }
                    }
                }
                entries.sort_by_key(|entry| entry.key);
                Ok(entries)
            }
        }
    }

    fn resolve_index_meta(&self, index_name: &str) -> Result<IndexMeta, String> {
        let idx_key = normalize_identifier(index_name);
        self.indexes
            .get(&idx_key)
            .cloned()
            .ok_or_else(|| format!("index '{}' not found", index_name))
    }

    fn index_eq_rowids(
        &mut self,
        index_meta: &IndexMeta,
        values: &[Value],
    ) -> Result<Vec<i64>, String> {
        if values.len() != index_meta.columns.len() {
            return Err(format!(
                "index equality arity mismatch for '{}': expected {} value(s), got {}",
                index_meta.table_name,
                index_meta.columns.len(),
                values.len()
            ));
        }
        let (key, bucket_value) = index_key_and_bucket_value(values)?;
        let mut idx_tree = BTree::new(&mut self.pager, index_meta.root_page);
        match idx_tree
            .lookup(key)
            .map_err(|e| format!("index lookup: {e}"))?
        {
            Some(payload) => {
                let buckets = decode_index_payload(&payload).map_err(|e| e.to_string())?;
                Ok(buckets
                    .into_iter()
                    .filter(|b| values_equal(&b.value, &bucket_value))
                    .flat_map(|b| b.rowids)
                    .collect::<Vec<i64>>())
            }
            None => Ok(Vec::new()),
        }
    }

    fn index_range_rowids(
        &mut self,
        index_root: PageNum,
        lower: Option<(&Value, bool)>,
        upper: Option<(&Value, bool)>,
    ) -> Result<Vec<i64>, String> {
        let mut idx_tree = BTree::new(&mut self.pager, index_root);
        let index_entries = if let Some((min_key, max_key)) = ordered_range_key_bounds(lower, upper)
        {
            if min_key > max_key {
                return Ok(Vec::new());
            }
            idx_tree
                .scan_range(min_key, max_key)
                .map_err(|e| format!("index range scan: {e}"))?
        } else {
            idx_tree
                .scan_all()
                .map_err(|e| format!("index scan: {e}"))?
        };

        let mut rowids = Vec::new();
        let mut seen = HashSet::new();

        for entry in index_entries {
            let buckets = decode_index_payload(&entry.payload).map_err(|e| e.to_string())?;
            for bucket in buckets {
                let lower_ok = if let Some((bound, inclusive)) = lower {
                    let ordering = compare_values(&bucket.value, bound)?;
                    ordering == std::cmp::Ordering::Greater
                        || (inclusive && ordering == std::cmp::Ordering::Equal)
                } else {
                    true
                };
                if !lower_ok {
                    continue;
                }

                let upper_ok = if let Some((bound, inclusive)) = upper {
                    let ordering = compare_values(&bucket.value, bound)?;
                    ordering == std::cmp::Ordering::Less
                        || (inclusive && ordering == std::cmp::Ordering::Equal)
                } else {
                    true
                };
                if !upper_ok {
                    continue;
                }

                for rowid in bucket.rowids {
                    if seen.insert(rowid) {
                        rowids.push(rowid);
                    }
                }
            }
        }

        Ok(rowids)
    }

    fn lookup_table_entries_by_rowids(
        &mut self,
        table_root: PageNum,
        mut rowids: Vec<i64>,
    ) -> Result<Vec<ralph_storage::btree::Entry>, String> {
        rowids.sort_unstable();
        rowids.dedup();

        let mut entries = Vec::with_capacity(rowids.len());
        let mut table_tree = BTree::new(&mut self.pager, table_root);
        for rowid in rowids {
            if let Some(payload) = table_tree
                .lookup(rowid)
                .map_err(|e| format!("table lookup: {e}"))?
            {
                entries.push(ralph_storage::btree::Entry {
                    key: rowid,
                    payload,
                });
            }
        }
        Ok(entries)
    }

    fn planner_indexes_for_table(&self, table_key: &str) -> Vec<IndexInfo> {
        let mut planner_indexes: Vec<IndexInfo> = self
            .indexes
            .iter()
            .filter(|(_, idx)| idx.table_key == table_key)
            .map(|(name, idx)| IndexInfo {
                name: name.clone(),
                table: idx.table_name.clone(),
                columns: idx.columns.clone(),
            })
            .collect();
        planner_indexes.sort_by(|left, right| left.name.cmp(&right.name));
        planner_indexes
    }

    fn read_rows_for_select(
        &mut self,
        meta: &TableMeta,
        where_clause: Option<&Expr>,
        access_path: &AccessPath,
    ) -> Result<Vec<Vec<Value>>, String> {
        let needs_materialized_candidate_read =
            matches!(access_path, AccessPath::IndexRange { .. })
                || matches!(access_path, AccessPath::IndexOr { .. })
                || matches!(
                    access_path,
                    AccessPath::IndexEq { columns, .. } if columns.len() != 1
                );
        if needs_materialized_candidate_read {
            return self.read_rows_via_candidates(meta, where_clause, access_path);
        }
        let scan_op: Box<dyn Operator + '_> = match access_path {
            AccessPath::TableScan => Box::new(TableScan::new(&mut self.pager, meta.root_page)),
            AccessPath::IndexEq {
                index_name,
                value_exprs,
                ..
            } => {
                let idx_key = normalize_identifier(index_name);
                let index_meta = self
                    .indexes
                    .get(&idx_key)
                    .cloned()
                    .ok_or_else(|| format!("index '{}' not found", index_name))?;

                let [value_expr] = value_exprs.as_slice() else {
                    return Err(format!(
                        "expected exactly one equality expression for index '{}'",
                        index_name
                    ));
                };
                let value = eval_expr(value_expr, None)?;

                Box::new(IndexEqScan::new(
                    &mut self.pager,
                    index_meta.root_page,
                    meta.root_page,
                    value,
                ))
            }
            AccessPath::IndexRange { .. } => unreachable!("handled above"),
            AccessPath::IndexOr { .. } => unreachable!("handled above"),
        };

        let root_op: Box<dyn Operator + '_> = if let Some(expr) = where_clause {
            let meta = meta.clone();
            let expr = expr.clone();
            Box::new(Filter::new(scan_op, move |row| {
                let val = eval_expr(&expr, Some((&meta, row)))
                    .map_err(|e| ralph_executor::ExecutorError::new(e))?;
                match val {
                    Value::Null => Ok(false),
                    Value::Integer(i) => Ok(i != 0),
                    Value::Real(f) => Ok(f != 0.0),
                    Value::Text(s) => Ok(!s.is_empty()),
                }
            }))
        } else {
            scan_op
        };

        let rows = ralph_executor::execute(root_op).map_err(|e| e.to_string())?;

        for row in &rows {
            if row.len() != meta.columns.len() {
                return Err(format!(
                    "row column count {} does not match table schema {}",
                    row.len(),
                    meta.columns.len()
                ));
            }
        }

        Ok(rows)
    }

    fn read_rows_via_candidates(
        &mut self,
        meta: &TableMeta,
        where_clause: Option<&Expr>,
        access_path: &AccessPath,
    ) -> Result<Vec<Vec<Value>>, String> {
        let entries = self.read_candidate_entries(meta, access_path)?;
        let mut rows = Vec::with_capacity(entries.len());
        for entry in entries {
            let row = decode_table_row(meta, &entry.payload)?;
            if where_clause_matches(meta, &row, where_clause)? {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    /// Read all rows from a table via full scan, returning decoded rows.
    fn read_all_rows(&mut self, meta: &TableMeta) -> Result<Vec<Vec<Value>>, String> {
        let scan = TableScan::new(&mut self.pager, meta.root_page);
        let rows = ralph_executor::execute(Box::new(scan)).map_err(|e| e.to_string())?;
        for row in &rows {
            if row.len() != meta.columns.len() {
                return Err(format!(
                    "row column count {} does not match table schema {}",
                    row.len(),
                    meta.columns.len()
                ));
            }
        }
        Ok(rows)
    }

    /// Execute a join query, producing a synthetic joined meta and joined rows.
    fn execute_join(
        &mut self,
        from: &ralph_parser::ast::FromClause,
    ) -> Result<(TableMeta, Vec<Vec<Value>>), String> {
        // Resolve left table
        let left_key = normalize_identifier(&from.table);
        let left_meta = self
            .tables
            .get(&left_key)
            .cloned()
            .ok_or_else(|| format!("no such table '{}'", from.table))?;
        let left_rows = self.read_all_rows(&left_meta)?;

        // Build joined meta starting from left table
        let left_alias = from.alias.as_deref().unwrap_or(&from.table);
        let mut joined_columns: Vec<String> = left_meta.columns.clone();
        let mut table_ranges: Vec<(String, usize, usize)> =
            vec![(left_alias.to_string(), 0, left_meta.columns.len())];
        let mut current_rows = left_rows;

        for join in &from.joins {
            let right_key = normalize_identifier(&join.table);
            let right_meta = self
                .tables
                .get(&right_key)
                .cloned()
                .ok_or_else(|| format!("no such table '{}'", join.table))?;
            let right_rows = self.read_all_rows(&right_meta)?;

            let right_alias = join.alias.as_deref().unwrap_or(&join.table);
            let right_col_start = joined_columns.len();
            joined_columns.extend(right_meta.columns.iter().cloned());
            table_ranges.push((
                right_alias.to_string(),
                right_col_start,
                right_col_start + right_meta.columns.len(),
            ));

            // Build the synthetic meta for the full joined table so far
            // so we can evaluate the ON condition
            let synthetic_meta = TableMeta {
                name: String::new(),
                columns: joined_columns.clone(),
                root_page: 0,
            };

            // Nested-loop join: cross product with optional ON filter
            let mut new_rows = Vec::new();
            for left_row in &current_rows {
                for right_row in &right_rows {
                    let mut combined: Vec<Value> =
                        Vec::with_capacity(left_row.len() + right_row.len());
                    combined.extend_from_slice(left_row);
                    combined.extend_from_slice(right_row);

                    if let Some(on_expr) = &join.condition {
                        let matches =
                            eval_join_expr(on_expr, &synthetic_meta, &combined, &table_ranges)?;
                        if !is_truthy(&matches) {
                            continue;
                        }
                    }
                    new_rows.push(combined);
                }
            }
            current_rows = new_rows;
        }

        let joined_meta = TableMeta {
            name: String::new(),
            columns: joined_columns,
            root_page: 0,
        };

        Ok((joined_meta, current_rows))
    }

    /// Execute a SELECT with JOIN clauses.
    fn execute_select_join(&mut self, stmt: SelectStmt) -> Result<ExecuteResult, String> {
        let aggregate_select = select_uses_aggregates(&stmt);
        let aggregate_having = stmt
            .having
            .as_ref()
            .map(expr_contains_aggregate)
            .unwrap_or(false);
        let from = stmt.from.as_ref().unwrap();
        let (joined_meta, mut joined_rows) = self.execute_join(from)?;

        // Build table_ranges for qualified column resolution
        let mut table_ranges: Vec<(String, usize, usize)> = Vec::new();
        {
            let left_key = normalize_identifier(&from.table);
            let left_meta = self.tables.get(&left_key).cloned().unwrap();
            let left_alias = from.alias.as_deref().unwrap_or(&from.table);
            table_ranges.push((left_alias.to_string(), 0, left_meta.columns.len()));
            let mut offset = left_meta.columns.len();
            for join in &from.joins {
                let right_key = normalize_identifier(&join.table);
                let right_meta = self.tables.get(&right_key).cloned().unwrap();
                let right_alias = join.alias.as_deref().unwrap_or(&join.table);
                table_ranges.push((
                    right_alias.to_string(),
                    offset,
                    offset + right_meta.columns.len(),
                ));
                offset += right_meta.columns.len();
            }
        }

        // Apply WHERE filter
        if let Some(where_expr) = &stmt.where_clause {
            let mut filtered = Vec::with_capacity(joined_rows.len());
            for row in joined_rows {
                let predicate = eval_join_expr(where_expr, &joined_meta, &row, &table_ranges)?;
                if is_truthy(&predicate) {
                    filtered.push(row);
                }
            }
            joined_rows = filtered;
        }

        let mut rows_with_order_keys = if !stmt.group_by.is_empty() {
            let mut groups = Vec::new();
            for row in joined_rows {
                let key =
                    evaluate_join_group_by_key(&stmt.group_by, &joined_meta, &row, &table_ranges)?;
                if let Some(existing) = groups
                    .iter_mut()
                    .find(|candidate: &&mut GroupState| group_keys_equal(&candidate.key, &key))
                {
                    existing.rows.push(row);
                } else {
                    groups.push(GroupState {
                        key,
                        rows: vec![row],
                        scalar_row_count: 0,
                    });
                }
            }

            let mut rows = Vec::with_capacity(groups.len());
            for group in &groups {
                let representative_row = group.rows.first().map(|row| row.as_slice());
                if let Some(having_expr) = stmt.having.as_ref() {
                    let predicate = eval_grouped_join_expr(
                        having_expr,
                        &joined_meta,
                        &group.rows,
                        representative_row,
                        &table_ranges,
                    )?;
                    if !is_truthy(&predicate) {
                        continue;
                    }
                }

                let projected = project_grouped_join_row(
                    &stmt.columns,
                    &joined_meta,
                    &group.rows,
                    representative_row,
                    &table_ranges,
                )?;
                let order_keys = evaluate_grouped_join_order_by_keys(
                    &stmt.order_by,
                    &joined_meta,
                    &group.rows,
                    representative_row,
                    &table_ranges,
                )?;
                rows.push((projected, order_keys));
            }
            rows
        } else {
            let aggregate_query = aggregate_select || aggregate_having;
            if stmt.having.is_some() && !aggregate_query {
                return Err("HAVING clause on a non-aggregate query".to_string());
            }

            if aggregate_query {
                let include_row = if let Some(having_expr) = stmt.having.as_ref() {
                    let predicate = eval_join_aggregate_expr(
                        having_expr,
                        &joined_meta,
                        &joined_rows,
                        &table_ranges,
                    )?;
                    is_truthy(&predicate)
                } else {
                    true
                };

                if include_row {
                    vec![(
                        project_join_aggregate_row(
                            &stmt.columns,
                            &joined_meta,
                            &joined_rows,
                            &table_ranges,
                        )?,
                        evaluate_join_aggregate_order_by_keys(
                            &stmt.order_by,
                            &joined_meta,
                            &joined_rows,
                            &table_ranges,
                        )?,
                    )]
                } else {
                    Vec::new()
                }
            } else {
                let mut rows = Vec::with_capacity(joined_rows.len());
                for row in &joined_rows {
                    let projected =
                        project_join_row(&stmt.columns, &joined_meta, row, &table_ranges)?;
                    let order_keys = evaluate_join_order_by_keys(
                        &stmt.order_by,
                        &joined_meta,
                        row,
                        &table_ranges,
                    )?;
                    rows.push((projected, order_keys));
                }
                rows
            }
        };

        // ORDER BY
        if !stmt.order_by.is_empty() {
            rows_with_order_keys.sort_by(|(_, left_keys), (_, right_keys)| {
                compare_order_keys(left_keys, right_keys, &stmt.order_by)
            });
        }

        let mut rows: Vec<Vec<Value>> = rows_with_order_keys
            .into_iter()
            .map(|(row, _)| row)
            .collect();

        // OFFSET
        let offset = eval_optional_usize_expr(stmt.offset.as_ref())?;
        if offset > 0 {
            if offset >= rows.len() {
                rows.clear();
            } else {
                rows = rows.into_iter().skip(offset).collect();
            }
        }

        // LIMIT
        if let Some(limit) = eval_optional_limit_expr(stmt.limit.as_ref())? {
            rows.truncate(limit);
        }

        let columns = select_join_output_columns(&stmt.columns, &joined_meta, &table_ranges)?;

        Ok(ExecuteResult::Select(QueryResult { columns, rows }))
    }

    fn validate_unique_constraints_for_insert_rows(
        &mut self,
        table_indexes: &[IndexMeta],
        rows: &[Vec<Value>],
    ) -> Result<(), String> {
        let excluded_rowids = HashSet::new();
        let mut seen_by_index: HashMap<PageNum, Vec<Value>> = HashMap::new();
        for row in rows {
            for index_meta in table_indexes {
                if !index_meta.unique {
                    continue;
                }
                let indexed_values = index_values_from_row(index_meta, row)?;
                if index_value_contains_null(&indexed_values) {
                    continue;
                }
                let (key, bucket_value) = index_key_and_bucket_value(&indexed_values)?;

                if self.unique_value_conflicts_with_existing(
                    index_meta,
                    key,
                    &bucket_value,
                    &excluded_rowids,
                )? {
                    return Err(unique_constraint_error(
                        &index_meta.table_name,
                        &index_meta.columns,
                    ));
                }

                let seen_values = seen_by_index.entry(index_meta.root_page).or_default();
                if seen_values
                    .iter()
                    .any(|seen_value| values_equal(seen_value, &bucket_value))
                {
                    return Err(unique_constraint_error(
                        &index_meta.table_name,
                        &index_meta.columns,
                    ));
                }
                seen_values.push(bucket_value);
            }
        }

        Ok(())
    }

    fn validate_unique_constraints_for_updates(
        &mut self,
        table_indexes: &[IndexMeta],
        updates: &[(i64, Vec<Value>, Vec<Value>)],
    ) -> Result<(), String> {
        let update_rowids: HashSet<i64> = updates.iter().map(|(rowid, _, _)| *rowid).collect();
        let mut current_values_by_index: HashMap<PageNum, HashMap<i64, Option<Value>>> =
            HashMap::new();

        for (rowid, original_row, _) in updates {
            for index_meta in table_indexes {
                if !index_meta.unique {
                    continue;
                }
                let indexed_values = index_values_from_row(index_meta, original_row)?;
                let encoded = if index_value_contains_null(&indexed_values) {
                    None
                } else {
                    Some(index_key_and_bucket_value(&indexed_values)?.1)
                };
                current_values_by_index
                    .entry(index_meta.root_page)
                    .or_default()
                    .insert(*rowid, encoded);
            }
        }

        for (rowid, _original_row, updated_row) in updates {
            for index_meta in table_indexes {
                if !index_meta.unique {
                    continue;
                }
                let indexed_values = index_values_from_row(index_meta, updated_row)?;

                let current_values = current_values_by_index
                    .entry(index_meta.root_page)
                    .or_default();

                if index_value_contains_null(&indexed_values) {
                    current_values.insert(*rowid, None);
                    continue;
                }
                let (key, bucket_value) = index_key_and_bucket_value(&indexed_values)?;

                if self.unique_value_conflicts_with_existing(
                    index_meta,
                    key,
                    &bucket_value,
                    &update_rowids,
                )? {
                    return Err(unique_constraint_error(
                        &index_meta.table_name,
                        &index_meta.columns,
                    ));
                }

                if current_values
                    .iter()
                    .any(|(candidate_rowid, candidate_value)| {
                        *candidate_rowid != *rowid
                            && candidate_value
                                .as_ref()
                                .is_some_and(|candidate| values_equal(candidate, &bucket_value))
                    })
                {
                    return Err(unique_constraint_error(
                        &index_meta.table_name,
                        &index_meta.columns,
                    ));
                }

                current_values.insert(*rowid, Some(bucket_value));
            }
        }

        Ok(())
    }

    fn unique_value_conflicts_with_existing(
        &mut self,
        index_meta: &IndexMeta,
        key: i64,
        bucket_value: &Value,
        excluded_rowids: &HashSet<i64>,
    ) -> Result<bool, String> {
        let mut tree = BTree::new(&mut self.pager, index_meta.root_page);
        let Some(payload) = tree
            .lookup(key)
            .map_err(|e| format!("lookup index entry: {e}"))?
        else {
            return Ok(false);
        };

        let buckets = decode_index_payload(&payload).map_err(|e| e.to_string())?;
        let Some(existing_bucket) = buckets
            .iter()
            .find(|bucket| values_equal(&bucket.value, bucket_value))
        else {
            return Ok(false);
        };

        Ok(existing_bucket
            .rowids
            .iter()
            .any(|rowid| !excluded_rowids.contains(rowid)))
    }
}

fn ordered_range_key_bounds(
    lower: Option<(&Value, bool)>,
    upper: Option<(&Value, bool)>,
) -> Option<(i64, i64)> {
    let min_key = match lower {
        Some((value, _)) => ordered_index_key_for_value(value)?,
        None => i64::MIN,
    };
    let max_key = match upper {
        Some((value, _)) => ordered_index_key_for_value(value)?,
        None => i64::MAX,
    };
    Some((min_key, max_key))
}

fn load_catalogs(
    pager: &mut Pager,
) -> Result<(HashMap<String, TableMeta>, HashMap<String, IndexMeta>), String> {
    let mut tables = HashMap::new();
    let table_entries = Schema::list_tables(pager).map_err(|e| format!("load tables: {e}"))?;
    for mut table in table_entries {
        table.columns.sort_by_key(|c| c.index);
        let table_key = normalize_identifier(&table.name);
        if tables.contains_key(&table_key) {
            return Err(format!("duplicate table in schema: '{}'", table.name));
        }

        tables.insert(
            table_key,
            TableMeta {
                name: table.name,
                columns: table.columns.into_iter().map(|c| c.name).collect(),
                root_page: table.root_page,
            },
        );
    }

    let mut indexes = HashMap::new();
    let index_entries = Schema::list_indexes(pager).map_err(|e| format!("load indexes: {e}"))?;
    for index in index_entries {
        let index_key = normalize_identifier(&index.name);
        if indexes.contains_key(&index_key) {
            return Err(format!("duplicate index in schema: '{}'", index.name));
        }
        if index.columns.is_empty() {
            return Err(format!(
                "index '{}' has no indexed column metadata",
                index.name
            ));
        }

        let table_key = normalize_identifier(&index.table_name);
        let table_meta = tables.get(&table_key).ok_or_else(|| {
            format!(
                "index '{}' references missing table '{}'",
                index.name, index.table_name
            )
        })?;
        let mut index_columns = Vec::with_capacity(index.columns.len());
        let mut index_column_indices = Vec::with_capacity(index.columns.len());
        for indexed_column in &index.columns {
            let column_idx = if (indexed_column.index as usize) < table_meta.columns.len()
                && table_meta.columns[indexed_column.index as usize]
                    .eq_ignore_ascii_case(&indexed_column.name)
            {
                indexed_column.index as usize
            } else {
                find_column_index(table_meta, &indexed_column.name).ok_or_else(|| {
                    format!(
                        "index '{}' references unknown column '{}' on table '{}'",
                        index.name, indexed_column.name, table_meta.name
                    )
                })?
            };
            index_columns.push(indexed_column.name.clone());
            index_column_indices.push(column_idx);
        }
        let unique = create_index_stmt_from_sql(&index.sql)
            .map(|stmt| stmt.unique)
            .unwrap_or(false);

        indexes.insert(
            index_key,
            IndexMeta {
                table_key,
                table_name: table_meta.name.clone(),
                columns: index_columns,
                column_indices: index_column_indices,
                root_page: index.root_page,
                unique,
            },
        );
    }

    Ok((tables, indexes))
}

fn create_index_stmt_from_sql(sql: &str) -> Option<CreateIndexStmt> {
    match ralph_parser::parse(sql) {
        Ok(Stmt::CreateIndex(stmt)) => Some(stmt),
        _ => None,
    }
}

fn unique_constraint_error(table_name: &str, columns: &[String]) -> String {
    let refs = columns
        .iter()
        .map(|column| format!("{table_name}.{column}"))
        .collect::<Vec<String>>()
        .join(", ");
    format!("UNIQUE constraint failed: {refs}")
}

fn validate_unique_index_backfill_rows<'a>(
    table_name: &str,
    columns: &[String],
    column_indices: &[usize],
    rows: impl Iterator<Item = &'a Vec<Value>>,
) -> Result<(), String> {
    if columns.len() != column_indices.len() {
        return Err(format!(
            "index column metadata mismatch for table '{}': {} columns vs {} indices",
            table_name,
            columns.len(),
            column_indices.len()
        ));
    }
    let mut seen_values = Vec::new();
    for row in rows {
        let values = indexed_values_from_row(columns, column_indices, table_name, row)?;
        if index_value_contains_null(&values) {
            continue;
        }
        let (_, bucket_value) = index_key_and_bucket_value(&values)?;
        if seen_values
            .iter()
            .any(|seen_value| values_equal(seen_value, &bucket_value))
        {
            return Err(unique_constraint_error(table_name, columns));
        }
        seen_values.push(bucket_value);
    }
    Ok(())
}

fn type_name_to_sql(type_name: Option<&TypeName>) -> String {
    match type_name {
        Some(TypeName::Integer) => "INTEGER".to_string(),
        Some(TypeName::Text) => "TEXT".to_string(),
        Some(TypeName::Real) => "REAL".to_string(),
        Some(TypeName::Blob) => "BLOB".to_string(),
        None => String::new(),
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
    let row = decode_row(payload).map_err(|e| e.to_string())?;
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

fn evaluate_group_by_key(
    group_by: &[Expr],
    row_ctx: Option<(&TableMeta, &[Value])>,
) -> Result<Vec<Value>, String> {
    let mut key = Vec::with_capacity(group_by.len());
    for expr in group_by {
        key.push(eval_expr(expr, row_ctx)?);
    }
    Ok(key)
}

fn group_keys_equal(left: &[Value], right: &[Value]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(lhs, rhs)| values_equal(lhs, rhs))
}

fn grouped_row_ctx<'a>(
    meta: Option<&'a TableMeta>,
    representative_row: Option<&'a [Value]>,
) -> Option<(&'a TableMeta, &'a [Value])> {
    meta.and_then(|table_meta| representative_row.map(|row| (table_meta, row)))
}

fn project_grouped_row(
    columns: &[SelectColumn],
    meta: Option<&TableMeta>,
    rows: &[Vec<Value>],
    scalar_row_count: usize,
    representative_row: Option<&[Value]>,
) -> Result<Vec<Value>, String> {
    let mut projected = Vec::new();
    for column in columns {
        match column {
            SelectColumn::AllColumns => {
                let row = representative_row
                    .ok_or_else(|| "SELECT * without FROM is not supported".to_string())?;
                projected.extend_from_slice(row);
            }
            SelectColumn::Expr { expr, .. } => projected.push(eval_grouped_expr(
                expr,
                meta,
                rows,
                scalar_row_count,
                representative_row,
            )?),
        }
    }
    Ok(projected)
}

fn evaluate_grouped_order_by_keys(
    order_by: &[OrderByItem],
    meta: Option<&TableMeta>,
    rows: &[Vec<Value>],
    scalar_row_count: usize,
    representative_row: Option<&[Value]>,
) -> Result<Vec<Value>, String> {
    let mut out = Vec::with_capacity(order_by.len());
    for item in order_by {
        out.push(eval_grouped_expr(
            &item.expr,
            meta,
            rows,
            scalar_row_count,
            representative_row,
        )?);
    }
    Ok(out)
}

fn eval_grouped_expr(
    expr: &Expr,
    meta: Option<&TableMeta>,
    rows: &[Vec<Value>],
    scalar_row_count: usize,
    representative_row: Option<&[Value]>,
) -> Result<Value, String> {
    let row_ctx = grouped_row_ctx(meta, representative_row);
    if !expr_contains_aggregate(expr) {
        return eval_expr(expr, row_ctx);
    }

    match expr {
        Expr::IntegerLiteral(_)
        | Expr::FloatLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Null
        | Expr::ColumnRef { .. } => eval_expr(expr, row_ctx),
        Expr::Paren(inner) => {
            eval_grouped_expr(inner, meta, rows, scalar_row_count, representative_row)
        }
        Expr::UnaryOp { op, expr } => {
            let value = eval_grouped_expr(expr, meta, rows, scalar_row_count, representative_row)?;
            match op {
                UnaryOperator::Negate => match value {
                    Value::Integer(i) => Ok(Value::Integer(-i)),
                    Value::Real(f) => Ok(Value::Real(-f)),
                    Value::Null => Ok(Value::Null),
                    _ => Err("cannot negate non-numeric value".to_string()),
                },
                UnaryOperator::Not => Ok(Value::Integer((!is_truthy(&value)) as i64)),
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let lhs = eval_grouped_expr(left, meta, rows, scalar_row_count, representative_row)?;
            let rhs = eval_grouped_expr(right, meta, rows, scalar_row_count, representative_row)?;
            eval_binary_op(&lhs, *op, &rhs)
        }
        Expr::IsNull { expr, negated } => {
            let value = eval_grouped_expr(expr, meta, rows, scalar_row_count, representative_row)?;
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
            let value = eval_grouped_expr(expr, meta, rows, scalar_row_count, representative_row)?;
            let low_value =
                eval_grouped_expr(low, meta, rows, scalar_row_count, representative_row)?;
            let high_value =
                eval_grouped_expr(high, meta, rows, scalar_row_count, representative_row)?;
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
            let value = eval_grouped_expr(expr, meta, rows, scalar_row_count, representative_row)?;
            let mut found = false;
            for item in list {
                let candidate =
                    eval_grouped_expr(item, meta, rows, scalar_row_count, representative_row)?;
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
            if is_aggregate_function(name) {
                eval_aggregate_function(name, args, meta, rows, scalar_row_count)
            } else {
                eval_expr(expr, row_ctx)
            }
        }
    }
}

fn select_uses_aggregates(stmt: &SelectStmt) -> bool {
    stmt.columns.iter().any(|column| match column {
        SelectColumn::AllColumns => false,
        SelectColumn::Expr { expr, .. } => expr_contains_aggregate(expr),
    })
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, args } => {
            is_aggregate_function(name) || args.iter().any(expr_contains_aggregate)
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } => expr_contains_aggregate(expr),
        Expr::IsNull { expr, .. } => expr_contains_aggregate(expr),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(low)
                || expr_contains_aggregate(high)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate(expr) || list.iter().any(expr_contains_aggregate)
        }
        Expr::Paren(inner) => expr_contains_aggregate(inner),
        _ => false,
    }
}

fn is_aggregate_function(name: &str) -> bool {
    name.eq_ignore_ascii_case("COUNT")
        || name.eq_ignore_ascii_case("SUM")
        || name.eq_ignore_ascii_case("AVG")
        || name.eq_ignore_ascii_case("MIN")
        || name.eq_ignore_ascii_case("MAX")
}

fn project_aggregate_row(
    columns: &[SelectColumn],
    meta: Option<&TableMeta>,
    rows: &[Vec<Value>],
    scalar_row_count: usize,
) -> Result<Vec<Value>, String> {
    let mut projected = Vec::new();
    for column in columns {
        match column {
            SelectColumn::AllColumns => {
                return Err("SELECT * is not supported in aggregate queries".to_string());
            }
            SelectColumn::Expr { expr, .. } => {
                projected.push(eval_aggregate_expr(expr, meta, rows, scalar_row_count)?)
            }
        }
    }
    Ok(projected)
}

fn evaluate_aggregate_order_by_keys(
    order_by: &[OrderByItem],
    meta: Option<&TableMeta>,
    rows: &[Vec<Value>],
    scalar_row_count: usize,
) -> Result<Vec<Value>, String> {
    let mut out = Vec::with_capacity(order_by.len());
    for item in order_by {
        out.push(eval_aggregate_expr(
            &item.expr,
            meta,
            rows,
            scalar_row_count,
        )?);
    }
    Ok(out)
}

fn eval_aggregate_expr(
    expr: &Expr,
    meta: Option<&TableMeta>,
    rows: &[Vec<Value>],
    scalar_row_count: usize,
) -> Result<Value, String> {
    match expr {
        Expr::IntegerLiteral(i) => Ok(Value::Integer(*i)),
        Expr::FloatLiteral(f) => Ok(Value::Real(*f)),
        Expr::StringLiteral(s) => Ok(Value::Text(s.clone())),
        Expr::Null => Ok(Value::Null),
        Expr::Paren(inner) => eval_aggregate_expr(inner, meta, rows, scalar_row_count),
        Expr::ColumnRef { .. } => Err(
            "column references outside aggregate functions are not supported without GROUP BY"
                .to_string(),
        ),
        Expr::UnaryOp { op, expr } => {
            let v = eval_aggregate_expr(expr, meta, rows, scalar_row_count)?;
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
            let lhs = eval_aggregate_expr(left, meta, rows, scalar_row_count)?;
            let rhs = eval_aggregate_expr(right, meta, rows, scalar_row_count)?;
            eval_binary_op(&lhs, *op, &rhs)
        }
        Expr::IsNull { expr, negated } => {
            let v = eval_aggregate_expr(expr, meta, rows, scalar_row_count)?;
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
            let v = eval_aggregate_expr(expr, meta, rows, scalar_row_count)?;
            let low_v = eval_aggregate_expr(low, meta, rows, scalar_row_count)?;
            let high_v = eval_aggregate_expr(high, meta, rows, scalar_row_count)?;
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
            let value = eval_aggregate_expr(expr, meta, rows, scalar_row_count)?;
            let mut found = false;
            for item in list {
                let candidate = eval_aggregate_expr(item, meta, rows, scalar_row_count)?;
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
            eval_aggregate_function(name, args, meta, rows, scalar_row_count)
        }
    }
}

fn eval_aggregate_function(
    name: &str,
    args: &[Expr],
    meta: Option<&TableMeta>,
    rows: &[Vec<Value>],
    scalar_row_count: usize,
) -> Result<Value, String> {
    if !is_aggregate_function(name) {
        return Err(format!("function '{name}' is not supported yet"));
    }

    if name.eq_ignore_ascii_case("COUNT") {
        if args.len() != 1 {
            return Err("COUNT() expects exactly one argument".to_string());
        }
        if is_count_star_argument(&args[0]) {
            let count = aggregate_input_row_count(meta, rows, scalar_row_count);
            return Ok(Value::Integer(count as i64));
        }

        validate_aggregate_argument(name, &args[0])?;
        let mut count = 0i64;
        for_each_aggregate_input_row(meta, rows, scalar_row_count, |row_ctx| {
            let value = eval_expr(&args[0], row_ctx)?;
            if !matches!(value, Value::Null) {
                count += 1;
            }
            Ok(())
        })?;
        return Ok(Value::Integer(count));
    }

    if args.len() != 1 {
        return Err(format!("{name}() expects exactly one argument"));
    }
    validate_aggregate_argument(name, &args[0])?;

    if name.eq_ignore_ascii_case("SUM") {
        let mut sum = 0.0f64;
        let mut saw_value = false;
        let mut all_integers = true;
        for_each_aggregate_input_row(meta, rows, scalar_row_count, |row_ctx| {
            let value = eval_expr(&args[0], row_ctx)?;
            match value {
                Value::Null => {}
                Value::Integer(i) => {
                    sum += i as f64;
                    saw_value = true;
                }
                Value::Real(f) => {
                    sum += f;
                    saw_value = true;
                    all_integers = false;
                }
                Value::Text(_) => {
                    return Err("SUM() expects numeric values".to_string());
                }
            }
            Ok(())
        })?;
        if !saw_value {
            return Ok(Value::Null);
        }
        return if all_integers {
            Ok(Value::Integer(sum as i64))
        } else {
            Ok(Value::Real(sum))
        };
    }

    if name.eq_ignore_ascii_case("AVG") {
        let mut sum = 0.0f64;
        let mut count = 0usize;
        for_each_aggregate_input_row(meta, rows, scalar_row_count, |row_ctx| {
            let value = eval_expr(&args[0], row_ctx)?;
            match value {
                Value::Null => {}
                Value::Integer(i) => {
                    sum += i as f64;
                    count += 1;
                }
                Value::Real(f) => {
                    sum += f;
                    count += 1;
                }
                Value::Text(_) => {
                    return Err("AVG() expects numeric values".to_string());
                }
            }
            Ok(())
        })?;
        if count == 0 {
            return Ok(Value::Null);
        }
        return Ok(Value::Real(sum / (count as f64)));
    }

    let mut best: Option<Value> = None;
    for_each_aggregate_input_row(meta, rows, scalar_row_count, |row_ctx| {
        let value = eval_expr(&args[0], row_ctx)?;
        if matches!(value, Value::Null) {
            return Ok(());
        }

        match &best {
            None => {
                best = Some(value);
            }
            Some(current) => {
                let cmp = compare_sort_values(&value, current);
                if name.eq_ignore_ascii_case("MIN") {
                    if cmp == std::cmp::Ordering::Less {
                        best = Some(value);
                    }
                } else if cmp == std::cmp::Ordering::Greater {
                    best = Some(value);
                }
            }
        }
        Ok(())
    })?;
    Ok(best.unwrap_or(Value::Null))
}

fn validate_aggregate_argument(name: &str, arg: &Expr) -> Result<(), String> {
    if expr_contains_aggregate(arg) {
        return Err(format!(
            "nested aggregate functions are not supported in {name}()"
        ));
    }
    Ok(())
}

fn for_each_aggregate_input_row<F>(
    meta: Option<&TableMeta>,
    rows: &[Vec<Value>],
    scalar_row_count: usize,
    mut f: F,
) -> Result<(), String>
where
    F: FnMut(Option<(&TableMeta, &[Value])>) -> Result<(), String>,
{
    if let Some(meta) = meta {
        for row in rows {
            f(Some((meta, row)))?;
        }
    } else {
        for _ in 0..scalar_row_count {
            f(None)?;
        }
    }
    Ok(())
}

fn aggregate_input_row_count(
    meta: Option<&TableMeta>,
    rows: &[Vec<Value>],
    scalar_row_count: usize,
) -> usize {
    if meta.is_some() {
        rows.len()
    } else {
        scalar_row_count
    }
}

fn is_count_star_argument(arg: &Expr) -> bool {
    matches!(
        arg,
        Expr::ColumnRef {
            table: None,
            column,
        } if column == "*"
    )
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

fn project_join_row(
    columns: &[SelectColumn],
    meta: &TableMeta,
    row: &[Value],
    table_ranges: &[(String, usize, usize)],
) -> Result<Vec<Value>, String> {
    let mut projected = Vec::new();
    for column in columns {
        match column {
            SelectColumn::AllColumns => projected.extend_from_slice(row),
            SelectColumn::Expr { expr, .. } => {
                projected.push(eval_join_expr(expr, meta, row, table_ranges)?)
            }
        }
    }
    Ok(projected)
}

fn evaluate_join_order_by_keys(
    order_by: &[OrderByItem],
    meta: &TableMeta,
    row: &[Value],
    table_ranges: &[(String, usize, usize)],
) -> Result<Vec<Value>, String> {
    let mut keys = Vec::with_capacity(order_by.len());
    for item in order_by {
        keys.push(eval_join_expr(&item.expr, meta, row, table_ranges)?);
    }
    Ok(keys)
}

fn evaluate_join_group_by_key(
    group_by: &[Expr],
    meta: &TableMeta,
    row: &[Value],
    table_ranges: &[(String, usize, usize)],
) -> Result<Vec<Value>, String> {
    let mut key = Vec::with_capacity(group_by.len());
    for expr in group_by {
        key.push(eval_join_expr(expr, meta, row, table_ranges)?);
    }
    Ok(key)
}

fn project_grouped_join_row(
    columns: &[SelectColumn],
    meta: &TableMeta,
    rows: &[Vec<Value>],
    representative_row: Option<&[Value]>,
    table_ranges: &[(String, usize, usize)],
) -> Result<Vec<Value>, String> {
    let mut projected = Vec::new();
    for column in columns {
        match column {
            SelectColumn::AllColumns => {
                let row = representative_row
                    .ok_or_else(|| "SELECT * without FROM is not supported".to_string())?;
                projected.extend_from_slice(row);
            }
            SelectColumn::Expr { expr, .. } => projected.push(eval_grouped_join_expr(
                expr,
                meta,
                rows,
                representative_row,
                table_ranges,
            )?),
        }
    }
    Ok(projected)
}

fn evaluate_grouped_join_order_by_keys(
    order_by: &[OrderByItem],
    meta: &TableMeta,
    rows: &[Vec<Value>],
    representative_row: Option<&[Value]>,
    table_ranges: &[(String, usize, usize)],
) -> Result<Vec<Value>, String> {
    let mut out = Vec::with_capacity(order_by.len());
    for item in order_by {
        out.push(eval_grouped_join_expr(
            &item.expr,
            meta,
            rows,
            representative_row,
            table_ranges,
        )?);
    }
    Ok(out)
}

fn eval_grouped_join_expr(
    expr: &Expr,
    meta: &TableMeta,
    rows: &[Vec<Value>],
    representative_row: Option<&[Value]>,
    table_ranges: &[(String, usize, usize)],
) -> Result<Value, String> {
    if !expr_contains_aggregate(expr) {
        let row = representative_row.ok_or_else(|| {
            "grouped join query requires at least one row for non-aggregate expressions".to_string()
        })?;
        return eval_join_expr(expr, meta, row, table_ranges);
    }

    match expr {
        Expr::IntegerLiteral(_)
        | Expr::FloatLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Null
        | Expr::ColumnRef { .. } => {
            let row = representative_row.ok_or_else(|| {
                "grouped join query requires at least one row for non-aggregate expressions"
                    .to_string()
            })?;
            eval_join_expr(expr, meta, row, table_ranges)
        }
        Expr::Paren(inner) => {
            eval_grouped_join_expr(inner, meta, rows, representative_row, table_ranges)
        }
        Expr::UnaryOp { op, expr } => {
            let value = eval_grouped_join_expr(expr, meta, rows, representative_row, table_ranges)?;
            match op {
                UnaryOperator::Negate => match value {
                    Value::Integer(i) => Ok(Value::Integer(-i)),
                    Value::Real(f) => Ok(Value::Real(-f)),
                    Value::Null => Ok(Value::Null),
                    _ => Err("cannot negate non-numeric value".to_string()),
                },
                UnaryOperator::Not => Ok(Value::Integer((!is_truthy(&value)) as i64)),
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let lhs = eval_grouped_join_expr(left, meta, rows, representative_row, table_ranges)?;
            let rhs = eval_grouped_join_expr(right, meta, rows, representative_row, table_ranges)?;
            eval_binary_op(&lhs, *op, &rhs)
        }
        Expr::IsNull { expr, negated } => {
            let value = eval_grouped_join_expr(expr, meta, rows, representative_row, table_ranges)?;
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
            let value = eval_grouped_join_expr(expr, meta, rows, representative_row, table_ranges)?;
            let low_value =
                eval_grouped_join_expr(low, meta, rows, representative_row, table_ranges)?;
            let high_value =
                eval_grouped_join_expr(high, meta, rows, representative_row, table_ranges)?;
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
            let value = eval_grouped_join_expr(expr, meta, rows, representative_row, table_ranges)?;
            let mut found = false;
            for item in list {
                let candidate =
                    eval_grouped_join_expr(item, meta, rows, representative_row, table_ranges)?;
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
            if is_aggregate_function(name) {
                eval_join_aggregate_function(name, args, meta, rows, table_ranges)
            } else {
                let row = representative_row.ok_or_else(|| {
                    "grouped join query requires at least one row for non-aggregate expressions"
                        .to_string()
                })?;
                eval_join_expr(expr, meta, row, table_ranges)
            }
        }
    }
}

fn project_join_aggregate_row(
    columns: &[SelectColumn],
    meta: &TableMeta,
    rows: &[Vec<Value>],
    table_ranges: &[(String, usize, usize)],
) -> Result<Vec<Value>, String> {
    let mut projected = Vec::new();
    for column in columns {
        match column {
            SelectColumn::AllColumns => {
                return Err("SELECT * is not supported in aggregate queries".to_string());
            }
            SelectColumn::Expr { expr, .. } => {
                projected.push(eval_join_aggregate_expr(expr, meta, rows, table_ranges)?)
            }
        }
    }
    Ok(projected)
}

fn evaluate_join_aggregate_order_by_keys(
    order_by: &[OrderByItem],
    meta: &TableMeta,
    rows: &[Vec<Value>],
    table_ranges: &[(String, usize, usize)],
) -> Result<Vec<Value>, String> {
    let mut out = Vec::with_capacity(order_by.len());
    for item in order_by {
        out.push(eval_join_aggregate_expr(
            &item.expr,
            meta,
            rows,
            table_ranges,
        )?);
    }
    Ok(out)
}

fn eval_join_aggregate_expr(
    expr: &Expr,
    meta: &TableMeta,
    rows: &[Vec<Value>],
    table_ranges: &[(String, usize, usize)],
) -> Result<Value, String> {
    match expr {
        Expr::IntegerLiteral(i) => Ok(Value::Integer(*i)),
        Expr::FloatLiteral(f) => Ok(Value::Real(*f)),
        Expr::StringLiteral(s) => Ok(Value::Text(s.clone())),
        Expr::Null => Ok(Value::Null),
        Expr::Paren(inner) => eval_join_aggregate_expr(inner, meta, rows, table_ranges),
        Expr::ColumnRef { .. } => Err(
            "column references outside aggregate functions are not supported without GROUP BY"
                .to_string(),
        ),
        Expr::UnaryOp { op, expr } => {
            let v = eval_join_aggregate_expr(expr, meta, rows, table_ranges)?;
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
            let lhs = eval_join_aggregate_expr(left, meta, rows, table_ranges)?;
            let rhs = eval_join_aggregate_expr(right, meta, rows, table_ranges)?;
            eval_binary_op(&lhs, *op, &rhs)
        }
        Expr::IsNull { expr, negated } => {
            let v = eval_join_aggregate_expr(expr, meta, rows, table_ranges)?;
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
            let v = eval_join_aggregate_expr(expr, meta, rows, table_ranges)?;
            let low_v = eval_join_aggregate_expr(low, meta, rows, table_ranges)?;
            let high_v = eval_join_aggregate_expr(high, meta, rows, table_ranges)?;
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
            let value = eval_join_aggregate_expr(expr, meta, rows, table_ranges)?;
            let mut found = false;
            for item in list {
                let candidate = eval_join_aggregate_expr(item, meta, rows, table_ranges)?;
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
            eval_join_aggregate_function(name, args, meta, rows, table_ranges)
        }
    }
}

fn eval_join_aggregate_function(
    name: &str,
    args: &[Expr],
    meta: &TableMeta,
    rows: &[Vec<Value>],
    table_ranges: &[(String, usize, usize)],
) -> Result<Value, String> {
    if !is_aggregate_function(name) {
        return Err(format!("function '{name}' is not supported yet"));
    }

    if name.eq_ignore_ascii_case("COUNT") {
        if args.len() != 1 {
            return Err("COUNT() expects exactly one argument".to_string());
        }
        if is_count_star_argument(&args[0]) {
            return Ok(Value::Integer(rows.len() as i64));
        }

        validate_aggregate_argument(name, &args[0])?;
        let mut count = 0i64;
        for row in rows {
            let value = eval_join_expr(&args[0], meta, row, table_ranges)?;
            if !matches!(value, Value::Null) {
                count += 1;
            }
        }
        return Ok(Value::Integer(count));
    }

    if args.len() != 1 {
        return Err(format!("{name}() expects exactly one argument"));
    }
    validate_aggregate_argument(name, &args[0])?;

    if name.eq_ignore_ascii_case("SUM") {
        let mut sum = 0.0f64;
        let mut saw_value = false;
        let mut all_integers = true;
        for row in rows {
            let value = eval_join_expr(&args[0], meta, row, table_ranges)?;
            match value {
                Value::Null => {}
                Value::Integer(i) => {
                    sum += i as f64;
                    saw_value = true;
                }
                Value::Real(f) => {
                    sum += f;
                    saw_value = true;
                    all_integers = false;
                }
                Value::Text(_) => {
                    return Err("SUM() expects numeric values".to_string());
                }
            }
        }
        if !saw_value {
            return Ok(Value::Null);
        }
        return if all_integers {
            Ok(Value::Integer(sum as i64))
        } else {
            Ok(Value::Real(sum))
        };
    }

    if name.eq_ignore_ascii_case("AVG") {
        let mut sum = 0.0f64;
        let mut count = 0usize;
        for row in rows {
            let value = eval_join_expr(&args[0], meta, row, table_ranges)?;
            match value {
                Value::Null => {}
                Value::Integer(i) => {
                    sum += i as f64;
                    count += 1;
                }
                Value::Real(f) => {
                    sum += f;
                    count += 1;
                }
                Value::Text(_) => {
                    return Err("AVG() expects numeric values".to_string());
                }
            }
        }
        if count == 0 {
            return Ok(Value::Null);
        }
        return Ok(Value::Real(sum / (count as f64)));
    }

    let mut best: Option<Value> = None;
    for row in rows {
        let value = eval_join_expr(&args[0], meta, row, table_ranges)?;
        if matches!(value, Value::Null) {
            continue;
        }

        match &best {
            None => {
                best = Some(value);
            }
            Some(current) => {
                let cmp = compare_sort_values(&value, current);
                if name.eq_ignore_ascii_case("MIN") {
                    if cmp == std::cmp::Ordering::Less {
                        best = Some(value);
                    }
                } else if cmp == std::cmp::Ordering::Greater {
                    best = Some(value);
                }
            }
        }
    }
    Ok(best.unwrap_or(Value::Null))
}

fn select_join_output_columns(
    columns: &[SelectColumn],
    meta: &TableMeta,
    table_ranges: &[(String, usize, usize)],
) -> Result<Vec<String>, String> {
    let mut names = Vec::new();
    for (idx, col) in columns.iter().enumerate() {
        match col {
            SelectColumn::AllColumns => {
                // For joins, qualify column names when there are name collisions
                let mut seen = HashSet::new();
                let mut duplicates = HashSet::new();
                for col_name in &meta.columns {
                    let lower = col_name.to_ascii_lowercase();
                    if !seen.insert(lower.clone()) {
                        duplicates.insert(lower);
                    }
                }
                for (table_alias, start, end) in table_ranges {
                    for col_name in &meta.columns[*start..*end] {
                        if duplicates.contains(&col_name.to_ascii_lowercase()) {
                            names.push(format!("{}.{}", table_alias, col_name));
                        } else {
                            names.push(col_name.clone());
                        }
                    }
                }
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

/// Evaluate an expression in a joined-row context with table-qualified column resolution.
///
/// `table_ranges` maps (table_alias, start_col_idx, end_col_idx) to locate which
/// slice of `row` belongs to which table.
fn eval_join_expr(
    expr: &Expr,
    meta: &TableMeta,
    row: &[Value],
    table_ranges: &[(String, usize, usize)],
) -> Result<Value, String> {
    match expr {
        Expr::IntegerLiteral(i) => Ok(Value::Integer(*i)),
        Expr::FloatLiteral(f) => Ok(Value::Real(*f)),
        Expr::StringLiteral(s) => Ok(Value::Text(s.clone())),
        Expr::Null => Ok(Value::Null),
        Expr::Paren(inner) => eval_join_expr(inner, meta, row, table_ranges),
        Expr::ColumnRef { table, column } => {
            if column == "*" {
                return Err("'*' cannot be used as a scalar expression".to_string());
            }
            if let Some(table_qualifier) = table {
                // Qualified: find the table range and resolve within it
                let (_, start, end) = table_ranges
                    .iter()
                    .find(|(alias, _, _)| alias.eq_ignore_ascii_case(table_qualifier))
                    .ok_or_else(|| format!("unknown table or alias '{}'", table_qualifier))?;
                let range_cols = &meta.columns[*start..*end];
                let local_idx = range_cols
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(column))
                    .ok_or_else(|| {
                        format!("unknown column '{}' in table '{}'", column, table_qualifier)
                    })?;
                Ok(row[start + local_idx].clone())
            } else {
                // Unqualified: search all table ranges, error if ambiguous
                let mut found: Option<usize> = None;
                for (_, start, end) in table_ranges {
                    for (i, col) in meta.columns[*start..*end].iter().enumerate() {
                        if col.eq_ignore_ascii_case(column) {
                            if found.is_some() {
                                return Err(format!("ambiguous column name '{}'", column));
                            }
                            found = Some(start + i);
                        }
                    }
                }
                let idx = found.ok_or_else(|| format!("unknown column '{}'", column))?;
                Ok(row[idx].clone())
            }
        }
        Expr::UnaryOp { op, expr } => {
            let v = eval_join_expr(expr, meta, row, table_ranges)?;
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
            let lhs = eval_join_expr(left, meta, row, table_ranges)?;
            let rhs = eval_join_expr(right, meta, row, table_ranges)?;
            eval_binary_op(&lhs, *op, &rhs)
        }
        Expr::IsNull { expr, negated } => {
            let v = eval_join_expr(expr, meta, row, table_ranges)?;
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
            let v = eval_join_expr(expr, meta, row, table_ranges)?;
            let low_v = eval_join_expr(low, meta, row, table_ranges)?;
            let high_v = eval_join_expr(high, meta, row, table_ranges)?;
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
            let value = eval_join_expr(expr, meta, row, table_ranges)?;
            let mut found = false;
            for item in list {
                let candidate = eval_join_expr(item, meta, row, table_ranges)?;
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

fn index_values_from_row(index_meta: &IndexMeta, row: &[Value]) -> Result<Vec<Value>, String> {
    indexed_values_from_row(
        &index_meta.columns,
        &index_meta.column_indices,
        &index_meta.table_name,
        row,
    )
}

fn indexed_values_from_row(
    columns: &[String],
    column_indices: &[usize],
    table_name: &str,
    row: &[Value],
) -> Result<Vec<Value>, String> {
    if columns.len() != column_indices.len() {
        return Err(format!(
            "index column metadata mismatch on table '{}': {} columns vs {} indices",
            table_name,
            columns.len(),
            column_indices.len()
        ));
    }
    let mut values = Vec::with_capacity(columns.len());
    for (column, column_idx) in columns.iter().zip(column_indices.iter().copied()) {
        let value = row.get(column_idx).ok_or_else(|| {
            format!(
                "row missing indexed column '{}' for index on '{}'",
                column, table_name
            )
        })?;
        values.push(value.clone());
    }
    Ok(values)
}

fn index_value_contains_null(values: &[Value]) -> bool {
    values.iter().any(|value| matches!(value, Value::Null))
}

fn index_key_and_bucket_value(indexed_values: &[Value]) -> Result<(i64, Value), String> {
    match indexed_values {
        [] => Err("index key requires at least one value".to_string()),
        [single] => Ok((
            index_key_for_value(single).map_err(|e| e.to_string())?,
            single.clone(),
        )),
        _ => {
            let encoded = encode_index_value_tuple(indexed_values)?;
            let hash = fnv1a64(&encoded);
            Ok((
                i64::from_be_bytes(hash.to_be_bytes()),
                Value::Text(format!("__idx_tuple__:{}", hex_encode(&encoded))),
            ))
        }
    }
}

fn encode_index_value_tuple(values: &[Value]) -> Result<Vec<u8>, String> {
    let value_count: u32 = values
        .len()
        .try_into()
        .map_err(|_| "index key has too many values".to_string())?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&value_count.to_be_bytes());
    for value in values {
        encode_value(value, &mut encoded).map_err(|e| e.to_string())?;
    }
    Ok(encoded)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
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

fn encode_row(row: &[Value]) -> Result<Vec<u8>, String> {
    let col_count: u32 = row
        .len()
        .try_into()
        .map_err(|_| "row has too many columns".to_string())?;

    let mut out = Vec::new();
    out.extend_from_slice(&col_count.to_be_bytes());
    for value in row {
        encode_value(value, &mut out).map_err(|e| e.to_string())?;
    }
    Ok(out)
}

fn encode_index_payload(buckets: &[IndexBucket]) -> Result<Vec<u8>, String> {
    let bucket_count: u32 = buckets
        .len()
        .try_into()
        .map_err(|_| "too many index buckets".to_string())?;
    let mut out = Vec::new();
    out.extend_from_slice(&bucket_count.to_be_bytes());

    for bucket in buckets {
        encode_value(&bucket.value, &mut out).map_err(|e| e.to_string())?;
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
        indexed_rowids_for_values(db, index_name, std::slice::from_ref(value))
    }

    fn indexed_rowids_for_values(
        db: &mut Database,
        index_name: &str,
        values: &[Value],
    ) -> Vec<i64> {
        let idx_key = normalize_identifier(index_name);
        let index_meta = db.indexes.get(&idx_key).unwrap().clone();
        let (key, bucket_value) = index_key_and_bucket_value(values).unwrap();
        let mut index_tree = BTree::new(&mut db.pager, index_meta.root_page);
        let payload = index_tree.lookup(key).unwrap().unwrap();
        let buckets = decode_index_payload(&payload).unwrap();
        buckets
            .into_iter()
            .find(|bucket| values_equal(&bucket.value, &bucket_value))
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
    fn select_aggregate_functions_with_where_and_nulls() {
        let path = temp_db_path("aggregate_where_nulls");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER, score INTEGER);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30);")
            .unwrap();

        let result = db
            .execute(
                "SELECT COUNT(*), COUNT(score), SUM(score), AVG(score), MIN(score), MAX(score) \
                 FROM t WHERE id >= 2;",
            )
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![
                        Value::Integer(2),
                        Value::Integer(1),
                        Value::Integer(30),
                        Value::Real(30.0),
                        Value::Integer(30),
                        Value::Integer(30),
                    ]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_aggregate_functions_over_empty_input() {
        let path = temp_db_path("aggregate_empty_input");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER, score INTEGER);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30);")
            .unwrap();

        let result = db
            .execute(
                "SELECT COUNT(*), COUNT(score), SUM(score), AVG(score), MIN(score), MAX(score) \
                 FROM t WHERE id > 10;",
            )
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![
                        Value::Integer(0),
                        Value::Integer(0),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                    ]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_aggregate_without_from_respects_where() {
        let path = temp_db_path("aggregate_no_from");
        let mut db = Database::open(&path).unwrap();

        let true_result = db
            .execute("SELECT COUNT(*), SUM(2 + 3), MAX(7) WHERE 1;")
            .unwrap();
        match true_result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![
                        Value::Integer(1),
                        Value::Integer(5),
                        Value::Integer(7)
                    ]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        let false_result = db
            .execute("SELECT COUNT(*), SUM(2 + 3), MAX(7) WHERE 0;")
            .unwrap();
        match false_result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![Value::Integer(0), Value::Null, Value::Null]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_mixed_aggregate_and_column_without_group_by_errors() {
        let path = temp_db_path("aggregate_mixed_column_error");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2), (3);").unwrap();

        let err = db.execute("SELECT COUNT(*) + 1, id FROM t;").unwrap_err();
        assert!(err.contains("without GROUP BY"));

        cleanup(&path);
    }

    #[test]
    fn select_group_by_aggregate_and_having_filters_groups() {
        let path = temp_db_path("group_by_aggregate_having");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER, score INTEGER);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 10), (3, 20), (4, NULL);")
            .unwrap();

        let result = db
            .execute(
                "SELECT score, COUNT(*), SUM(id) FROM t GROUP BY score HAVING COUNT(*) > 1 \
                 ORDER BY score;",
            )
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![
                        Value::Integer(10),
                        Value::Integer(2),
                        Value::Integer(3),
                    ]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_group_by_without_aggregates_deduplicates_rows() {
        let path = temp_db_path("group_by_dedup");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER, score INTEGER);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 10), (3, 20), (4, NULL);")
            .unwrap();

        let result = db
            .execute("SELECT score FROM t GROUP BY score ORDER BY score;")
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Null],
                        vec![Value::Integer(10)],
                        vec![Value::Integer(20)],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_having_without_group_by_aggregate_query() {
        let path = temp_db_path("having_aggregate_query");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2), (3);").unwrap();

        let true_result = db
            .execute("SELECT COUNT(*) FROM t HAVING COUNT(*) > 0;")
            .unwrap();
        match true_result {
            ExecuteResult::Select(q) => {
                assert_eq!(q.rows, vec![vec![Value::Integer(3)]]);
            }
            _ => panic!("expected SELECT result"),
        }

        let false_result = db
            .execute("SELECT COUNT(*) FROM t HAVING COUNT(*) > 3;")
            .unwrap();
        match false_result {
            ExecuteResult::Select(q) => {
                assert!(q.rows.is_empty());
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_having_without_group_by_non_aggregate_errors() {
        let path = temp_db_path("having_non_aggregate_error");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2);").unwrap();

        let err = db.execute("SELECT 1 FROM t HAVING 1;").unwrap_err();
        assert!(err.contains("HAVING clause on a non-aggregate query"));

        cleanup(&path);
    }

    #[test]
    fn select_group_by_rejects_aggregate_expression() {
        let path = temp_db_path("group_by_aggregate_expr_error");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2);").unwrap();

        let err = db
            .execute("SELECT COUNT(*) FROM t GROUP BY COUNT(*);")
            .unwrap_err();
        assert!(err.contains("aggregate functions are not allowed in GROUP BY"));

        cleanup(&path);
    }

    #[test]
    fn select_group_by_without_from_uses_single_scalar_row() {
        let path = temp_db_path("group_by_without_from");
        let mut db = Database::open(&path).unwrap();

        let result = db.execute("SELECT 2 + 2 GROUP BY 2 + 2;").unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(q.rows, vec![vec![Value::Integer(4)]]);
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
    fn create_multi_column_index_backfills_existing_rows() {
        let path = temp_db_path("index_backfill_multi_column");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER, a INTEGER, b TEXT);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 7, 'x'), (2, 7, 'y'), (3, 7, 'x');")
            .unwrap();

        let result = db.execute("CREATE INDEX idx_t_a_b ON t(a, b);").unwrap();
        assert_eq!(result, ExecuteResult::CreateIndex);

        assert_eq!(
            indexed_rowids_for_values(
                &mut db,
                "idx_t_a_b",
                &[Value::Integer(7), Value::Text("x".to_string())],
            ),
            vec![1, 3]
        );
        assert_eq!(
            indexed_rowids_for_values(
                &mut db,
                "idx_t_a_b",
                &[Value::Integer(7), Value::Text("y".to_string())],
            ),
            vec![2]
        );

        cleanup(&path);
    }

    #[test]
    fn insert_updates_multi_column_index() {
        let path = temp_db_path("index_insert_multi_column");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, age INTEGER, city TEXT);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_age_city ON users(age, city);")
            .unwrap();
        db.execute(
            "INSERT INTO users VALUES (1, 30, 'austin'), (2, 30, 'austin'), (3, 30, 'dallas');",
        )
        .unwrap();

        assert_eq!(
            indexed_rowids_for_values(
                &mut db,
                "idx_users_age_city",
                &[Value::Integer(30), Value::Text("austin".to_string())],
            ),
            vec![1, 2]
        );
        assert_eq!(
            indexed_rowids_for_values(
                &mut db,
                "idx_users_age_city",
                &[Value::Integer(30), Value::Text("dallas".to_string())],
            ),
            vec![3]
        );

        cleanup(&path);
    }

    #[test]
    fn create_unique_multi_column_index_rejects_existing_duplicates() {
        let path = temp_db_path("create_unique_index_multi_column_duplicates");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, first TEXT, last TEXT);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'a', 'x'), (2, 'a', 'x');")
            .unwrap();

        let err = db
            .execute("CREATE UNIQUE INDEX ux_users_name ON users(first, last);")
            .unwrap_err();
        assert!(err.contains("UNIQUE constraint failed: users.first, users.last"));
        assert!(!db
            .indexes
            .contains_key(&normalize_identifier("ux_users_name")));

        cleanup(&path);
    }

    #[test]
    fn multi_column_unique_allows_null_values() {
        let path = temp_db_path("unique_multi_column_allows_nulls");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, a INTEGER, b INTEGER);")
            .unwrap();
        db.execute("CREATE UNIQUE INDEX ux_users_a_b ON users(a, b);")
            .unwrap();
        db.execute(
            "INSERT INTO users VALUES (1, 10, NULL), (2, 10, NULL), (3, NULL, 5), (4, NULL, 5);",
        )
        .unwrap();

        let selected = db
            .execute(
                "SELECT COUNT(*) FROM users WHERE (a = 10 AND b IS NULL) OR (a IS NULL AND b = 5);",
            )
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => assert_eq!(q.rows, vec![vec![Value::Integer(4)]]),
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn update_rejects_duplicate_for_multi_column_unique_index() {
        let path = temp_db_path("update_unique_multi_column_violation");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, a INTEGER, b INTEGER);")
            .unwrap();
        db.execute("CREATE UNIQUE INDEX ux_users_a_b ON users(a, b);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 1, 1), (2, 1, 2);")
            .unwrap();

        let err = db
            .execute("UPDATE users SET b = 1 WHERE id = 2;")
            .unwrap_err();
        assert!(err.contains("UNIQUE constraint failed: users.a, users.b"));

        let selected = db
            .execute("SELECT id, a, b FROM users ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(1), Value::Integer(1), Value::Integer(1)],
                        vec![Value::Integer(2), Value::Integer(1), Value::Integer(2)],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn create_unique_index_rejects_existing_duplicates() {
        let path = temp_db_path("create_unique_index_duplicates");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, email TEXT);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'a@x'), (2, 'a@x');")
            .unwrap();

        let err = db
            .execute("CREATE UNIQUE INDEX ux_users_email ON users(email);")
            .unwrap_err();
        assert!(err.contains("UNIQUE constraint failed: users.email"));
        assert!(!db
            .indexes
            .contains_key(&normalize_identifier("ux_users_email")));

        let selected = db
            .execute("SELECT id, email FROM users ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(1), Value::Text("a@x".to_string())],
                        vec![Value::Integer(2), Value::Text("a@x".to_string())],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn insert_rejects_duplicate_value_for_unique_index() {
        let path = temp_db_path("insert_unique_violation");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, email TEXT);")
            .unwrap();
        db.execute("CREATE UNIQUE INDEX ux_users_email ON users(email);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'a@x');").unwrap();

        let err = db
            .execute("INSERT INTO users VALUES (2, 'a@x');")
            .unwrap_err();
        assert!(err.contains("UNIQUE constraint failed: users.email"));

        let selected = db
            .execute("SELECT id, email FROM users ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![Value::Integer(1), Value::Text("a@x".to_string())]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn unique_index_allows_multiple_null_values() {
        let path = temp_db_path("unique_allows_nulls");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, email TEXT);")
            .unwrap();
        db.execute("CREATE UNIQUE INDEX ux_users_email ON users(email);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, NULL), (2, NULL);")
            .unwrap();

        let selected = db
            .execute("SELECT COUNT(*) FROM users WHERE email IS NULL;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => assert_eq!(q.rows, vec![vec![Value::Integer(2)]]),
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn update_rejects_duplicate_for_unique_index_without_partial_changes() {
        let path = temp_db_path("update_unique_violation");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, email TEXT);")
            .unwrap();
        db.execute("CREATE UNIQUE INDEX ux_users_email ON users(email);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'a@x'), (2, 'b@x');")
            .unwrap();

        let err = db
            .execute("UPDATE users SET email = 'a@x' WHERE id = 2;")
            .unwrap_err();
        assert!(err.contains("UNIQUE constraint failed: users.email"));

        let selected = db
            .execute("SELECT id, email FROM users ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(1), Value::Text("a@x".to_string())],
                        vec![Value::Integer(2), Value::Text("b@x".to_string())],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn update_rejects_statement_that_creates_duplicate_unique_values() {
        let path = temp_db_path("update_unique_statement_violation");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, email TEXT);")
            .unwrap();
        db.execute("CREATE UNIQUE INDEX ux_users_email ON users(email);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'a@x'), (2, 'b@x');")
            .unwrap();

        let err = db.execute("UPDATE users SET email = 'z@x';").unwrap_err();
        assert!(err.contains("UNIQUE constraint failed: users.email"));

        let selected = db
            .execute("SELECT id, email FROM users ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(1), Value::Text("a@x".to_string())],
                        vec![Value::Integer(2), Value::Text("b@x".to_string())],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn update_allows_unique_value_handoff_when_prior_row_moves_away() {
        let path = temp_db_path("update_unique_handoff");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, code INTEGER);")
            .unwrap();
        db.execute("CREATE UNIQUE INDEX ux_users_code ON users(code);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 1), (2, 2);")
            .unwrap();

        let updated = db
            .execute("UPDATE users SET code = code + (id = 1) * 2 - (id = 2);")
            .unwrap();
        assert_eq!(updated, ExecuteResult::Update { rows_affected: 2 });

        let selected = db
            .execute("SELECT id, code FROM users ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(1), Value::Integer(3)],
                        vec![Value::Integer(2), Value::Integer(1)],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn drop_table_removes_table_indexes_and_reclaims_pages() {
        let path = temp_db_path("drop_table_reclaim");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, age INTEGER);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 30), (2, 20), (3, 30);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_age ON users(age);")
            .unwrap();

        let freelist_before = db.pager.header().freelist_count;
        let dropped = db.execute("DROP TABLE users;").unwrap();
        assert_eq!(dropped, ExecuteResult::DropTable);
        assert!(db
            .execute("SELECT * FROM users;")
            .unwrap_err()
            .contains("no such table"));
        assert!(!db.indexes.contains_key("idx_users_age"));
        assert!(db.pager.header().freelist_count > freelist_before);

        assert_eq!(
            db.execute("CREATE TABLE users (id INTEGER, age INTEGER);")
                .unwrap(),
            ExecuteResult::CreateTable
        );
        assert_eq!(
            db.execute("CREATE INDEX idx_users_age ON users(age);")
                .unwrap(),
            ExecuteResult::CreateIndex
        );

        cleanup(&path);
    }

    #[test]
    fn drop_table_if_exists_is_noop_for_missing_table() {
        let path = temp_db_path("drop_table_if_exists");
        let mut db = Database::open(&path).unwrap();

        assert_eq!(
            db.execute("DROP TABLE IF EXISTS missing;").unwrap(),
            ExecuteResult::DropTable
        );

        let err = db.execute("DROP TABLE missing;").unwrap_err();
        assert!(err.contains("no such table"));

        cleanup(&path);
    }

    #[test]
    fn drop_index_removes_index_and_reclaims_pages() {
        let path = temp_db_path("drop_index_reclaim");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, age INTEGER);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 30), (2, 20), (3, 30);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_age ON users(age);")
            .unwrap();

        let freelist_before = db.pager.header().freelist_count;
        let dropped = db.execute("DROP INDEX idx_users_age;").unwrap();
        assert_eq!(dropped, ExecuteResult::DropIndex);
        assert!(!db.indexes.contains_key("idx_users_age"));
        assert!(db.pager.header().freelist_count > freelist_before);

        let selected = db
            .execute("SELECT id FROM users WHERE age = 30 ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        assert_eq!(
            db.execute("CREATE INDEX idx_users_age ON users(age);")
                .unwrap(),
            ExecuteResult::CreateIndex
        );

        cleanup(&path);
    }

    #[test]
    fn drop_index_if_exists_is_noop_for_missing_index() {
        let path = temp_db_path("drop_index_if_exists");
        let mut db = Database::open(&path).unwrap();

        assert_eq!(
            db.execute("DROP INDEX IF EXISTS missing_idx;").unwrap(),
            ExecuteResult::DropIndex
        );

        let err = db.execute("DROP INDEX missing_idx;").unwrap_err();
        assert!(err.contains("no such index"));

        cleanup(&path);
    }

    #[test]
    fn table_catalog_persists_across_reopen() {
        let path = temp_db_path("table_catalog_reopen");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE users (id INTEGER, name TEXT);")
                .unwrap();
            db.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob');")
                .unwrap();
        }

        let mut reopened = Database::open(&path).unwrap();
        let result = reopened
            .execute("SELECT id, name FROM users ORDER BY id;")
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
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
    fn index_catalog_persists_across_reopen() {
        let path = temp_db_path("index_catalog_reopen");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE users (id INTEGER, age INTEGER);")
                .unwrap();
            db.execute("INSERT INTO users VALUES (1, 30), (2, 20), (3, 30);")
                .unwrap();
            db.execute("CREATE INDEX idx_users_age ON users(age);")
                .unwrap();
        }

        let mut reopened = Database::open(&path).unwrap();
        let selected = reopened
            .execute("SELECT id FROM users WHERE age = 30 ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        assert_eq!(
            indexed_rowids(&mut reopened, "idx_users_age", &Value::Integer(30)),
            vec![1, 3]
        );
        assert_eq!(
            indexed_rowids(&mut reopened, "idx_users_age", &Value::Integer(20)),
            vec![2]
        );

        cleanup(&path);
    }

    #[test]
    fn unique_index_constraint_persists_across_reopen() {
        let path = temp_db_path("unique_index_reopen");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE users (id INTEGER, email TEXT);")
                .unwrap();
            db.execute("CREATE UNIQUE INDEX ux_users_email ON users(email);")
                .unwrap();
            db.execute("INSERT INTO users VALUES (1, 'a@x');").unwrap();
        }

        let mut reopened = Database::open(&path).unwrap();
        let err = reopened
            .execute("INSERT INTO users VALUES (2, 'a@x');")
            .unwrap_err();
        assert!(err.contains("UNIQUE constraint failed: users.email"));

        let selected = reopened
            .execute("SELECT id, email FROM users ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![Value::Integer(1), Value::Text("a@x".to_string())]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn unique_multi_column_index_constraint_persists_across_reopen() {
        let path = temp_db_path("unique_multi_column_index_reopen");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE users (id INTEGER, first TEXT, last TEXT);")
                .unwrap();
            db.execute("CREATE UNIQUE INDEX ux_users_name ON users(first, last);")
                .unwrap();
            db.execute("INSERT INTO users VALUES (1, 'a', 'x');")
                .unwrap();
        }

        let mut reopened = Database::open(&path).unwrap();
        let err = reopened
            .execute("INSERT INTO users VALUES (2, 'a', 'x');")
            .unwrap_err();
        assert!(err.contains("UNIQUE constraint failed: users.first, users.last"));

        let selected = reopened
            .execute("SELECT id, first, last FROM users ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![
                        Value::Integer(1),
                        Value::Text("a".to_string()),
                        Value::Text("x".to_string()),
                    ]]
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
    fn update_maintains_secondary_index_entries() {
        let path = temp_db_path("index_update_maintenance");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, age INTEGER);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_age ON users(age);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 30), (2, 30), (3, 42);")
            .unwrap();

        let updated = db
            .execute("UPDATE users SET age = 31 WHERE id = 2;")
            .unwrap();
        assert_eq!(updated, ExecuteResult::Update { rows_affected: 1 });

        assert_eq!(
            indexed_rowids(&mut db, "idx_users_age", &Value::Integer(30)),
            vec![1]
        );
        assert_eq!(
            indexed_rowids(&mut db, "idx_users_age", &Value::Integer(31)),
            vec![2]
        );
        assert_eq!(
            indexed_rowids(&mut db, "idx_users_age", &Value::Integer(42)),
            vec![3]
        );

        let selected = db.execute("SELECT id FROM users WHERE age = 31;").unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(q.rows, vec![vec![Value::Integer(2)]]);
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn delete_maintains_secondary_index_entries() {
        let path = temp_db_path("index_delete_maintenance");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, age INTEGER);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_age ON users(age);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 30), (2, 30), (3, 42);")
            .unwrap();

        let deleted = db.execute("DELETE FROM users WHERE id = 1;").unwrap();
        assert_eq!(deleted, ExecuteResult::Delete { rows_affected: 1 });

        assert_eq!(
            indexed_rowids(&mut db, "idx_users_age", &Value::Integer(30)),
            vec![2]
        );
        assert_eq!(
            indexed_rowids(&mut db, "idx_users_age", &Value::Integer(42)),
            vec![3]
        );

        let selected = db.execute("SELECT id FROM users WHERE age = 30;").unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(q.rows, vec![vec![Value::Integer(2)]]);
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

    #[test]
    fn update_uses_index_for_where_predicate() {
        let path = temp_db_path("update_index_selection");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, name TEXT, score INTEGER);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_score ON users(score);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'cara', 10);")
            .unwrap();

        // UPDATE with WHERE on indexed column — planner should use IndexEq
        let result = db
            .execute("UPDATE users SET name = 'updated' WHERE score = 10;")
            .unwrap();
        assert_eq!(result, ExecuteResult::Update { rows_affected: 2 });

        let selected = db
            .execute("SELECT id, name, score FROM users ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![
                            Value::Integer(1),
                            Value::Text("updated".to_string()),
                            Value::Integer(10),
                        ],
                        vec![
                            Value::Integer(2),
                            Value::Text("bob".to_string()),
                            Value::Integer(20),
                        ],
                        vec![
                            Value::Integer(3),
                            Value::Text("updated".to_string()),
                            Value::Integer(10),
                        ],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        // Verify index is still consistent after update
        assert_eq!(
            indexed_rowids(&mut db, "idx_users_score", &Value::Integer(10)),
            vec![1, 3]
        );
        assert_eq!(
            indexed_rowids(&mut db, "idx_users_score", &Value::Integer(20)),
            vec![2]
        );

        cleanup(&path);
    }

    #[test]
    fn select_plans_multi_column_index_for_matching_equalities() {
        let path = temp_db_path("select_multi_column_index_selection");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, score INTEGER, age INTEGER);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_score_age ON users(score, age);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 10, 20), (2, 10, 21), (3, 10, 20), (4, 11, 20);")
            .unwrap();

        let stmt = match ralph_parser::parse(
            "SELECT id FROM users WHERE age = 20 AND score = 10 ORDER BY id;",
        )
        .unwrap()
        {
            Stmt::Select(stmt) => stmt,
            other => panic!("expected SELECT statement, got {other:?}"),
        };
        let planner_indexes = db.planner_indexes_for_table(&normalize_identifier("users"));
        let access_path = plan_select(&stmt, "users", &planner_indexes).access_path;
        assert_eq!(
            access_path,
            AccessPath::IndexEq {
                index_name: "idx_users_score_age".to_string(),
                columns: vec!["score".to_string(), "age".to_string()],
                value_exprs: vec![Expr::IntegerLiteral(10), Expr::IntegerLiteral(20)],
            }
        );

        let selected = db
            .execute("SELECT id FROM users WHERE age = 20 AND score = 10 ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn update_uses_multi_column_index_for_where_predicate() {
        let path = temp_db_path("update_multi_column_index_selection");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, label TEXT, score INTEGER, age INTEGER);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_score_age ON users(score, age);")
            .unwrap();
        db.execute(
            "INSERT INTO users VALUES (1, 'a', 10, 20), (2, 'b', 10, 21), (3, 'c', 10, 20), (4, 'd', 11, 20);",
        )
        .unwrap();

        let result = db
            .execute("UPDATE users SET label = 'hit' WHERE age = 20 AND score = 10;")
            .unwrap();
        assert_eq!(result, ExecuteResult::Update { rows_affected: 2 });

        let selected = db
            .execute("SELECT id, label FROM users ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(1), Value::Text("hit".to_string())],
                        vec![Value::Integer(2), Value::Text("b".to_string())],
                        vec![Value::Integer(3), Value::Text("hit".to_string())],
                        vec![Value::Integer(4), Value::Text("d".to_string())],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn delete_uses_index_for_where_predicate() {
        let path = temp_db_path("delete_index_selection");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, name TEXT, score INTEGER);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_score ON users(score);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'cara', 10);")
            .unwrap();

        // DELETE with WHERE on indexed column — planner should use IndexEq
        let result = db.execute("DELETE FROM users WHERE score = 10;").unwrap();
        assert_eq!(result, ExecuteResult::Delete { rows_affected: 2 });

        let selected = db.execute("SELECT id, name, score FROM users;").unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![
                        Value::Integer(2),
                        Value::Text("bob".to_string()),
                        Value::Integer(20),
                    ]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        // Verify index entries were cleaned up
        let key = index_key_for_value(&Value::Integer(10)).unwrap();
        let mut idx_tree = BTree::new(
            &mut db.pager,
            db.indexes
                .get(&normalize_identifier("idx_users_score"))
                .unwrap()
                .root_page,
        );
        assert!(idx_tree.lookup(key).unwrap().is_none());

        assert_eq!(
            indexed_rowids(&mut db, "idx_users_score", &Value::Integer(20)),
            vec![2]
        );

        cleanup(&path);
    }

    #[test]
    fn select_supports_index_or_predicates() {
        let path = temp_db_path("select_index_or");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, score INTEGER, age INTEGER);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_score ON users(score);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_age ON users(age);")
            .unwrap();
        db.execute(
            "INSERT INTO users VALUES (1, 10, 20), (2, 20, 30), (3, 10, 40), (4, 30, 45), (5, 20, 50);",
        )
        .unwrap();

        let selected = db
            .execute("SELECT id FROM users WHERE score = 10 OR age > 35 ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(1)],
                        vec![Value::Integer(3)],
                        vec![Value::Integer(4)],
                        vec![Value::Integer(5)],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn update_uses_index_for_or_predicate() {
        let path = temp_db_path("update_index_or");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, score INTEGER, age INTEGER, label TEXT);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_score ON users(score);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_age ON users(age);")
            .unwrap();
        db.execute(
            "INSERT INTO users VALUES (1, 10, 20, 'a'), (2, 20, 30, 'b'), (3, 10, 40, 'c'), (4, 30, 40, 'd');",
        )
        .unwrap();

        let updated = db
            .execute("UPDATE users SET label = 'hit' WHERE score = 10 OR age = 40;")
            .unwrap();
        assert_eq!(updated, ExecuteResult::Update { rows_affected: 3 });

        let selected = db
            .execute("SELECT id, label FROM users ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(1), Value::Text("hit".to_string())],
                        vec![Value::Integer(2), Value::Text("b".to_string())],
                        vec![Value::Integer(3), Value::Text("hit".to_string())],
                        vec![Value::Integer(4), Value::Text("hit".to_string())],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn delete_uses_index_for_or_predicate() {
        let path = temp_db_path("delete_index_or");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, score INTEGER, age INTEGER);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_score ON users(score);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_age ON users(age);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 10, 20), (2, 20, 30), (3, 10, 40), (4, 30, 40);")
            .unwrap();

        let deleted = db
            .execute("DELETE FROM users WHERE score = 10 OR age = 40;")
            .unwrap();
        assert_eq!(deleted, ExecuteResult::Delete { rows_affected: 3 });

        let selected = db.execute("SELECT id FROM users;").unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(q.rows, vec![vec![Value::Integer(2)]]);
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn delete_uses_multi_column_index_for_where_predicate() {
        let path = temp_db_path("delete_multi_column_index_selection");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, score INTEGER, age INTEGER);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_score_age ON users(score, age);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 10, 20), (2, 10, 21), (3, 10, 20), (4, 11, 20);")
            .unwrap();

        let result = db
            .execute("DELETE FROM users WHERE age = 20 AND score = 10;")
            .unwrap();
        assert_eq!(result, ExecuteResult::Delete { rows_affected: 2 });

        let selected = db
            .execute("SELECT id, score, age FROM users ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(2), Value::Integer(10), Value::Integer(21)],
                        vec![Value::Integer(4), Value::Integer(11), Value::Integer(20)],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        // Remaining tuple still present in the composite index.
        assert_eq!(
            indexed_rowids_for_values(
                &mut db,
                "idx_users_score_age",
                &[Value::Integer(10), Value::Integer(21)]
            ),
            vec![2]
        );

        cleanup(&path);
    }

    #[test]
    fn select_supports_index_range_predicates() {
        let path = temp_db_path("select_index_range");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, score INTEGER);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_score ON users(score);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 10), (2, 15), (3, 25), (4, 30);")
            .unwrap();

        let selected = db
            .execute("SELECT id FROM users WHERE score >= 15 AND score < 30 ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn update_uses_index_for_range_predicate() {
        let path = temp_db_path("update_index_range");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, score INTEGER, label TEXT);")
            .unwrap();
        db.execute("CREATE INDEX idx_users_score ON users(score);")
            .unwrap();
        db.execute(
            "INSERT INTO users VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, 40, 'd');",
        )
        .unwrap();

        let updated = db
            .execute("UPDATE users SET label = 'hit' WHERE score > 15 AND score <= 30;")
            .unwrap();
        assert_eq!(updated, ExecuteResult::Update { rows_affected: 2 });

        let selected = db
            .execute("SELECT id, label FROM users ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(1), Value::Text("a".to_string())],
                        vec![Value::Integer(2), Value::Text("hit".to_string())],
                        vec![Value::Integer(3), Value::Text("hit".to_string())],
                        vec![Value::Integer(4), Value::Text("d".to_string())],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_supports_index_range_predicates_with_real_values() {
        let path = temp_db_path("select_index_range_real");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE metrics (id INTEGER, score REAL);")
            .unwrap();
        db.execute("CREATE INDEX idx_metrics_score ON metrics(score);")
            .unwrap();
        db.execute("INSERT INTO metrics VALUES (1, 1.0), (2, 1.5), (3, 2.5), (4, 3.0);")
            .unwrap();

        let selected = db
            .execute("SELECT id FROM metrics WHERE score > 1.0 AND score < 3.0 ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_supports_index_range_predicates_with_text_values() {
        let path = temp_db_path("select_index_range_text");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE words (id INTEGER, term TEXT);")
            .unwrap();
        db.execute("CREATE INDEX idx_words_term ON words(term);")
            .unwrap();
        db.execute(
            "INSERT INTO words VALUES (1, 'abcdefgh1'), (2, 'abcdefgh5'), (3, 'abcdefghz'), (4, 'abcdefgi0');",
        )
        .unwrap();

        let selected = db
            .execute(
                "SELECT id FROM words WHERE term > 'abcdefgh2' AND term < 'abcdefghz' ORDER BY id;",
            )
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(q.rows, vec![vec![Value::Integer(2)]]);
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn ordered_range_key_bounds_maps_text_values() {
        let bounds = ordered_range_key_bounds(
            Some((&Value::Text("a".to_string()), true)),
            Some((&Value::Text("z".to_string()), true)),
        )
        .unwrap();
        assert!(bounds.0 < bounds.1);
    }

    #[test]
    fn ordered_range_key_bounds_split_long_text_suffixes() {
        let bounds = ordered_range_key_bounds(
            Some((&Value::Text("abcdefgh1".to_string()), true)),
            Some((&Value::Text("abcdefghz".to_string()), true)),
        )
        .unwrap();
        assert!(bounds.0 < bounds.1);
    }

    #[test]
    fn ordered_range_key_bounds_maps_numeric_values() {
        let bounds = ordered_range_key_bounds(
            Some((&Value::Integer(10), true)),
            Some((&Value::Real(20.0), false)),
        )
        .unwrap();
        assert!(bounds.0 < bounds.1);
    }

    #[test]
    fn update_with_indexed_column_change_maintains_index() {
        let path = temp_db_path("update_indexed_col_change");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t (id INTEGER, category TEXT);")
            .unwrap();
        db.execute("CREATE INDEX idx_t_category ON t(category);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'a');")
            .unwrap();

        // Update the indexed column value via index-driven selection
        let result = db
            .execute("UPDATE t SET category = 'c' WHERE category = 'a';")
            .unwrap();
        assert_eq!(result, ExecuteResult::Update { rows_affected: 2 });

        let selected = db
            .execute("SELECT id, category FROM t ORDER BY id;")
            .unwrap();
        match selected {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(1), Value::Text("c".to_string())],
                        vec![Value::Integer(2), Value::Text("b".to_string())],
                        vec![Value::Integer(3), Value::Text("c".to_string())],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        // Verify old index entries are gone and new ones exist
        let key_a = index_key_for_value(&Value::Text("a".to_string())).unwrap();
        let mut idx_tree = BTree::new(
            &mut db.pager,
            db.indexes
                .get(&normalize_identifier("idx_t_category"))
                .unwrap()
                .root_page,
        );
        assert!(idx_tree.lookup(key_a).unwrap().is_none());
        drop(idx_tree);

        assert_eq!(
            indexed_rowids(&mut db, "idx_t_category", &Value::Text("c".to_string())),
            vec![1, 3]
        );
        assert_eq!(
            indexed_rowids(&mut db, "idx_t_category", &Value::Text("b".to_string())),
            vec![2]
        );

        cleanup(&path);
    }

    // ── JOIN tests ─────────────────────────────────────────────────────

    #[test]
    fn select_cross_join_comma_syntax() {
        let path = temp_db_path("cross_join_comma");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE a (id INTEGER, x TEXT);").unwrap();
        db.execute("CREATE TABLE b (id INTEGER, y TEXT);").unwrap();
        db.execute("INSERT INTO a VALUES (1, 'a1'), (2, 'a2');")
            .unwrap();
        db.execute("INSERT INTO b VALUES (10, 'b1'), (20, 'b2');")
            .unwrap();

        let result = db
            .execute("SELECT a.x, b.y FROM a, b ORDER BY a.x, b.y;")
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(q.rows.len(), 4); // 2 x 2 = 4
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Text("a1".into()), Value::Text("b1".into())],
                        vec![Value::Text("a1".into()), Value::Text("b2".into())],
                        vec![Value::Text("a2".into()), Value::Text("b1".into())],
                        vec![Value::Text("a2".into()), Value::Text("b2".into())],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_inner_join_on() {
        let path = temp_db_path("inner_join_on");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        db.execute("CREATE TABLE orders (user_id INTEGER, product TEXT);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');")
            .unwrap();
        db.execute("INSERT INTO orders VALUES (1, 'widget'), (1, 'gadget'), (2, 'sprocket');")
            .unwrap();

        let result = db
            .execute(
                "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id ORDER BY users.name, orders.product;",
            )
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Text("alice".into()), Value::Text("gadget".into())],
                        vec![Value::Text("alice".into()), Value::Text("widget".into())],
                        vec![Value::Text("bob".into()), Value::Text("sprocket".into())],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_inner_join_with_where() {
        let path = temp_db_path("inner_join_where");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t1 (id INTEGER, val INTEGER);")
            .unwrap();
        db.execute("CREATE TABLE t2 (id INTEGER, ref_id INTEGER, label TEXT);")
            .unwrap();
        db.execute("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);")
            .unwrap();
        db.execute("INSERT INTO t2 VALUES (100, 1, 'x'), (200, 2, 'y'), (300, 3, 'z');")
            .unwrap();

        let result = db
            .execute(
                "SELECT t1.val, t2.label FROM t1 JOIN t2 ON t1.id = t2.ref_id WHERE t1.val > 15 ORDER BY t1.val;",
            )
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(20), Value::Text("y".into())],
                        vec![Value::Integer(30), Value::Text("z".into())],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_join_with_alias() {
        let path = temp_db_path("join_alias");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE employees (id INTEGER, name TEXT);")
            .unwrap();
        db.execute("CREATE TABLE departments (emp_id INTEGER, dept TEXT);")
            .unwrap();
        db.execute("INSERT INTO employees VALUES (1, 'alice'), (2, 'bob');")
            .unwrap();
        db.execute("INSERT INTO departments VALUES (1, 'eng'), (2, 'sales');")
            .unwrap();

        let result = db
            .execute(
                "SELECT e.name, d.dept FROM employees AS e JOIN departments AS d ON e.id = d.emp_id ORDER BY e.name;",
            )
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Text("alice".into()), Value::Text("eng".into())],
                        vec![Value::Text("bob".into()), Value::Text("sales".into())],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_cross_join_explicit() {
        let path = temp_db_path("cross_join_explicit");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE x (a INTEGER);").unwrap();
        db.execute("CREATE TABLE y (b INTEGER);").unwrap();
        db.execute("INSERT INTO x VALUES (1), (2);").unwrap();
        db.execute("INSERT INTO y VALUES (10), (20);").unwrap();

        let result = db
            .execute("SELECT x.a, y.b FROM x CROSS JOIN y ORDER BY x.a, y.b;")
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(q.rows.len(), 4);
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(1), Value::Integer(10)],
                        vec![Value::Integer(1), Value::Integer(20)],
                        vec![Value::Integer(2), Value::Integer(10)],
                        vec![Value::Integer(2), Value::Integer(20)],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_join_star_expands_all_columns() {
        let path = temp_db_path("join_star");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE p (id INTEGER, name TEXT);")
            .unwrap();
        db.execute("CREATE TABLE q (pid INTEGER, score INTEGER);")
            .unwrap();
        db.execute("INSERT INTO p VALUES (1, 'alice');").unwrap();
        db.execute("INSERT INTO q VALUES (1, 99);").unwrap();

        let result = db
            .execute("SELECT * FROM p JOIN q ON p.id = q.pid;")
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(q.columns.len(), 4); // id, name, pid, score
                assert_eq!(
                    q.rows,
                    vec![vec![
                        Value::Integer(1),
                        Value::Text("alice".into()),
                        Value::Integer(1),
                        Value::Integer(99),
                    ]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_join_with_limit() {
        let path = temp_db_path("join_limit");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE m (id INTEGER);").unwrap();
        db.execute("CREATE TABLE n (id INTEGER);").unwrap();
        db.execute("INSERT INTO m VALUES (1), (2), (3);").unwrap();
        db.execute("INSERT INTO n VALUES (10), (20);").unwrap();

        let result = db
            .execute("SELECT m.id, n.id FROM m, n ORDER BY m.id, n.id LIMIT 3;")
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(q.rows.len(), 3);
                assert_eq!(
                    q.rows,
                    vec![
                        vec![Value::Integer(1), Value::Integer(10)],
                        vec![Value::Integer(1), Value::Integer(20)],
                        vec![Value::Integer(2), Value::Integer(10)],
                    ]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_join_unqualified_column_resolution() {
        let path = temp_db_path("join_unqualified_col");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE s (sid INTEGER, sname TEXT);")
            .unwrap();
        db.execute("CREATE TABLE r (rid INTEGER, sid_ref INTEGER);")
            .unwrap();
        db.execute("INSERT INTO s VALUES (1, 'one');").unwrap();
        db.execute("INSERT INTO r VALUES (100, 1);").unwrap();

        // sname is unambiguous — only in table s
        let result = db
            .execute("SELECT sname, rid FROM s JOIN r ON s.sid = r.sid_ref;")
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![Value::Text("one".into()), Value::Integer(100)]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_three_way_join() {
        let path = temp_db_path("three_way_join");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE t1 (id INTEGER, val TEXT);")
            .unwrap();
        db.execute("CREATE TABLE t2 (t1id INTEGER, extra INTEGER);")
            .unwrap();
        db.execute("CREATE TABLE t3 (t2extra INTEGER, label TEXT);")
            .unwrap();
        db.execute("INSERT INTO t1 VALUES (1, 'hello');").unwrap();
        db.execute("INSERT INTO t2 VALUES (1, 42);").unwrap();
        db.execute("INSERT INTO t3 VALUES (42, 'found');").unwrap();

        let result = db
            .execute(
                "SELECT t1.val, t3.label FROM t1 JOIN t2 ON t1.id = t2.t1id JOIN t3 ON t2.extra = t3.t2extra;",
            )
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![
                        Value::Text("hello".into()),
                        Value::Text("found".into()),
                    ]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_join_group_by_and_having() {
        let path = temp_db_path("join_group_by_having");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        db.execute("CREATE TABLE orders (user_id INTEGER, total INTEGER);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob');")
            .unwrap();
        db.execute("INSERT INTO orders VALUES (1, 10), (1, 15), (2, 7);")
            .unwrap();

        let result = db
            .execute(
                "SELECT u.name, COUNT(*), SUM(o.total) \
                 FROM users AS u JOIN orders AS o ON u.id = o.user_id \
                 GROUP BY u.name HAVING COUNT(*) > 1 ORDER BY u.name;",
            )
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(
                    q.rows,
                    vec![vec![
                        Value::Text("alice".into()),
                        Value::Integer(2),
                        Value::Integer(25),
                    ]]
                );
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_join_aggregate_without_group_by() {
        let path = temp_db_path("join_aggregate_no_group");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        db.execute("CREATE TABLE orders (user_id INTEGER, total INTEGER);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob');")
            .unwrap();
        db.execute("INSERT INTO orders VALUES (1, 10), (1, 15), (2, 7);")
            .unwrap();

        let result = db
            .execute(
                "SELECT COUNT(*), SUM(o.total) \
                 FROM users AS u JOIN orders AS o ON u.id = o.user_id \
                 WHERE o.total >= 10;",
            )
            .unwrap();
        match result {
            ExecuteResult::Select(q) => {
                assert_eq!(q.rows, vec![vec![Value::Integer(2), Value::Integer(25)]]);
            }
            _ => panic!("expected SELECT result"),
        }

        cleanup(&path);
    }

    #[test]
    fn select_join_aggregate_without_group_by_rejects_bare_column() {
        let path = temp_db_path("join_aggregate_bare_column_error");
        let mut db = Database::open(&path).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, name TEXT);")
            .unwrap();
        db.execute("CREATE TABLE orders (user_id INTEGER, total INTEGER);")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'alice');")
            .unwrap();
        db.execute("INSERT INTO orders VALUES (1, 10);").unwrap();

        let err = db
            .execute(
                "SELECT u.name, COUNT(*) \
                 FROM users AS u JOIN orders AS o ON u.id = o.user_id;",
            )
            .unwrap_err();
        assert!(err.contains("without GROUP BY"));

        cleanup(&path);
    }
}
