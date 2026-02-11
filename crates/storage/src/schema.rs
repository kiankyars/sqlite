//! Schema table: stores metadata about tables and indexes.
//!
//! The schema table is a B+tree (rooted at `header.schema_root`) that stores
//! one entry per database object (table, index). Each entry is keyed by a
//! sequential ID and contains a serialized `SchemaEntry`.
//!
//! This is analogous to SQLite's `sqlite_master` table.

use std::io;

use crate::btree::BTree;
use crate::pager::{PageNum, Pager};

/// Type of a schema object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectType {
    Table,
    Index,
    Stats,
}

/// A single schema entry describing a database object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaEntry {
    /// Sequential ID (used as B+tree key).
    pub id: i64,
    /// Type of the object.
    pub object_type: ObjectType,
    /// Name of the object (table or index name).
    pub name: String,
    /// For tables: the name is redundant. For indexes: the associated table name.
    pub table_name: String,
    /// Root page number of the B+tree for this object.
    pub root_page: PageNum,
    /// The SQL text used to create this object.
    pub sql: String,
    /// Column definitions (for tables).
    pub columns: Vec<ColumnInfo>,
}

/// Column metadata stored in the schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    /// Column index (0-based position in the table).
    pub index: u32,
}

/// Persisted table-level planner statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableStatsEntry {
    pub table_name: String,
    pub estimated_rows: usize,
}

/// Persisted index-level planner statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexStatsEntry {
    pub index_name: String,
    pub table_name: String,
    pub estimated_rows: usize,
    pub estimated_distinct_keys: usize,
}

const TABLE_STATS_PREFIX: &str = "table:";
const INDEX_STATS_PREFIX: &str = "index:";
const PLANNER_TABLE_STATS_SQL: &str = "planner_stats_table";
const PLANNER_INDEX_STATS_SQL: &str = "planner_stats_index";
const ESTIMATED_ROWS_FIELD: &str = "estimated_rows";
const ESTIMATED_DISTINCT_KEYS_FIELD: &str = "estimated_distinct_keys";

/// Manages the schema table.
pub struct Schema;

impl Schema {
    /// Initialize the schema table in a new database.
    /// Creates the schema B+tree and records its root in the file header.
    pub fn initialize(pager: &mut Pager) -> io::Result<PageNum> {
        let root = BTree::create(pager)?;
        pager.header_mut().schema_root = root;
        pager.flush_all()?;
        Ok(root)
    }

    /// Create a new table. Returns the root page of the new table's B+tree.
    pub fn create_table(
        pager: &mut Pager,
        table_name: &str,
        columns: &[(String, String)],
        sql: &str,
    ) -> io::Result<PageNum> {
        let schema_root = pager.header().schema_root;
        if schema_root == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "schema table not initialized",
            ));
        }

        // Check if table already exists.
        if Self::find_table(pager, table_name)?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("table '{}' already exists", table_name),
            ));
        }

        // Allocate a new B+tree root for this table.
        let table_root = BTree::create(pager)?;

        let col_infos: Vec<ColumnInfo> = columns
            .iter()
            .enumerate()
            .map(|(i, (name, dtype))| ColumnInfo {
                name: name.clone(),
                data_type: dtype.clone(),
                index: i as u32,
            })
            .collect();

        let entry = SchemaEntry {
            id: 0,
            object_type: ObjectType::Table,
            name: table_name.to_string(),
            table_name: table_name.to_string(),
            root_page: table_root,
            sql: sql.to_string(),
            columns: col_infos,
        };

        Self::insert_entry(pager, entry)?;
        Ok(table_root)
    }

    /// Create a new index. Returns the root page of the new index's B+tree.
    pub fn create_index(
        pager: &mut Pager,
        index_name: &str,
        table_name: &str,
        columns: &[(String, u32)],
        sql: &str,
    ) -> io::Result<PageNum> {
        let schema_root = pager.header().schema_root;
        if schema_root == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "schema table not initialized",
            ));
        }

        if Self::find_index(pager, index_name)?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("index '{}' already exists", index_name),
            ));
        }
        if columns.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "index must include at least one column",
            ));
        }

        let index_root = BTree::create(pager)?;
        let entry = SchemaEntry {
            id: 0,
            object_type: ObjectType::Index,
            name: index_name.to_string(),
            table_name: table_name.to_string(),
            root_page: index_root,
            sql: sql.to_string(),
            columns: columns
                .iter()
                .map(|(name, index)| ColumnInfo {
                    name: name.clone(),
                    data_type: String::new(),
                    index: *index,
                })
                .collect(),
        };

        Self::insert_entry(pager, entry)?;
        Ok(index_root)
    }

    /// Find a table's schema entry by name.
    pub fn find_table(pager: &mut Pager, table_name: &str) -> io::Result<Option<SchemaEntry>> {
        Self::find_by_name(pager, ObjectType::Table, table_name)
    }

    /// Find an index's schema entry by name.
    pub fn find_index(pager: &mut Pager, index_name: &str) -> io::Result<Option<SchemaEntry>> {
        Self::find_by_name(pager, ObjectType::Index, index_name)
    }

    /// List all tables in the schema.
    pub fn list_tables(pager: &mut Pager) -> io::Result<Vec<SchemaEntry>> {
        Self::list_by_type(pager, ObjectType::Table)
    }

    /// List all indexes in the schema.
    pub fn list_indexes(pager: &mut Pager) -> io::Result<Vec<SchemaEntry>> {
        Self::list_by_type(pager, ObjectType::Index)
    }

    /// Upsert persisted table-level planner statistics.
    pub fn upsert_table_stats(
        pager: &mut Pager,
        table_name: &str,
        estimated_rows: usize,
    ) -> io::Result<()> {
        let entry_name = table_stats_entry_name(table_name);
        let entry = SchemaEntry {
            id: 0,
            object_type: ObjectType::Stats,
            name: entry_name.clone(),
            table_name: table_name.to_string(),
            root_page: 0,
            sql: PLANNER_TABLE_STATS_SQL.to_string(),
            columns: vec![ColumnInfo {
                name: ESTIMATED_ROWS_FIELD.to_string(),
                data_type: estimated_rows.to_string(),
                index: 0,
            }],
        };
        Self::upsert_named_entry(pager, ObjectType::Stats, &entry_name, entry)
    }

    /// Upsert persisted index-level planner statistics.
    pub fn upsert_index_stats(
        pager: &mut Pager,
        index_name: &str,
        table_name: &str,
        estimated_rows: usize,
        estimated_distinct_keys: usize,
    ) -> io::Result<()> {
        let entry_name = index_stats_entry_name(index_name);
        let entry = SchemaEntry {
            id: 0,
            object_type: ObjectType::Stats,
            name: entry_name.clone(),
            table_name: table_name.to_string(),
            root_page: 0,
            sql: PLANNER_INDEX_STATS_SQL.to_string(),
            columns: vec![
                ColumnInfo {
                    name: ESTIMATED_ROWS_FIELD.to_string(),
                    data_type: estimated_rows.to_string(),
                    index: 0,
                },
                ColumnInfo {
                    name: ESTIMATED_DISTINCT_KEYS_FIELD.to_string(),
                    data_type: estimated_distinct_keys.to_string(),
                    index: 1,
                },
            ],
        };
        Self::upsert_named_entry(pager, ObjectType::Stats, &entry_name, entry)
    }

    /// List persisted table-level planner statistics.
    pub fn list_table_stats(pager: &mut Pager) -> io::Result<Vec<TableStatsEntry>> {
        let stats_entries = Self::list_by_type(pager, ObjectType::Stats)?;
        let mut table_stats = Vec::new();

        for entry in stats_entries {
            if entry.sql != PLANNER_TABLE_STATS_SQL || !entry.name.starts_with(TABLE_STATS_PREFIX) {
                continue;
            }
            let estimated_rows = parse_usize_field(&entry.columns, ESTIMATED_ROWS_FIELD)?;
            table_stats.push(TableStatsEntry {
                table_name: entry.table_name,
                estimated_rows,
            });
        }

        Ok(table_stats)
    }

    /// List persisted index-level planner statistics.
    pub fn list_index_stats(pager: &mut Pager) -> io::Result<Vec<IndexStatsEntry>> {
        let stats_entries = Self::list_by_type(pager, ObjectType::Stats)?;
        let mut index_stats = Vec::new();

        for entry in stats_entries {
            if entry.sql != PLANNER_INDEX_STATS_SQL || !entry.name.starts_with(INDEX_STATS_PREFIX) {
                continue;
            }
            let estimated_rows = parse_usize_field(&entry.columns, ESTIMATED_ROWS_FIELD)?;
            let estimated_distinct_keys =
                parse_usize_field(&entry.columns, ESTIMATED_DISTINCT_KEYS_FIELD)?;
            let index_name = entry
                .name
                .strip_prefix(INDEX_STATS_PREFIX)
                .unwrap_or_default()
                .to_string();
            index_stats.push(IndexStatsEntry {
                index_name,
                table_name: entry.table_name,
                estimated_rows,
                estimated_distinct_keys,
            });
        }

        Ok(index_stats)
    }

    /// Remove persisted table-level planner statistics.
    pub fn drop_table_stats(pager: &mut Pager, table_name: &str) -> io::Result<bool> {
        let entry_name = table_stats_entry_name(table_name);
        Ok(Self::delete_by_name(pager, ObjectType::Stats, &entry_name)?.is_some())
    }

    /// Remove persisted index-level planner statistics.
    pub fn drop_index_stats(pager: &mut Pager, index_name: &str) -> io::Result<bool> {
        let entry_name = index_stats_entry_name(index_name);
        Ok(Self::delete_by_name(pager, ObjectType::Stats, &entry_name)?.is_some())
    }

    /// List indexes associated with the given table.
    pub fn list_indexes_for_table(
        pager: &mut Pager,
        table_name: &str,
    ) -> io::Result<Vec<SchemaEntry>> {
        let indexes = Self::list_indexes(pager)?;
        Ok(indexes
            .into_iter()
            .filter(|entry| entry.table_name.eq_ignore_ascii_case(table_name))
            .collect())
    }

    /// Remove a table entry from the schema and return the removed metadata.
    pub fn drop_table(pager: &mut Pager, table_name: &str) -> io::Result<Option<SchemaEntry>> {
        Self::delete_by_name(pager, ObjectType::Table, table_name)
    }

    /// Remove an index entry from the schema and return the removed metadata.
    pub fn drop_index(pager: &mut Pager, index_name: &str) -> io::Result<Option<SchemaEntry>> {
        Self::delete_by_name(pager, ObjectType::Index, index_name)
    }

    fn insert_entry(pager: &mut Pager, mut entry: SchemaEntry) -> io::Result<()> {
        let new_id = Self::next_id(pager)?;
        entry.id = new_id;
        let payload = serialize_entry(&entry);

        // Reload schema_root since create may have changed page allocations.
        let schema_root = pager.header().schema_root;
        let mut tree = BTree::new(pager, schema_root);
        tree.insert(new_id, &payload)?;

        // Update the schema root in case it changed (due to splits).
        let new_schema_root = tree.root_page();
        pager.header_mut().schema_root = new_schema_root;
        Ok(())
    }

    fn upsert_named_entry(
        pager: &mut Pager,
        object_type: ObjectType,
        name: &str,
        entry: SchemaEntry,
    ) -> io::Result<()> {
        if let Some(existing) = Self::find_by_name(pager, object_type, name)? {
            Self::delete_by_id(pager, existing.id)?;
        }
        Self::insert_entry(pager, entry)
    }

    fn find_by_name(
        pager: &mut Pager,
        object_type: ObjectType,
        name: &str,
    ) -> io::Result<Option<SchemaEntry>> {
        let entries = Self::list_entries(pager)?;
        Ok(entries.into_iter().find(|entry| {
            entry.object_type == object_type && entry.name.eq_ignore_ascii_case(name)
        }))
    }

    fn list_by_type(pager: &mut Pager, object_type: ObjectType) -> io::Result<Vec<SchemaEntry>> {
        let entries = Self::list_entries(pager)?;
        Ok(entries
            .into_iter()
            .filter(|entry| entry.object_type == object_type)
            .collect())
    }

    fn list_entries(pager: &mut Pager) -> io::Result<Vec<SchemaEntry>> {
        let schema_root = pager.header().schema_root;
        if schema_root == 0 {
            return Ok(Vec::new());
        }

        let mut tree = BTree::new(pager, schema_root);
        let records = tree.scan_all()?;

        let mut entries = Vec::with_capacity(records.len());
        for record in records {
            let mut schema_entry = deserialize_entry(&record.payload)?;
            schema_entry.id = record.key;
            entries.push(schema_entry);
        }
        Ok(entries)
    }

    fn delete_by_name(
        pager: &mut Pager,
        object_type: ObjectType,
        name: &str,
    ) -> io::Result<Option<SchemaEntry>> {
        let Some(entry) = Self::find_by_name(pager, object_type, name)? else {
            return Ok(None);
        };

        Self::delete_by_id(pager, entry.id)?;
        Ok(Some(entry))
    }

    fn delete_by_id(pager: &mut Pager, entry_id: i64) -> io::Result<()> {
        let schema_root = pager.header().schema_root;
        let mut tree = BTree::new(pager, schema_root);
        let deleted = tree.delete(entry_id)?;
        if !deleted {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("schema entry id {} not found during delete", entry_id),
            ));
        }

        pager.header_mut().schema_root = tree.root_page();
        Ok(())
    }

    /// Get the next available schema entry ID.
    fn next_id(pager: &mut Pager) -> io::Result<i64> {
        let schema_root = pager.header().schema_root;
        let mut tree = BTree::new(pager, schema_root);
        let entries = tree.scan_all()?;

        let max_id = entries.iter().map(|e| e.key).max().unwrap_or(0);
        Ok(max_id + 1)
    }
}

// ─── Serialization ───────────────────────────────────────────────────────────
//
// Simple binary format for schema entries:
//   [0]       object_type: u8 (0=table, 1=index, 2=stats)
//   [1..5]    root_page: u32 (big-endian)
//   [5..7]    name_len: u16
//   [7..7+N]  name: utf-8 bytes
//   [..]      table_name_len: u16 + table_name bytes
//   [..]      sql_len: u16 + sql bytes
//   [..]      column_count: u16
//   For each column:
//     [..]    col_name_len: u16 + col_name bytes
//     [..]    col_type_len: u16 + col_type bytes
//     [..]    col_index: u32

fn serialize_entry(entry: &SchemaEntry) -> Vec<u8> {
    let mut buf = Vec::new();

    // object_type
    buf.push(match entry.object_type {
        ObjectType::Table => 0,
        ObjectType::Index => 1,
        ObjectType::Stats => 2,
    });

    // root_page
    buf.extend_from_slice(&entry.root_page.to_be_bytes());

    // name
    write_str(&mut buf, &entry.name);

    // table_name
    write_str(&mut buf, &entry.table_name);

    // sql
    write_str(&mut buf, &entry.sql);

    // columns
    buf.extend_from_slice(&(entry.columns.len() as u16).to_be_bytes());
    for col in &entry.columns {
        write_str(&mut buf, &col.name);
        write_str(&mut buf, &col.data_type);
        buf.extend_from_slice(&col.index.to_be_bytes());
    }

    buf
}

fn deserialize_entry(data: &[u8]) -> io::Result<SchemaEntry> {
    let mut pos = 0;

    if data.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty schema entry",
        ));
    }

    let object_type = match data[pos] {
        0 => ObjectType::Table,
        1 => ObjectType::Index,
        2 => ObjectType::Stats,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown object type: {}", other),
            ))
        }
    };
    pos += 1;

    let root_page = read_u32(data, &mut pos)?;
    let name = read_str(data, &mut pos)?;
    let table_name = read_str(data, &mut pos)?;
    let sql = read_str(data, &mut pos)?;

    let col_count = read_u16(data, &mut pos)? as usize;
    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let col_name = read_str(data, &mut pos)?;
        let col_type = read_str(data, &mut pos)?;
        let col_index = read_u32(data, &mut pos)?;
        columns.push(ColumnInfo {
            name: col_name,
            data_type: col_type,
            index: col_index,
        });
    }

    Ok(SchemaEntry {
        id: 0, // Will be set from the B+tree key
        object_type,
        name,
        table_name,
        root_page,
        sql,
        columns,
    })
}

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(bytes);
}

fn table_stats_entry_name(table_name: &str) -> String {
    format!("{TABLE_STATS_PREFIX}{}", table_name.to_ascii_lowercase())
}

fn index_stats_entry_name(index_name: &str) -> String {
    format!("{INDEX_STATS_PREFIX}{}", index_name.to_ascii_lowercase())
}

fn parse_usize_field(columns: &[ColumnInfo], field_name: &str) -> io::Result<usize> {
    let value = columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(field_name))
        .map(|column| column.data_type.as_str())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("missing planner stats field '{}'", field_name),
            )
        })?;

    value.parse::<usize>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "invalid planner stats value '{}' for field '{}'",
                value, field_name
            ),
        )
    })
}

fn read_u16(data: &[u8], pos: &mut usize) -> io::Result<u16> {
    if *pos + 2 > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated schema entry",
        ));
    }
    let val = u16::from_be_bytes(data[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(val)
}

fn read_u32(data: &[u8], pos: &mut usize) -> io::Result<u32> {
    if *pos + 4 > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated schema entry",
        ));
    }
    let val = u32::from_be_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(val)
}

fn read_str(data: &[u8], pos: &mut usize) -> io::Result<String> {
    let len = read_u16(data, pos)? as usize;
    if *pos + len > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated string in schema entry",
        ));
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .to_string();
    *pos += len;
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_db_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("ralph_schema_tests");
        fs::create_dir_all(&dir).ok();
        dir.join(name)
    }

    fn cleanup(path: &std::path::Path) {
        fs::remove_file(path).ok();
    }

    #[test]
    fn schema_entry_serialization_roundtrip() {
        let entry = SchemaEntry {
            id: 1,
            object_type: ObjectType::Table,
            name: "users".to_string(),
            table_name: "users".to_string(),
            root_page: 42,
            sql: "CREATE TABLE users (id INTEGER, name TEXT)".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    index: 0,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: "TEXT".to_string(),
                    index: 1,
                },
            ],
        };

        let data = serialize_entry(&entry);
        let decoded = deserialize_entry(&data).unwrap();

        assert_eq!(decoded.object_type, ObjectType::Table);
        assert_eq!(decoded.name, "users");
        assert_eq!(decoded.root_page, 42);
        assert_eq!(decoded.columns.len(), 2);
        assert_eq!(decoded.columns[0].name, "id");
        assert_eq!(decoded.columns[1].name, "name");
    }

    #[test]
    fn initialize_and_create_table() {
        let path = temp_db_path("schema_create.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        Schema::initialize(&mut pager).unwrap();

        let table_root = Schema::create_table(
            &mut pager,
            "users",
            &[
                ("id".to_string(), "INTEGER".to_string()),
                ("name".to_string(), "TEXT".to_string()),
            ],
            "CREATE TABLE users (id INTEGER, name TEXT)",
        )
        .unwrap();

        assert!(table_root > 0);

        // Should be findable.
        let entry = Schema::find_table(&mut pager, "users").unwrap().unwrap();
        assert_eq!(entry.name, "users");
        assert_eq!(entry.root_page, table_root);
        assert_eq!(entry.columns.len(), 2);

        // Nonexistent table.
        assert!(Schema::find_table(&mut pager, "posts").unwrap().is_none());

        cleanup(&path);
    }

    #[test]
    fn duplicate_table_name_rejected() {
        let path = temp_db_path("schema_dup.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        Schema::initialize(&mut pager).unwrap();

        Schema::create_table(
            &mut pager,
            "users",
            &[("id".to_string(), "INTEGER".to_string())],
            "CREATE TABLE users (id INTEGER)",
        )
        .unwrap();

        let result = Schema::create_table(
            &mut pager,
            "users",
            &[("id".to_string(), "INTEGER".to_string())],
            "CREATE TABLE users (id INTEGER)",
        );

        assert!(result.is_err());

        cleanup(&path);
    }

    #[test]
    fn list_multiple_tables() {
        let path = temp_db_path("schema_list.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        Schema::initialize(&mut pager).unwrap();

        Schema::create_table(
            &mut pager,
            "users",
            &[("id".to_string(), "INTEGER".to_string())],
            "CREATE TABLE users (id INTEGER)",
        )
        .unwrap();

        Schema::create_table(
            &mut pager,
            "posts",
            &[
                ("id".to_string(), "INTEGER".to_string()),
                ("title".to_string(), "TEXT".to_string()),
            ],
            "CREATE TABLE posts (id INTEGER, title TEXT)",
        )
        .unwrap();

        let tables = Schema::list_tables(&mut pager).unwrap();
        assert_eq!(tables.len(), 2);
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"users"));
        assert!(names.contains(&"posts"));

        cleanup(&path);
    }

    #[test]
    fn schema_persists_after_flush() {
        let path = temp_db_path("schema_persist.db");
        cleanup(&path);

        {
            let mut pager = Pager::open(&path).unwrap();
            Schema::initialize(&mut pager).unwrap();

            Schema::create_table(
                &mut pager,
                "items",
                &[
                    ("id".to_string(), "INTEGER".to_string()),
                    ("name".to_string(), "TEXT".to_string()),
                    ("price".to_string(), "REAL".to_string()),
                ],
                "CREATE TABLE items (id INTEGER, name TEXT, price REAL)",
            )
            .unwrap();

            pager.flush_all().unwrap();
        }

        {
            let mut pager = Pager::open(&path).unwrap();
            let entry = Schema::find_table(&mut pager, "items").unwrap().unwrap();
            assert_eq!(entry.name, "items");
            assert_eq!(entry.columns.len(), 3);
            assert_eq!(entry.columns[2].name, "price");
            assert_eq!(entry.columns[2].data_type, "REAL");
        }

        cleanup(&path);
    }

    #[test]
    fn schema_not_initialized_error() {
        let path = temp_db_path("schema_uninit.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();

        let result = Schema::create_table(
            &mut pager,
            "test",
            &[("id".to_string(), "INTEGER".to_string())],
            "CREATE TABLE test (id INTEGER)",
        );

        assert!(result.is_err());

        cleanup(&path);
    }

    #[test]
    fn create_and_find_index() {
        let path = temp_db_path("schema_index.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        Schema::initialize(&mut pager).unwrap();
        Schema::create_table(
            &mut pager,
            "users",
            &[
                ("id".to_string(), "INTEGER".to_string()),
                ("age".to_string(), "INTEGER".to_string()),
            ],
            "CREATE TABLE users (id INTEGER, age INTEGER)",
        )
        .unwrap();

        let index_root = Schema::create_index(
            &mut pager,
            "idx_users_age",
            "users",
            &[("age".to_string(), 1)],
            "CREATE INDEX idx_users_age ON users(age)",
        )
        .unwrap();

        let index = Schema::find_index(&mut pager, "idx_users_age")
            .unwrap()
            .unwrap();
        assert_eq!(index.root_page, index_root);
        assert_eq!(index.table_name, "users");
        assert_eq!(index.columns.len(), 1);
        assert_eq!(index.columns[0].name, "age");
        assert_eq!(index.columns[0].index, 1);

        let indexes = Schema::list_indexes(&mut pager).unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "idx_users_age");

        cleanup(&path);
    }

    #[test]
    fn planner_stats_upsert_list_and_drop() {
        let path = temp_db_path("schema_planner_stats.db");
        cleanup(&path);

        {
            let mut pager = Pager::open(&path).unwrap();
            Schema::initialize(&mut pager).unwrap();
            Schema::create_table(
                &mut pager,
                "users",
                &[
                    ("id".to_string(), "INTEGER".to_string()),
                    ("age".to_string(), "INTEGER".to_string()),
                ],
                "CREATE TABLE users (id INTEGER, age INTEGER)",
            )
            .unwrap();
            Schema::create_index(
                &mut pager,
                "idx_users_age",
                "users",
                &[("age".to_string(), 1)],
                "CREATE INDEX idx_users_age ON users(age)",
            )
            .unwrap();

            Schema::upsert_table_stats(&mut pager, "users", 7).unwrap();
            Schema::upsert_table_stats(&mut pager, "users", 9).unwrap();
            Schema::upsert_index_stats(&mut pager, "idx_users_age", "users", 9, 3).unwrap();
            Schema::upsert_index_stats(&mut pager, "idx_users_age", "users", 10, 4).unwrap();

            let table_stats = Schema::list_table_stats(&mut pager).unwrap();
            assert_eq!(table_stats.len(), 1);
            assert_eq!(table_stats[0].table_name, "users");
            assert_eq!(table_stats[0].estimated_rows, 9);

            let index_stats = Schema::list_index_stats(&mut pager).unwrap();
            assert_eq!(index_stats.len(), 1);
            assert_eq!(index_stats[0].index_name, "idx_users_age");
            assert_eq!(index_stats[0].table_name, "users");
            assert_eq!(index_stats[0].estimated_rows, 10);
            assert_eq!(index_stats[0].estimated_distinct_keys, 4);

            pager.flush_all().unwrap();
        }

        {
            let mut pager = Pager::open(&path).unwrap();
            let table_stats = Schema::list_table_stats(&mut pager).unwrap();
            assert_eq!(table_stats.len(), 1);
            assert_eq!(table_stats[0].estimated_rows, 9);

            let index_stats = Schema::list_index_stats(&mut pager).unwrap();
            assert_eq!(index_stats.len(), 1);
            assert_eq!(index_stats[0].estimated_rows, 10);
            assert_eq!(index_stats[0].estimated_distinct_keys, 4);

            assert!(Schema::drop_table_stats(&mut pager, "users").unwrap());
            assert!(!Schema::drop_table_stats(&mut pager, "users").unwrap());
            assert!(Schema::drop_index_stats(&mut pager, "idx_users_age").unwrap());
            assert!(!Schema::drop_index_stats(&mut pager, "idx_users_age").unwrap());
            assert!(Schema::list_table_stats(&mut pager).unwrap().is_empty());
            assert!(Schema::list_index_stats(&mut pager).unwrap().is_empty());
        }

        cleanup(&path);
    }

    #[test]
    fn drop_table_removes_schema_entry() {
        let path = temp_db_path("schema_drop_table.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        Schema::initialize(&mut pager).unwrap();

        let table_root = Schema::create_table(
            &mut pager,
            "users",
            &[("id".to_string(), "INTEGER".to_string())],
            "CREATE TABLE users (id INTEGER)",
        )
        .unwrap();

        let dropped = Schema::drop_table(&mut pager, "users").unwrap().unwrap();
        assert_eq!(dropped.root_page, table_root);
        assert!(Schema::find_table(&mut pager, "users").unwrap().is_none());
        assert!(Schema::drop_table(&mut pager, "users").unwrap().is_none());

        cleanup(&path);
    }

    #[test]
    fn drop_index_removes_schema_entry() {
        let path = temp_db_path("schema_drop_index.db");
        cleanup(&path);

        let mut pager = Pager::open(&path).unwrap();
        Schema::initialize(&mut pager).unwrap();
        Schema::create_table(
            &mut pager,
            "users",
            &[
                ("id".to_string(), "INTEGER".to_string()),
                ("age".to_string(), "INTEGER".to_string()),
            ],
            "CREATE TABLE users (id INTEGER, age INTEGER)",
        )
        .unwrap();
        let index_root = Schema::create_index(
            &mut pager,
            "idx_users_age",
            "users",
            &[("age".to_string(), 1)],
            "CREATE INDEX idx_users_age ON users(age)",
        )
        .unwrap();

        let indexes_for_table = Schema::list_indexes_for_table(&mut pager, "users").unwrap();
        assert_eq!(indexes_for_table.len(), 1);
        assert_eq!(indexes_for_table[0].name, "idx_users_age");

        let dropped = Schema::drop_index(&mut pager, "idx_users_age")
            .unwrap()
            .unwrap();
        assert_eq!(dropped.root_page, index_root);
        assert!(Schema::find_index(&mut pager, "idx_users_age")
            .unwrap()
            .is_none());
        assert!(Schema::drop_index(&mut pager, "idx_users_age")
            .unwrap()
            .is_none());

        cleanup(&path);
    }
}
