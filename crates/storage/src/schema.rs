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
        column_name: &str,
        column_index: u32,
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

        let index_root = BTree::create(pager)?;
        let entry = SchemaEntry {
            id: 0,
            object_type: ObjectType::Index,
            name: index_name.to_string(),
            table_name: table_name.to_string(),
            root_page: index_root,
            sql: sql.to_string(),
            columns: vec![ColumnInfo {
                name: column_name.to_string(),
                data_type: String::new(),
                index: column_index,
            }],
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
//   [0]       object_type: u8 (0=table, 1=index)
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
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty schema entry"));
    }

    let object_type = match data[pos] {
        0 => ObjectType::Table,
        1 => ObjectType::Index,
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

fn read_u16(data: &[u8], pos: &mut usize) -> io::Result<u16> {
    if *pos + 2 > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated schema entry"));
    }
    let val = u16::from_be_bytes(data[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(val)
}

fn read_u32(data: &[u8], pos: &mut usize) -> io::Result<u32> {
    if *pos + 4 > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated schema entry"));
    }
    let val = u32::from_be_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(val)
}

fn read_str(data: &[u8], pos: &mut usize) -> io::Result<String> {
    let len = read_u16(data, pos)? as usize;
    if *pos + len > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated string in schema entry"));
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
            "age",
            1,
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
}
