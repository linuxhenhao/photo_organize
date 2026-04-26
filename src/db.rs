use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

pub const FEATURE_VERSION: i64 = 3;

pub fn open_scan_db(path: impl AsRef<Path>) -> Result<Connection> {
    let conn = Connection::open(path.as_ref())
        .with_context(|| format!("open scan db {}", path.as_ref().display()))?;
    configure(&conn)?;
    init_scan_schema(&conn)?;
    Ok(conn)
}

pub fn open_catalog_db(path: impl AsRef<Path>) -> Result<Connection> {
    let conn = Connection::open(path.as_ref())
        .with_context(|| format!("open catalog db {}", path.as_ref().display()))?;
    configure(&conn)?;
    init_catalog_schema(&conn)?;
    Ok(conn)
}

fn configure(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "busy_timeout", 5000_i64)?;
    Ok(())
}

fn init_scan_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS source_items (
            id INTEGER PRIMARY KEY,
            source_path TEXT UNIQUE NOT NULL,
            size_bytes INTEGER NOT NULL,
            mime_type TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            exact_hash TEXT NOT NULL DEFAULT '',
            phash TEXT NOT NULL DEFAULT '',
            phash_bits INTEGER NOT NULL DEFAULT 0,
            width INTEGER NOT NULL DEFAULT 0,
            height INTEGER NOT NULL DEFAULT 0,
            scan_status TEXT NOT NULL,
            last_scanned_at TEXT NOT NULL,
            meta_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_source_items_exact_hash ON source_items(exact_hash);
        "#,
    )?;
    Ok(())
}

fn init_catalog_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS target_items (
            id INTEGER PRIMARY KEY,
            target_path TEXT UNIQUE NOT NULL,
            size_bytes INTEGER NOT NULL,
            mime_type TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            exact_hash TEXT NOT NULL DEFAULT '',
            phash TEXT NOT NULL DEFAULT '',
            phash_bits INTEGER NOT NULL DEFAULT 0,
            width INTEGER NOT NULL DEFAULT 0,
            height INTEGER NOT NULL DEFAULT 0,
            group_id INTEGER,
            keep_state TEXT NOT NULL DEFAULT 'undecided',
            is_group_primary INTEGER NOT NULL DEFAULT 0,
            group_status TEXT NOT NULL DEFAULT 'pending',
            origin_source_id INTEGER,
            meta_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS operations_log (
            id INTEGER PRIMARY KEY,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS feature_cache (
            exact_hash TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            akaze_keypoints INTEGER,
            akaze_descriptors BLOB,
            feature_version INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (exact_hash, size_bytes)
        );
        CREATE INDEX IF NOT EXISTS idx_target_items_group_id ON target_items(group_id);
        CREATE INDEX IF NOT EXISTS idx_target_items_phash ON target_items(phash);
        "#,
    )?;
    Ok(())
}

pub fn now_string() -> String {
    chrono::Utc::now().to_rfc3339()
}

pub fn insert_operation(conn: &Connection, kind: &str, payload_json: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO operations_log (kind, payload_json, created_at) VALUES (?1, ?2, ?3)",
        params![kind, payload_json, now_string()],
    )?;
    Ok(())
}

pub fn max_group_id(conn: &Connection) -> Result<i64> {
    let id = conn
        .query_row(
            "SELECT COALESCE(MAX(group_id), 0) FROM target_items WHERE group_id IS NOT NULL",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .unwrap_or(0);
    Ok(id)
}
