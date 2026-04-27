use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

pub const FEATURE_VERSION: i64 = 6;

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
            akaze_status TEXT NOT NULL DEFAULT 'pending',
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
    migrate_feature_cache_schema(conn)?;
    Ok(())
}

fn migrate_feature_cache_schema(conn: &Connection) -> Result<()> {
    if !table_has_column(conn, "feature_cache", "akaze_status")? {
        conn.execute(
            "ALTER TABLE feature_cache ADD COLUMN akaze_status TEXT NOT NULL DEFAULT 'pending'",
            [],
        )?;
    }
    normalize_feature_cache_rows(conn)?;
    Ok(())
}

fn normalize_feature_cache_rows(conn: &Connection) -> Result<()> {
    conn.execute(
        r#"
        UPDATE feature_cache
        SET
            akaze_status = 'ready',
            feature_version = ?1
        WHERE
            akaze_descriptors IS NOT NULL
            AND (
                akaze_status IS NULL
                OR akaze_status = ''
                OR akaze_status != 'ready'
                OR feature_version < ?1
            )
        "#,
        params![FEATURE_VERSION],
    )?;

    conn.execute(
        r#"
        UPDATE feature_cache
        SET
            akaze_status = 'decode_error',
            feature_version = ?1
        WHERE
            akaze_descriptors IS NULL
            AND (
                akaze_status IS NULL
                OR akaze_status = ''
                OR akaze_status = 'pending'
                OR akaze_status = 'unavailable'
                OR (akaze_status = 'decode_error' AND feature_version < ?1)
            )
        "#,
        params![FEATURE_VERSION],
    )?;

    conn.execute(
        r#"
        UPDATE feature_cache
        SET feature_version = ?1
        WHERE
            akaze_descriptors IS NULL
            AND akaze_status = 'too_small'
            AND feature_version < ?1
        "#,
        params![FEATURE_VERSION],
    )?;

    conn.execute(
        r#"
        DELETE FROM feature_cache
        WHERE
            akaze_descriptors IS NULL
            AND akaze_status = 'no_keypoints'
            AND feature_version < ?1
        "#,
        params![FEATURE_VERSION],
    )?;
    Ok(())
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == column {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::features::serialize_akaze_descriptors;
    use tempfile::tempdir;

    #[test]
    fn open_catalog_db_migrates_legacy_feature_cache_rows() {
        let tmp = tempdir().unwrap();
        let db_path = tmp.path().join("catalog.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE feature_cache (
                exact_hash TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                akaze_keypoints INTEGER,
                akaze_descriptors BLOB,
                feature_version INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (exact_hash, size_bytes)
            );
            "#,
        )
        .unwrap();
        let ready_blob = serialize_akaze_descriptors(&[vec![1, 2, 3, 4]]).unwrap();
        conn.execute(
            "INSERT INTO feature_cache (exact_hash, size_bytes, akaze_keypoints, akaze_descriptors, feature_version, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))",
            params!["ready-hash", 10_i64, 1_i64, ready_blob, 3_i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO feature_cache (exact_hash, size_bytes, akaze_keypoints, akaze_descriptors, feature_version, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))",
            params!["stale-hash", 20_i64, Option::<i64>::None, Option::<Vec<u8>>::None, 3_i64],
        )
        .unwrap();
        drop(conn);

        let migrated = open_catalog_db(&db_path).unwrap();

        let has_status = table_has_column(&migrated, "feature_cache", "akaze_status").unwrap();
        assert!(has_status);

        let ready_row: (String, i64) = migrated
            .query_row(
                "SELECT akaze_status, feature_version FROM feature_cache WHERE exact_hash = 'ready-hash'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(ready_row.0, "ready");
        assert_eq!(ready_row.1, FEATURE_VERSION);

        let stale_row: (String, i64) = migrated
            .query_row(
                "SELECT akaze_status, feature_version FROM feature_cache WHERE exact_hash = 'stale-hash'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stale_row.0, "decode_error");
        assert_eq!(stale_row.1, FEATURE_VERSION);
    }

    #[test]
    fn open_catalog_db_reclassifies_retryable_unavailable_rows() {
        let tmp = tempdir().unwrap();
        let db_path = tmp.path().join("catalog.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE feature_cache (
                exact_hash TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                akaze_status TEXT NOT NULL DEFAULT 'pending',
                akaze_keypoints INTEGER,
                akaze_descriptors BLOB,
                feature_version INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (exact_hash, size_bytes)
            );
            "#,
        )
        .unwrap();
        conn.execute(
            "INSERT INTO feature_cache (exact_hash, size_bytes, akaze_status, akaze_keypoints, akaze_descriptors, feature_version, updated_at)
             VALUES (?1, ?2, 'unavailable', NULL, NULL, 4, datetime('now'))",
            params!["retry-hash", 10_i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO feature_cache (exact_hash, size_bytes, akaze_status, akaze_keypoints, akaze_descriptors, feature_version, updated_at)
             VALUES (?1, ?2, 'no_keypoints', NULL, NULL, 4, datetime('now'))",
            params!["nokp-hash", 11_i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO feature_cache (exact_hash, size_bytes, akaze_status, akaze_keypoints, akaze_descriptors, feature_version, updated_at)
             VALUES (?1, ?2, 'too_small', NULL, NULL, 4, datetime('now'))",
            params!["small-hash", 12_i64],
        )
        .unwrap();
        drop(conn);

        let migrated = open_catalog_db(&db_path).unwrap();

        let retry_row: (String, i64) = migrated
            .query_row(
                "SELECT akaze_status, feature_version FROM feature_cache WHERE exact_hash = 'retry-hash'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(retry_row.0, "decode_error");
        assert_eq!(retry_row.1, FEATURE_VERSION);

        let nokp_count: i64 = migrated
            .query_row(
                "SELECT COUNT(*) FROM feature_cache WHERE exact_hash = 'nokp-hash'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(nokp_count, 0);

        let too_small_row: (String, i64) = migrated
            .query_row(
                "SELECT akaze_status, feature_version FROM feature_cache WHERE exact_hash = 'small-hash'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(too_small_row.0, "too_small");
        assert_eq!(too_small_row.1, FEATURE_VERSION);
    }
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
