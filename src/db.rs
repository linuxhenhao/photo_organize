use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use std::path::Path;
use std::time::Instant;

pub const FEATURE_VERSION: i64 = 7;

pub fn open_scan_db(path: impl AsRef<Path>) -> Result<Connection> {
    let path = path.as_ref();
    let started = Instant::now();
    tracing::info!(db = %path.display(), "opening scan db");
    let conn = Connection::open(path).with_context(|| format!("open scan db {}", path.display()))?;
    configure_writable(&conn)?;
    init_scan_schema(&conn)?;
    tracing::info!(
        db = %path.display(),
        elapsed_ms = started.elapsed().as_millis(),
        "opened scan db"
    );
    Ok(conn)
}

pub fn open_catalog_db(path: impl AsRef<Path>) -> Result<Connection> {
    let path = path.as_ref();
    let total_started = Instant::now();
    tracing::info!(db = %path.display(), "opening catalog db");

    let started = Instant::now();
    let conn =
        Connection::open(path).with_context(|| format!("open catalog db {}", path.display()))?;
    tracing::info!(
        db = %path.display(),
        elapsed_ms = started.elapsed().as_millis(),
        "catalog sqlite open"
    );

    let started = Instant::now();
    configure_writable(&conn)?;
    tracing::info!(
        elapsed_ms = started.elapsed().as_millis(),
        "catalog sqlite pragmas"
    );

    let started = Instant::now();
    init_catalog_schema(&conn)?;
    tracing::info!(
        elapsed_ms = started.elapsed().as_millis(),
        "catalog schema/migrate"
    );

    tracing::info!(
        db = %path.display(),
        elapsed_ms = total_started.elapsed().as_millis(),
        "opened catalog db"
    );
    Ok(conn)
}

pub fn open_catalog_db_readonly(path: impl AsRef<Path>) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        path.as_ref(),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("open readonly catalog db {}", path.as_ref().display()))?;
    configure_readonly(&conn)?;
    Ok(conn)
}

fn configure_writable(conn: &Connection) -> Result<()> {
    let started = Instant::now();
    let current: String = conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    if !current.eq_ignore_ascii_case("wal") {
        conn.pragma_update(None, "journal_mode", "WAL")?;
    }
    tracing::info!(
        already_wal = current.eq_ignore_ascii_case("wal"),
        elapsed_ms = started.elapsed().as_millis(),
        "catalog/scan journal_mode=WAL"
    );
    conn.pragma_update(None, "busy_timeout", 5000_i64)?;
    configure_common(conn)?;
    Ok(())
}

fn configure_readonly(conn: &Connection) -> Result<()> {
    conn.busy_timeout(std::time::Duration::from_millis(5000))?;
    configure_common(conn)?;
    Ok(())
}

fn configure_common(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "foreign_keys", "ON")?;
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
    init_feature_cache_schema(conn)?;
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
        CREATE INDEX IF NOT EXISTS idx_target_items_group_id ON target_items(group_id);
        CREATE INDEX IF NOT EXISTS idx_target_items_phash ON target_items(phash);
        CREATE INDEX IF NOT EXISTS idx_target_items_exact_hash ON target_items(exact_hash);
        "#,
    )?;
    init_feature_cache_schema(conn)?;
    Ok(())
}

fn init_feature_cache_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS feature_cache (
            exact_hash TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            akaze_status TEXT NOT NULL DEFAULT 'pending',
            akaze_keypoints INTEGER,
            akaze_descriptors BLOB,
            akaze_points BLOB,
            feature_version INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (exact_hash, size_bytes)
        );
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
    if !table_has_column(conn, "feature_cache", "akaze_points")? {
        conn.execute("ALTER TABLE feature_cache ADD COLUMN akaze_points BLOB", [])?;
    }
    normalize_feature_cache_rows(conn)?;
    Ok(())
}

fn exec_timed(conn: &Connection, stage: &'static str, sql: &str, version: i64) -> Result<usize> {
    tracing::info!(stage, "feature_cache migrate start");
    let started = Instant::now();
    let rows = conn.execute(sql, params![version])?;
    tracing::info!(
        stage,
        rows,
        elapsed_ms = started.elapsed().as_millis(),
        "feature_cache migrate done"
    );
    Ok(rows)
}

fn feature_cache_needs_normalize(conn: &Connection) -> Result<bool> {
    let needs = conn
        .query_row(
            r#"
            SELECT 1 FROM feature_cache
            WHERE
                feature_version < ?1
                OR akaze_status IS NULL
                OR akaze_status = ''
                OR (
                    akaze_descriptors IS NOT NULL
                    AND akaze_points IS NOT NULL
                    AND akaze_status != 'ready'
                )
                OR (
                    akaze_descriptors IS NULL
                    AND akaze_status IN ('pending', 'unavailable', '')
                )
            LIMIT 1
            "#,
            params![FEATURE_VERSION],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    Ok(needs)
}

fn normalize_feature_cache_rows(conn: &Connection) -> Result<()> {
    tracing::info!("feature_cache migrate start");
    let started = Instant::now();
    if !feature_cache_needs_normalize(conn)? {
        tracing::info!(
            elapsed_ms = started.elapsed().as_millis(),
            "feature_cache migrate skipped (already current)"
        );
        return Ok(());
    }

    exec_timed(
        conn,
        "normalize_ready",
        r#"
        UPDATE feature_cache
        SET
            akaze_status = 'ready',
            feature_version = ?1
        WHERE
            akaze_descriptors IS NOT NULL
            AND akaze_points IS NOT NULL
            AND (
                akaze_status IS NULL
                OR akaze_status = ''
                OR akaze_status != 'ready'
                OR feature_version < ?1
            )
        "#,
        FEATURE_VERSION,
    )?;

    exec_timed(
        conn,
        "normalize_decode_error",
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
        FEATURE_VERSION,
    )?;

    exec_timed(
        conn,
        "normalize_too_small",
        r#"
        UPDATE feature_cache
        SET feature_version = ?1
        WHERE
            akaze_descriptors IS NULL
            AND akaze_status = 'too_small'
            AND feature_version < ?1
        "#,
        FEATURE_VERSION,
    )?;

    exec_timed(
        conn,
        "delete_missing_points",
        r#"
        DELETE FROM feature_cache
        WHERE
            akaze_descriptors IS NOT NULL
            AND akaze_points IS NULL
            AND feature_version < ?1
        "#,
        FEATURE_VERSION,
    )?;

    exec_timed(
        conn,
        "delete_legacy_no_keypoints",
        r#"
        DELETE FROM feature_cache
        WHERE
            akaze_descriptors IS NULL
            AND akaze_status = 'no_keypoints'
            AND feature_version < ?1
        "#,
        FEATURE_VERSION,
    )?;
    tracing::info!(
        elapsed_ms = started.elapsed().as_millis(),
        "feature_cache migrate done"
    );
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
    use crate::features::{AkazePoint, serialize_akaze_descriptors, serialize_akaze_points};
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

        let ready_count: i64 = migrated
            .query_row(
                "SELECT COUNT(*) FROM feature_cache WHERE exact_hash = 'ready-hash'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ready_count, 0);

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

    #[test]
    fn open_catalog_db_keeps_geometry_aware_ready_rows() {
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
                akaze_points BLOB,
                feature_version INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (exact_hash, size_bytes)
            );
            "#,
        )
        .unwrap();
        let ready_blob = serialize_akaze_descriptors(&[vec![1, 2, 3, 4]]).unwrap();
        let points_blob = serialize_akaze_points(&[AkazePoint { x: 1.0, y: 2.0 }]).unwrap();
        conn.execute(
            "INSERT INTO feature_cache (exact_hash, size_bytes, akaze_status, akaze_keypoints, akaze_descriptors, akaze_points, feature_version, updated_at)
             VALUES (?1, ?2, 'ready', ?3, ?4, ?5, ?6, datetime('now'))",
            params!["ready-hash", 10_i64, 1_i64, ready_blob, points_blob, FEATURE_VERSION - 1],
        )
        .unwrap();
        drop(conn);

        let migrated = open_catalog_db(&db_path).unwrap();
        let ready_row: (String, i64) = migrated
            .query_row(
                "SELECT akaze_status, feature_version FROM feature_cache WHERE exact_hash = 'ready-hash'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(ready_row.0, "ready");
        assert_eq!(ready_row.1, FEATURE_VERSION);
    }

    #[test]
    fn open_catalog_db_does_not_rewrite_current_version_feature_cache_rows() {
        let tmp = tempdir().unwrap();
        let db_path = tmp.path().join("catalog.db");
        let conn = open_catalog_db(&db_path).unwrap();
        let ready_blob = serialize_akaze_descriptors(&[vec![9, 8, 7, 6]]).unwrap();
        let points_blob = serialize_akaze_points(&[AkazePoint { x: 3.0, y: 4.0 }]).unwrap();
        let updated_at = "2020-01-02T03:04:05Z";
        conn.execute(
            "INSERT INTO feature_cache (
                exact_hash, size_bytes, akaze_status, akaze_keypoints, akaze_descriptors,
                akaze_points, feature_version, updated_at
             ) VALUES (?1, ?2, 'ready', ?3, ?4, ?5, ?6, ?7)",
            params![
                "current-ready",
                42_i64,
                1_i64,
                ready_blob.clone(),
                points_blob.clone(),
                FEATURE_VERSION,
                updated_at
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO feature_cache (
                exact_hash, size_bytes, akaze_status, akaze_keypoints, akaze_descriptors,
                akaze_points, feature_version, updated_at
             ) VALUES (?1, ?2, 'decode_error', NULL, NULL, NULL, ?3, ?4)",
            params!["current-error", 7_i64, FEATURE_VERSION, updated_at],
        )
        .unwrap();
        drop(conn);

        let reopened = open_catalog_db(&db_path).unwrap();
        let ready: (String, i64, String, Vec<u8>, Vec<u8>) = reopened
            .query_row(
                "SELECT akaze_status, feature_version, updated_at, akaze_descriptors, akaze_points
                 FROM feature_cache WHERE exact_hash = 'current-ready'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(ready.0, "ready");
        assert_eq!(ready.1, FEATURE_VERSION);
        assert_eq!(ready.2, updated_at);
        assert_eq!(ready.3, ready_blob);
        assert_eq!(ready.4, points_blob);

        let error: (String, i64, String, Option<Vec<u8>>) = reopened
            .query_row(
                "SELECT akaze_status, feature_version, updated_at, akaze_descriptors
                 FROM feature_cache WHERE exact_hash = 'current-error'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(error.0, "decode_error");
        assert_eq!(error.1, FEATURE_VERSION);
        assert_eq!(error.2, updated_at);
        assert!(error.3.is_none());
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
