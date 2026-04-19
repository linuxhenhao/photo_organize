package migrate

import (
	"context"
	"database/sql"
	"fmt"
)

func tableColumns(ctx context.Context, db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name string
		var ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

func ensureFileCacheColumns(ctx context.Context, db *sql.DB) {
	// Best-effort for older DBs.
	_, _ = db.ExecContext(ctx, `ALTER TABLE file_cache ADD COLUMN metadata TEXT DEFAULT '{}'`)
	_, _ = db.ExecContext(ctx, `ALTER TABLE file_cache ADD COLUMN thumbnails TEXT DEFAULT '[]'`)
	_, _ = db.ExecContext(ctx, `UPDATE file_cache SET thumbnails = '[]' WHERE thumbnails = '' OR thumbnails IS NULL`)
	_, _ = db.ExecContext(ctx, `ALTER TABLE file_cache DROP COLUMN master_path`)
}

// MigrateFileCacheHashColumn ensures file_cache stores first-stage hashes under column `dhash`.
// If a legacy `phash` column exists, it is renamed (or rebuilt) into `dhash`.
func MigrateFileCacheHashColumn(ctx context.Context, db *sql.DB) (bool, error) {
	// Ensure table exists (new installs).
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS file_cache (
			target_path TEXT PRIMARY KEY,
			mmh3_hash TEXT,
			dhash TEXT,
			size INTEGER,
			metadata TEXT DEFAULT '{}',
			thumbnails TEXT DEFAULT '[]'
		);
	`)
	if err != nil {
		return false, fmt.Errorf("create file_cache: %w", err)
	}

	ensureFileCacheColumns(ctx, db)

	cols, err := tableColumns(ctx, db, "file_cache")
	if err != nil {
		return false, fmt.Errorf("inspect file_cache: %w", err)
	}
	if cols["dhash"] {
		return false, nil
	}
	if !cols["phash"] {
		// Nothing to migrate (unexpected), but keep going.
		return false, nil
	}

	// Try native rename first.
	if _, err := db.ExecContext(ctx, `ALTER TABLE file_cache RENAME COLUMN phash TO dhash`); err == nil {
		return true, nil
	}

	// Fallback: rebuild table (for older SQLite engines).
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		CREATE TABLE file_cache__new (
			target_path TEXT PRIMARY KEY,
			mmh3_hash TEXT,
			dhash TEXT,
			size INTEGER,
			metadata TEXT DEFAULT '{}',
			thumbnails TEXT DEFAULT '[]'
		);
	`)
	if err != nil {
		return false, fmt.Errorf("create file_cache__new: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO file_cache__new (target_path, mmh3_hash, dhash, size, metadata, thumbnails)
		SELECT
			target_path,
			mmh3_hash,
			phash,
			size,
			metadata,
			CASE WHEN thumbnails = '' OR thumbnails IS NULL THEN '[]' ELSE thumbnails END
		FROM file_cache
	`)
	if err != nil {
		return false, fmt.Errorf("copy file_cache to file_cache__new: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DROP TABLE file_cache`); err != nil {
		return false, fmt.Errorf("drop old file_cache: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE file_cache__new RENAME TO file_cache`); err != nil {
		return false, fmt.Errorf("rename file_cache__new: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// MigratePhotosHashColumn ensures photos stores first-stage hashes under column `dhash`.
func MigratePhotosHashColumn(ctx context.Context, db *sql.DB) (bool, error) {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS photos (
			source_path TEXT PRIMARY KEY,
			size INTEGER,
			create_time TEXT,
			mmh3_hash TEXT DEFAULT '',
			dhash TEXT DEFAULT '',
			mime_type TEXT DEFAULT '',
			group_id INTEGER DEFAULT 0
		);
	`)
	if err != nil {
		return false, fmt.Errorf("create photos: %w", err)
	}

	// Best-effort for existing DBs.
	_, _ = db.ExecContext(ctx, `ALTER TABLE photos ADD COLUMN mime_type TEXT DEFAULT ''`)

	cols, err := tableColumns(ctx, db, "photos")
	if err != nil {
		return false, fmt.Errorf("inspect photos: %w", err)
	}

	if cols["dhash"] {
		return false, nil
	}
	if !cols["phash"] {
		_, _ = db.ExecContext(ctx, `ALTER TABLE photos ADD COLUMN dhash TEXT DEFAULT ''`)
		return true, nil
	}

	if _, err := db.ExecContext(ctx, `ALTER TABLE photos RENAME COLUMN phash TO dhash`); err == nil {
		return true, nil
	}

	// Rebuild fallback.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		CREATE TABLE photos__new (
			source_path TEXT PRIMARY KEY,
			size INTEGER,
			create_time TEXT,
			mmh3_hash TEXT DEFAULT '',
			dhash TEXT DEFAULT '',
			mime_type TEXT DEFAULT '',
			group_id INTEGER DEFAULT 0
		);
	`)
	if err != nil {
		return false, fmt.Errorf("create photos__new: %w", err)
	}

	// Ensure mime_type exists on old table so we can SELECT it.
	_, _ = tx.ExecContext(ctx, `ALTER TABLE photos ADD COLUMN mime_type TEXT DEFAULT ''`)

	_, err = tx.ExecContext(ctx, `
		INSERT INTO photos__new (source_path, size, create_time, mmh3_hash, dhash, mime_type, group_id)
		SELECT source_path, size, create_time, mmh3_hash, phash, mime_type, group_id
		FROM photos
	`)
	if err != nil {
		return false, fmt.Errorf("copy photos to photos__new: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DROP TABLE photos`); err != nil {
		return false, fmt.Errorf("drop old photos: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE photos__new RENAME TO photos`); err != nil {
		return false, fmt.Errorf("rename photos__new: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func IntegrityCheck(ctx context.Context, db *sql.DB) (string, error) {
	var result string
	if err := db.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&result); err != nil {
		return "", err
	}
	return result, nil
}
