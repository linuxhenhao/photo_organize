package db

import (
	"database/sql"
	"fmt"
	"log"

	_ "modernc.org/sqlite" // SQLite driver
)

// InitDB initializes the SQLite schema and backwards compatibility logic
func InitDB(db *sql.DB) error {
	_, err := db.Exec(`PRAGMA synchronous = OFF`)
	if err != nil {
		log.Printf("Warning: Failed to set synchronous mode: %v", err)
	}
	_, err = db.Exec(`PRAGMA journal_mode = WAL`)
	if err != nil {
		log.Printf("Warning: Failed to set journal mode: %v", err)
	}
	_, err = db.Exec(`PRAGMA cache_size = -64000`) // 64MB cache
	if err != nil {
		log.Printf("Warning: Failed to set cache size: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS photos (
			source_path TEXT PRIMARY KEY,
			size INTEGER,
			create_time TEXT,
			mmh3_hash TEXT DEFAULT '',
			phash TEXT DEFAULT '',
			group_id INTEGER DEFAULT 0
		);
	`)
	// Backward compatibility: add columns for existing databases
	_, _ = db.Exec(`ALTER TABLE photos ADD COLUMN phash TEXT DEFAULT '';`)
	_, _ = db.Exec(`ALTER TABLE photos ADD COLUMN mime_type TEXT DEFAULT '';`)

	if err != nil {
		return fmt.Errorf("failed to create database table: %w", err)
	}
	return nil
}

// LoadExistingPaths reads all source_path entries from the database into a map for quick lookups.
func LoadExistingPaths(db *sql.DB) (map[string]bool, error) {
	log.Println("Loading existing file paths from the database to prevent re-scanning...")
	rows, err := db.Query("SELECT source_path FROM photos")
	if err != nil {
		return nil, fmt.Errorf("failed to query existing paths: %w", err)
	}
	defer rows.Close()

	existingPaths := make(map[string]bool)
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			log.Printf("Failed to scan path: %v", err)
			continue
		}
		existingPaths[path] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over existing paths: %w", err)
	}
	log.Printf("Loaded %d existing file paths. These will be skipped.", len(existingPaths))
	return existingPaths, nil
}
