package db

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestInitDB(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "photo_organize_db_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer db.Close()

	// Initial Init
	err = InitDB(db)
	require.NoError(t, err)

	// Check if table exists
	var count int
	err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='photos'").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Re-init (should be idempotent)
	err = InitDB(db)
	require.NoError(t, err)
}

func TestLoadExistingPaths(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "photo_organize_db_load_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	database, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer database.Close()

	err = InitDB(database)
	require.NoError(t, err)

	paths := []string{"/a/b.jpg", "/c/d.png"}
	for _, p := range paths {
		_, err = database.Exec(`INSERT INTO photos (source_path) VALUES (?)`, p)
		require.NoError(t, err)
	}

	loadedPaths, err := LoadExistingPaths(database)
	require.NoError(t, err)
	require.Len(t, loadedPaths, 2)
	for _, p := range paths {
		require.True(t, loadedPaths[p])
	}
}
