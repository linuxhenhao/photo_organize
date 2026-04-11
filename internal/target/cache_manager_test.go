package target

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCacheManager_Basic(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "photo_organize_cache_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cm, err := NewCacheManager(tempDir, 100)
	require.NoError(t, err)
	defer cm.Close()

	path := filepath.Join(tempDir, "test.jpg")
	hash := "12345678"
	phash := uint64(0xABCDEF)
	size := int64(1000)

	// Test Add and Check with CheckAndAddPerceptualMatch (Unique)
	match := cm.CheckAndAddPerceptualMatch(phash, path, size, hash)
	require.Nil(t, match)

	// Test IsCached
	require.True(t, cm.IsCached(path))

	// Test FindExactMatch
	foundPath, found := cm.FindExactMatch(hash)
	require.True(t, found)
	require.Equal(t, path, foundPath)

	// Test Match finding (Self)
	match = cm.CheckAndAddPerceptualMatch(phash, "another.jpg", 1001, "hash2")
	require.NotNil(t, match)
	require.Equal(t, path, match.Path)

	// Test Delete
	cm.DeleteEntry(path)
	// Note: Delete is async for DB, but sync for memory state in our new logic
	require.False(t, cm.IsCached(path))
	_, found = cm.FindExactMatch(hash)
	require.False(t, found)
}

func TestCacheManager_Persistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "photo_organize_persistence_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	path := "persistent.jpg"
	hash := "87654321"
	phash := uint64(0x123456)
	size := int64(2000)

	// 1. Create and Add
	cm, err := NewCacheManager(tempDir, 1) // batchSize=1 for immediate flush
	require.NoError(t, err)
	metadata := `{"width":800,"height":600,"create_time":"2023-01-01 12:00:00"}`
	cm.AddEntry(path, hash, phash, size, metadata)
	cm.AppendThumbnailToMaster(path, "some_thumbnail_path", `{"width":200}`)
	cm.Close() // This should flush everything

	// 2. Re-open and check
	cm2, err := NewCacheManager(tempDir, 100)
	require.NoError(t, err)
	defer cm2.Close()

	require.True(t, cm2.IsCached(path))
	foundPath, found := cm2.FindExactMatch(hash)
	require.True(t, found)
	require.Equal(t, path, foundPath)

	matches := cm2.SearchPHash(phash, 0)
	require.Len(t, matches, 1)
	require.Equal(t, path, matches[0].Path)

	// Verify metadata in DB
	var dbMetadata sql.NullString
	err = cm2.db.QueryRow("SELECT metadata FROM file_cache WHERE target_path = ?", path).Scan(&dbMetadata)
	require.NoError(t, err)
	require.Equal(t, metadata, dbMetadata.String)

	// Verify thumbnails in DB
	var dbThumbnails sql.NullString
	err = cm2.db.QueryRow("SELECT thumbnails FROM file_cache WHERE target_path = ?", path).Scan(&dbThumbnails)
	require.NoError(t, err)
	require.Equal(t, `[{"path":"some_thumbnail_path","metadata":{"width":200}}]`, dbThumbnails.String)
}

func TestInitTargetDirCache_Migration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "initcache_migration_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 1. Create a real image file in target
	path := filepath.Join(tempDir, "migrateme.jpg")
	f, _ := os.Create(path)
	f.Write([]byte("dummy content")) // Not a real JPEG but enough for phash calc to be called (it will fail but it's fine)
	f.Close()

	// 2. Pre-populate DB with an incomplete entry (missing phash and metadata)
	cm, err := NewCacheManager(tempDir, 1)
	require.NoError(t, err)

	_, err = cm.db.Exec(`INSERT INTO file_cache (target_path, mmh3_hash, phash, size, metadata, thumbnails) VALUES (?, 'abc', '', 100, '{}', '[]')`, path)
	require.NoError(t, err)
	cm.Close()

	// 3. Re-open and run InitTargetDirCache
	cm2, err := NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cm2.Close()

	InitTargetDirCache(tempDir, cm2)

	// Give background worker a moment
	require.Eventually(t, func() bool {
		var phash, metadata string
		err := cm2.db.QueryRow("SELECT phash, metadata FROM file_cache WHERE target_path = ?", path).Scan(&phash, &metadata)
		return err == nil && metadata != "{}"
	}, 2*time.Second, 100*time.Millisecond, "Incomplete entry should be auto-migrated")
}

func TestCacheManager_DeleteEntryFiltersStaleHashesAndPHashes(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "cache_manager_stale_match_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cm, err := NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cm.Close()

	path := filepath.Join(tempDir, "master.jpg")
	oldHash := "old-hash"
	newHash := "new-hash"
	oldPHash := uint64(0x1010)
	newPHash := uint64(0x2020)

	cm.AddEntry(path, oldHash, oldPHash, 100, `{"width":1}`)
	cm.DeleteEntry(path)

	_, found := cm.FindExactMatch(oldHash)
	require.False(t, found)
	require.Empty(t, cm.SearchPHash(oldPHash, 0))

	cm.AddEntry(path, newHash, newPHash, 120, `{"width":2}`)

	_, found = cm.FindExactMatch(oldHash)
	require.False(t, found)
	foundPath, found := cm.FindExactMatch(newHash)
	require.True(t, found)
	require.Equal(t, path, foundPath)
	require.Empty(t, cm.SearchPHash(oldPHash, 0))

	matches := cm.SearchPHash(newPHash, 0)
	require.Len(t, matches, 1)
	require.Equal(t, path, matches[0].Path)
}

func TestCacheManager_ZeroValuedPHashIsSearchable(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "cache_manager_zero_phash_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cm, err := NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cm.Close()

	path := filepath.Join(tempDir, "flat.jpg")
	cm.AddEntryWithPresence(path, "zero-hash", 0, true, 64, `{"width":32}`)

	matches := cm.SearchPHash(0, 0)
	require.Len(t, matches, 1)
	require.Equal(t, path, matches[0].Path)

	info, ok := cm.GetCachedInfo(path)
	require.True(t, ok)
	require.Equal(t, "0000000000000000", info.PHash)
}
