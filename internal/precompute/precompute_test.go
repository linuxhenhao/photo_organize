package precompute

import (
	"context"
	"database/sql"
	"encoding/json"
	"image"
	"image/color"
	"image/jpeg"
	"os"
	"path/filepath"
	"testing"

	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func writeNoisyJPEG(t *testing.T, path string, width int, height int) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))

	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			// Deterministic pattern with lots of corners/edges for ORB.
			v := uint8((x*31 + y*17 + (x^y)*13) % 255)
			img.SetRGBA(x, y, color.RGBA{R: v, G: 255 - v, B: uint8((x * y) % 255), A: 255})
		}
	}

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, jpeg.Encode(f, img, &jpeg.Options{Quality: 92}))
}

func openCacheDB(t *testing.T, dir string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", filepath.Join(dir, "cache.db")+"?_busy_timeout=5000")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS file_cache (
			target_path TEXT PRIMARY KEY,
			mmh3_hash TEXT,
			dhash TEXT,
			size INTEGER,
			metadata TEXT DEFAULT '{}',
			thumbnails TEXT DEFAULT '[]'
		);
	`)
	require.NoError(t, err)
	return db
}

func TestPrecomputeSkipsEmptyThumbnailRows(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "precompute_scope")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	masterPath := filepath.Join(tempDir, "repo", "album", "master.jpg")
	thumbPath := filepath.Join(tempDir, "repo", "thumbnails", "album", "thumb.jpg")
	writeNoisyJPEG(t, masterPath, 220, 180)
	writeNoisyJPEG(t, thumbPath, 160, 120)

	masterMMH3, err := hasher.CalculateHash(masterPath)
	require.NoError(t, err)
	thumbMMH3, err := hasher.CalculateHash(thumbPath)
	require.NoError(t, err)

	thumbsJSON, err := json.Marshal([]map[string]any{
		{
			"path":      filepath.ToSlash(thumbPath),
			"mmh3_hash": thumbMMH3,
			"metadata":  map[string]any{},
		},
	})
	require.NoError(t, err)

	db := openCacheDB(t, filepath.Join(tempDir, "repo"))
	defer db.Close()

	// Excluded rows.
	_, err = db.Exec(`INSERT INTO file_cache (target_path, mmh3_hash, thumbnails) VALUES (?, ?, NULL)`,
		filepath.ToSlash(filepath.Join(tempDir, "repo", "ignore_null.jpg")), "nullhash")
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO file_cache (target_path, mmh3_hash, thumbnails) VALUES (?, ?, '')`,
		filepath.ToSlash(filepath.Join(tempDir, "repo", "ignore_empty.jpg")), "emptyhash")
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO file_cache (target_path, mmh3_hash, thumbnails) VALUES (?, ?, '[]')`,
		filepath.ToSlash(filepath.Join(tempDir, "repo", "ignore_array.jpg")), "arrayhash")
	require.NoError(t, err)

	// Included row.
	_, err = db.Exec(`INSERT INTO file_cache (target_path, mmh3_hash, thumbnails) VALUES (?, ?, ?)`,
		filepath.ToSlash(masterPath), masterMMH3, string(thumbsJSON))
	require.NoError(t, err)

	err = Run(context.Background(), filepath.Join(tempDir, "repo"), db, Options{Workers: 1})
	require.NoError(t, err)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM visual_feature_cache`).Scan(&count))
	require.Equal(t, 2, count, "expected master+thumbnail entries only")

	var orbRows int
	var orbCols int
	var orbType int
	var orbImgWidth int
	var orbImgHeight int
	var keypoints []byte
	var descriptors []byte
	require.NoError(t, db.QueryRow(`
		SELECT orb_rows, orb_cols, orb_type, orb_img_width, orb_img_height, orb_keypoints, orb_descriptors
		FROM visual_feature_cache
		WHERE mmh3_hash = ? AND feature_version = ?
	`, masterMMH3, visualFeatureVersion).Scan(&orbRows, &orbCols, &orbType, &orbImgWidth, &orbImgHeight, &keypoints, &descriptors))
	require.Greater(t, orbRows, 0)
	require.Greater(t, orbCols, 0)
	require.GreaterOrEqual(t, orbType, 0)
	require.Greater(t, orbImgWidth, 0)
	require.Greater(t, orbImgHeight, 0)
	require.NotEmpty(t, keypoints)
	require.NotEmpty(t, descriptors)

	// Second run should skip cached entries and keep the same count.
	err = Run(context.Background(), filepath.Join(tempDir, "repo"), db, Options{Workers: 1})
	require.NoError(t, err)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM visual_feature_cache`).Scan(&count))
	require.Equal(t, 2, count)
}
