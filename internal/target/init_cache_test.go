package target

import (
	"database/sql"
	"encoding/json"
	"image"
	"image/color"
	"image/jpeg"
	"os"
	"path/filepath"
	"testing"

	"github.com/linuxhenhao/photo_organize/internal/dedupe"
	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/stretchr/testify/require"
)

func copyFixtureFile(t *testing.T, relPath string, destPath string) {
	t.Helper()

	srcPath := filepath.Clean(filepath.Join("..", "..", relPath))
	data, err := os.ReadFile(srcPath)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(destPath), 0755))
	require.NoError(t, os.WriteFile(destPath, data, 0644))
}

func writeJPEGWithQuality(t *testing.T, path string, quality int) {
	writeSizedJPEGWithQuality(t, path, 64, 64, quality)
}

func writeSizedJPEGWithQuality(t *testing.T, path string, width int, height int, quality int) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))

	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			rx := float64(x) / float64(width)
			ry := float64(y) / float64(height)
			img.Set(x, y, color.RGBA{
				R: uint8(255 * rx),
				G: uint8(255 * ry),
				B: uint8(255 * (1 - rx*ry)),
				A: 255,
			})
		}
	}

	file, err := os.Create(path)
	require.NoError(t, err)
	defer file.Close()

	require.NoError(t, jpeg.Encode(file, img, &jpeg.Options{Quality: quality}))
}

func TestInitTargetDirCacheReadOnlyDoesNotMoveFiles(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "initcache_readonly_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	pathA := filepath.Join(tempDir, "2024", "01", "01", "a.jpg")
	pathB := filepath.Join(tempDir, "2024", "01", "01", "b.jpg")
	writeJPEGWithQuality(t, pathA, 90)
	writeJPEGWithQuality(t, pathB, 40)

	cm, err := NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cm.Close()

	InitTargetDirCache(tempDir, cm)

	require.Eventually(t, func() bool {
		var count int
		err := cm.db.QueryRow(`SELECT COUNT(*) FROM file_cache`).Scan(&count)
		return err == nil && count == 2
	}, 2e9, 1e8)

	_, err = os.Stat(pathA)
	require.NoError(t, err)
	_, err = os.Stat(pathB)
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(tempDir, "thumbnails"))
	require.True(t, os.IsNotExist(err))
}

func TestInitTargetDirCacheReadOnlyRebuildsExistingThumbnailLinks(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "initcache_readonly_thumbs_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	masterPath := filepath.Join(tempDir, "album", "img_master.jpg")
	thumbPath := filepath.Join(tempDir, "thumbnails", "album", "img_thumb.jpg")
	copyFixtureFile(t, filepath.Join("test_data", "source_mock", "img_2023_05_01.jpg"), masterPath)
	copyFixtureFile(t, filepath.Join("test_data", "source_mock_thumbs", "thumb_2023_05_01.jpg"), thumbPath)

	cm, err := NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cm.Close()

	masterStat, err := os.Stat(masterPath)
	require.NoError(t, err)
	masterFile, err := buildTargetFile(masterPath, masterStat, fileCacheRow{}, false)
	require.NoError(t, err)
	require.True(t, masterFile.HasPHash)
	require.NotEmpty(t, masterFile.PHashStr)

	InitTargetDirCache(tempDir, cm)

	var count int
	err = cm.db.QueryRow(`SELECT COUNT(*) FROM file_cache`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	var masterPHash string
	err = cm.db.QueryRow(`SELECT phash FROM file_cache WHERE target_path = ?`, masterPath).Scan(&masterPHash)
	require.NoError(t, err)
	require.NotEmpty(t, masterPHash)

	thumbPHash, err := hasher.CalculatePHash(thumbPath)
	require.NoError(t, err)
	masterPHashVal, err := hasher.StringToPHash(masterPHash)
	require.NoError(t, err)
	distance := hasher.HammingDistance(masterPHashVal, thumbPHash)
	require.Greater(t, distance, 5)
	require.LessOrEqual(t, distance, dedupe.CandidateSearchDistance)
	matches := cm.SearchPHash(thumbPHash, dedupe.CandidateSearchDistance)
	require.NotEmpty(t, matches)
	require.Equal(t, masterPath, matches[0].Path)

	var thumbnailsRaw string
	err = cm.db.QueryRow(`SELECT thumbnails FROM file_cache WHERE target_path = ?`, masterPath).Scan(&thumbnailsRaw)
	require.NoError(t, err)

	var thumbnails []thumbnailEntry
	require.NoError(t, json.Unmarshal([]byte(thumbnailsRaw), &thumbnails))
	require.Len(t, thumbnails, 1)
	require.Equal(t, thumbPath, thumbnails[0].Path)

	_, err = os.Stat(masterPath)
	require.NoError(t, err)
	_, err = os.Stat(thumbPath)
	require.NoError(t, err)
}

func TestInitTargetDirCacheMoveDuplicatesUsesExistingCache(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "initcache_move_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	bigPath := filepath.Join(tempDir, "2024", "01", "01", "a-big.jpg")
	smallPath := filepath.Join(tempDir, "2024", "01", "01", "b-small.jpg")
	copyFixtureFile(t, filepath.Join("test_data", "source_mock", "img_2023_05_01.jpg"), bigPath)
	copyFixtureFile(t, filepath.Join("test_data", "source_mock_thumbs", "thumb_2023_05_01.jpg"), smallPath)

	bigStat, err := os.Stat(bigPath)
	require.NoError(t, err)
	smallStat, err := os.Stat(smallPath)
	require.NoError(t, err)
	require.Greater(t, bigStat.Size(), smallStat.Size())

	bigHash, err := hasher.CalculateHash(bigPath)
	require.NoError(t, err)
	bigPHash, err := hasher.CalculatePHash(bigPath)
	require.NoError(t, err)
	bigMeta := metadata.ExtractImageMetaJson(bigPath)

	smallHash, err := hasher.CalculateHash(smallPath)
	require.NoError(t, err)
	smallPHash, err := hasher.CalculatePHash(smallPath)
	require.NoError(t, err)
	smallMeta := metadata.ExtractImageMetaJson(smallPath)

	cm, err := NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cm.Close()

	_, err = cm.db.Exec(`
		INSERT INTO file_cache (target_path, mmh3_hash, phash, size, metadata, thumbnails)
		VALUES (?, ?, ?, ?, ?, '[]'),
		       (?, ?, ?, ?, ?, '[]')
	`,
		bigPath, bigHash, hasher.PHashToString(bigPHash), bigStat.Size(), bigMeta,
		smallPath, smallHash, hasher.PHashToString(smallPHash), smallStat.Size(), smallMeta,
	)
	require.NoError(t, err)
	cm.SetEntryMemoryWithPresence(bigPath, bigHash, bigPHash, true, bigStat.Size(), bigMeta)
	cm.SetEntryMemoryWithPresence(smallPath, smallHash, smallPHash, true, smallStat.Size(), smallMeta)

	InitTargetDirCacheWithOptions(tempDir, cm, InitCacheOptions{MoveDuplicates: true})

	expectedThumbPath := filepath.Join(tempDir, "thumbnails", "2024", "01", "01", filepath.Base(smallPath))

	_, err = os.Stat(bigPath)
	require.NoError(t, err)
	_, err = os.Stat(expectedThumbPath)
	require.NoError(t, err)
	_, err = os.Stat(smallPath)
	require.True(t, os.IsNotExist(err))

	var count int
	err = cm.db.QueryRow(`SELECT COUNT(*) FROM file_cache`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	var thumbnailsRaw string
	err = cm.db.QueryRow(`SELECT thumbnails FROM file_cache WHERE target_path = ?`, bigPath).Scan(&thumbnailsRaw)
	require.NoError(t, err)

	var thumbnails []thumbnailEntry
	require.NoError(t, json.Unmarshal([]byte(thumbnailsRaw), &thumbnails))
	require.Len(t, thumbnails, 1)
	require.Equal(t, expectedThumbPath, thumbnails[0].Path)

	var dbHash string
	var dbPHash string
	var dbMeta sql.NullString
	err = cm.db.QueryRow(`SELECT mmh3_hash, phash, metadata FROM file_cache WHERE target_path = ?`, bigPath).Scan(&dbHash, &dbPHash, &dbMeta)
	require.NoError(t, err)
	require.Equal(t, bigHash, dbHash)
	require.Equal(t, hasher.PHashToString(bigPHash), dbPHash)
	require.Equal(t, bigMeta, dbMeta.String)
}

func TestInitTargetDirCacheMoveDuplicatesDoesNotPublishMemoryStateOnDBFailure(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "initcache_move_db_failure")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	path := filepath.Join(tempDir, "2024", "01", "01", "only.jpg")
	writeJPEGWithQuality(t, path, 90)

	cm, err := NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cm.Close()

	_, err = cm.db.Exec(`
		CREATE TRIGGER fail_file_cache_insert
		BEFORE INSERT ON file_cache
		BEGIN
			SELECT RAISE(FAIL, 'blocked');
		END;
	`)
	require.NoError(t, err)

	InitTargetDirCacheWithOptions(tempDir, cm, InitCacheOptions{MoveDuplicates: true})

	require.False(t, cm.IsCached(path))
	_, err = os.Stat(path)
	require.NoError(t, err)
}
