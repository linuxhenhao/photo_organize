package target

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

	"github.com/linuxhenhao/photo_organize/internal/dedupe"
	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/nfnt/resize"
	"github.com/stretchr/testify/require"
)

func writeJPEGWithQuality(t *testing.T, path string, quality int) {
	writeSizedJPEGWithQuality(t, path, 64, 64, quality)
}

func writeJPEGFixturePair(t *testing.T, masterPath string, thumbPath string) {
	t.Helper()
	writeSizedJPEGWithQuality(t, masterPath, 96, 72, 90)

	file, err := os.Open(masterPath)
	require.NoError(t, err)
	defer file.Close()

	img, _, err := image.Decode(file)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(filepath.Dir(thumbPath), 0755))
	thumbFile, err := os.Create(thumbPath)
	require.NoError(t, err)
	defer thumbFile.Close()

	thumbImg := resize.Resize(48, 36, img, resize.Lanczos3)
	require.NoError(t, jpeg.Encode(thumbFile, thumbImg, &jpeg.Options{Quality: 70}))
}

func writeSizedJPEGWithQuality(t *testing.T, path string, width int, height int, quality int) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))

	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			rx := float64(x) / float64(maxDim(width-1, 1))
			ry := float64(y) / float64(maxDim(height-1, 1))
			baseR := uint8(255 * rx)
			baseG := uint8(255 * ry)
			baseB := uint8(255 * (1 - rx*ry))
			if ((x/8)+(y/8))%2 == 0 {
				baseR = 255 - baseR/2
				baseB /= 2
			}
			if (x-width/3)*(x-width/3)+(y-height/2)*(y-height/2) < (minDim(width, height)/6)*(minDim(width, height)/6) {
				baseG = 255
			}
			img.Set(x, y, color.RGBA{R: baseR, G: baseG, B: baseB, A: 255})
		}
	}

	file, err := os.Create(path)
	require.NoError(t, err)
	defer file.Close()

	require.NoError(t, jpeg.Encode(file, img, &jpeg.Options{Quality: quality}))
}

func maxDim(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func minDim(a int, b int) int {
	if a < b {
		return a
	}
	return b
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
	writeJPEGFixturePair(t, masterPath, thumbPath)

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
	require.NotEmpty(t, thumbnails[0].MMH3)

	_, err = os.Stat(masterPath)
	require.NoError(t, err)
	_, err = os.Stat(thumbPath)
	require.NoError(t, err)
}

func TestInitTargetDirCacheReadOnlyBackfillsThumbnailMMH3(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "initcache_readonly_thumb_mmh3_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	masterPath := filepath.Join(tempDir, "album", "img_master.jpg")
	thumbPath := filepath.Join(tempDir, "thumbnails", "album", "img_thumb.jpg")
	writeJPEGFixturePair(t, masterPath, thumbPath)

	cm, err := NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cm.Close()

	masterStat, err := os.Stat(masterPath)
	require.NoError(t, err)
	masterFile, err := buildTargetFile(masterPath, masterStat, fileCacheRow{}, false)
	require.NoError(t, err)

	thumbMeta := metadata.ExtractImageMetaJson(thumbPath)
	thumbsJSON := marshalThumbnailEntries([]thumbnailEntry{makeThumbnailEntry(thumbPath, "", thumbMeta)})
	_, err = cm.db.Exec(
		`INSERT INTO file_cache (target_path, mmh3_hash, phash, size, metadata, thumbnails) VALUES (?, ?, ?, ?, ?, ?)`,
		masterPath,
		masterFile.MMH3,
		masterFile.PHashStr,
		masterFile.Size,
		masterFile.Metadata,
		thumbsJSON,
	)
	require.NoError(t, err)

	InitTargetDirCache(tempDir, cm)

	var thumbnailsRaw string
	err = cm.db.QueryRow(`SELECT thumbnails FROM file_cache WHERE target_path = ?`, masterPath).Scan(&thumbnailsRaw)
	require.NoError(t, err)

	var thumbnails []thumbnailEntry
	require.NoError(t, json.Unmarshal([]byte(thumbnailsRaw), &thumbnails))
	require.Len(t, thumbnails, 1)
	require.Equal(t, thumbPath, thumbnails[0].Path)

	expectedThumbHash, err := hasher.CalculateHash(thumbPath)
	require.NoError(t, err)
	require.Equal(t, expectedThumbHash, thumbnails[0].MMH3)
}

func TestInitTargetDirCacheMoveDuplicatesUsesExistingCache(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "initcache_move_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	bigPath := filepath.Join(tempDir, "2024", "01", "01", "a-big.jpg")
	smallPath := filepath.Join(tempDir, "2024", "02", "03", "b-small.jpg")
	writeJPEGFixturePair(t, bigPath, smallPath)

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

	expectedThumbPath := filepath.Join(tempDir, "thumbnails", "2024", "02", "03", filepath.Base(smallPath))

	_, err = os.Stat(bigPath)
	require.NoError(t, err)
	_, err = os.Stat(expectedThumbPath)
	require.NoError(t, err)
	_, err = os.Stat(smallPath)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(tempDir, "2024", "02", "03"))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(tempDir, "2024", "02"))
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

func TestInitTargetDirCacheWithContextStopsBeforeMoveOnCancellation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "initcache_move_cancel")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	bigPath := filepath.Join(tempDir, "2024", "01", "01", "a-big.jpg")
	smallPath := filepath.Join(tempDir, "2024", "02", "03", "b-small.jpg")
	writeJPEGFixturePair(t, bigPath, smallPath)

	bigStat, err := os.Stat(bigPath)
	require.NoError(t, err)
	smallStat, err := os.Stat(smallPath)
	require.NoError(t, err)

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

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	InitTargetDirCacheWithContext(ctx, tempDir, cm, InitCacheOptions{MoveDuplicates: true})

	_, err = os.Stat(bigPath)
	require.NoError(t, err)
	_, err = os.Stat(smallPath)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(tempDir, "thumbnails"))
	require.True(t, os.IsNotExist(err))

	var count int
	err = cm.db.QueryRow(`SELECT COUNT(*) FROM file_cache`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)
}
