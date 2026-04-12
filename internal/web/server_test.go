package web

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/linuxhenhao/photo_organize/internal/target"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func writeJPEG(t *testing.T, path string, width int, height int, fill color.RGBA) []byte {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))

	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Set(x, y, fill)
		}
	}

	var buf bytes.Buffer
	require.NoError(t, jpeg.Encode(&buf, img, nil))
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0644))

	return buf.Bytes()
}

func TestListenAddrUsesConfiguredHost(t *testing.T) {
	require.Equal(t, "127.0.0.1:8080", listenAddr("127.0.0.1", 8080))
	require.Equal(t, "0.0.0.0:9090", listenAddr("0.0.0.0", 9090))
}

func TestHandleImageServeRejectsPathEscape(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "web_image_escape_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	outsideDir, err := os.MkdirTemp("", "web_image_escape_outside")
	require.NoError(t, err)
	defer os.RemoveAll(outsideDir)

	outsidePath := filepath.Join(outsideDir, "outside.jpg")
	require.NoError(t, os.WriteFile(outsidePath, []byte("outside"), 0644))

	ws := NewWebServer(nil, tempDir)
	req := httptest.NewRequest(http.MethodGet, "/image?path="+outsidePath, nil)
	rr := httptest.NewRecorder()

	ws.handleImageServe(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestHandleResolveGroupPromotesThumbnailToMaster(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "web_promote_thumbnail_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cacheManager, err := target.NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cacheManager.Close()

	dbPath := filepath.Join(tempDir, "cache.db")
	sqliteDB, err := sql.Open("sqlite", dbPath+"?_busy_timeout=5000")
	require.NoError(t, err)
	defer sqliteDB.Close()

	masterPath := filepath.Join(tempDir, "2024", "01", "02", "master.jpg")
	thumbPath := filepath.Join(tempDir, "thumbnails", "2024", "01", "02", "thumb.jpg")

	writeJPEG(t, masterPath, 12, 12, color.RGBA{255, 0, 0, 255})
	thumbBytes := writeJPEG(t, thumbPath, 24, 24, color.RGBA{0, 255, 0, 255})

	masterStat, err := os.Stat(masterPath)
	require.NoError(t, err)
	masterMeta := metadata.ExtractImageMetaJson(masterPath)
	thumbMeta := metadata.ExtractImageMetaJson(thumbPath)

	_, err = sqliteDB.Exec(`INSERT INTO file_cache (target_path, mmh3_hash, phash, size, metadata, thumbnails) VALUES (?, ?, ?, ?, ?, ?)`,
		masterPath, "old-master-hash", "0000000000000001", masterStat.Size(), masterMeta,
		`[{"path":"`+thumbPath+`","metadata":`+thumbMeta+`}]`)
	require.NoError(t, err)
	cacheManager.SetEntryMemory(masterPath, "old-master-hash", 1, masterStat.Size(), masterMeta)

	ws := NewWebServer(cacheManager, tempDir)
	ws.db = sqliteDB

	body, err := json.Marshal(map[string]any{
		"keepPath":    thumbPath,
		"deletePaths": []string{masterPath},
		"masterPath":  masterPath,
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/api/resolve", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	ws.handleResolveGroup(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	gotBytes, err := os.ReadFile(masterPath)
	require.NoError(t, err)
	require.Equal(t, thumbBytes, gotBytes)
	_, err = os.Stat(thumbPath)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(tempDir, "thumbnails"))
	require.True(t, os.IsNotExist(err))

	expectedHash, err := hasher.CalculateHash(masterPath)
	require.NoError(t, err)
	expectedPHash, err := hasher.CalculatePHash(masterPath)
	require.NoError(t, err)

	var gotHash string
	var gotPHash string
	var thumbnails string
	err = sqliteDB.QueryRow(`SELECT mmh3_hash, phash, thumbnails FROM file_cache WHERE target_path = ?`, masterPath).Scan(&gotHash, &gotPHash, &thumbnails)
	require.NoError(t, err)
	require.Equal(t, expectedHash, gotHash)
	require.Equal(t, hasher.PHashToString(expectedPHash), gotPHash)
	require.Equal(t, "[]", thumbnails)

	foundPath, found := cacheManager.FindExactMatch(expectedHash)
	require.True(t, found)
	require.Equal(t, masterPath, foundPath)
}

func TestHandleResolveGroupKeepsMultipleSelectedItems(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "web_keep_multiple_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cacheManager, err := target.NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cacheManager.Close()

	dbPath := filepath.Join(tempDir, "cache.db")
	sqliteDB, err := sql.Open("sqlite", dbPath+"?_busy_timeout=5000")
	require.NoError(t, err)
	defer sqliteDB.Close()

	masterPath := filepath.Join(tempDir, "2024", "01", "02", "master.jpg")
	keepThumbPath := filepath.Join(tempDir, "thumbnails", "2024", "01", "02", "keep-thumb.jpg")
	deleteThumbPath := filepath.Join(tempDir, "thumbnails", "2024", "01", "02", "delete-thumb.jpg")
	restoredKeepPath := filepath.Join(tempDir, "2024", "01", "02", "keep-thumb.jpg")

	writeJPEG(t, masterPath, 24, 24, color.RGBA{255, 0, 0, 255})
	writeJPEG(t, keepThumbPath, 20, 20, color.RGBA{0, 255, 0, 255})
	writeJPEG(t, deleteThumbPath, 18, 18, color.RGBA{0, 0, 255, 255})

	masterStat, err := os.Stat(masterPath)
	require.NoError(t, err)
	masterMeta := metadata.ExtractImageMetaJson(masterPath)
	keepMeta := metadata.ExtractImageMetaJson(keepThumbPath)
	deleteMeta := metadata.ExtractImageMetaJson(deleteThumbPath)

	_, err = sqliteDB.Exec(`INSERT INTO file_cache (target_path, mmh3_hash, phash, size, metadata, thumbnails) VALUES (?, ?, ?, ?, ?, ?)`,
		masterPath, "old-master-hash", "0000000000000001", masterStat.Size(), masterMeta,
		`[{"path":"`+keepThumbPath+`","metadata":`+keepMeta+`},{"path":"`+deleteThumbPath+`","metadata":`+deleteMeta+`}]`)
	require.NoError(t, err)
	cacheManager.SetEntryMemory(masterPath, "old-master-hash", 1, masterStat.Size(), masterMeta)

	ws := NewWebServer(cacheManager, tempDir)
	ws.db = sqliteDB

	body, err := json.Marshal(map[string]any{
		"keepPaths":  []string{masterPath, keepThumbPath},
		"masterPath": masterPath,
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/api/resolve", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	ws.handleResolveGroup(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	_, err = os.Stat(masterPath)
	require.NoError(t, err)
	_, err = os.Stat(restoredKeepPath)
	require.NoError(t, err)
	_, err = os.Stat(keepThumbPath)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(deleteThumbPath)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(tempDir, "thumbnails"))
	require.True(t, os.IsNotExist(err))

	var masterThumbs string
	err = sqliteDB.QueryRow(`SELECT thumbnails FROM file_cache WHERE target_path = ?`, masterPath).Scan(&masterThumbs)
	require.NoError(t, err)
	require.Equal(t, "[]", masterThumbs)

	expectedHash, err := hasher.CalculateHash(restoredKeepPath)
	require.NoError(t, err)
	expectedPHash, err := hasher.CalculatePHash(restoredKeepPath)
	require.NoError(t, err)

	var keepHash string
	var keepPHash string
	var keepThumbs string
	err = sqliteDB.QueryRow(`SELECT mmh3_hash, phash, thumbnails FROM file_cache WHERE target_path = ?`, restoredKeepPath).Scan(&keepHash, &keepPHash, &keepThumbs)
	require.NoError(t, err)
	require.Equal(t, expectedHash, keepHash)
	require.Equal(t, hasher.PHashToString(expectedPHash), keepPHash)
	require.Equal(t, "[]", keepThumbs)

	var deletedCount int
	err = sqliteDB.QueryRow(`SELECT COUNT(*) FROM file_cache WHERE target_path = ?`, deleteThumbPath).Scan(&deletedCount)
	require.NoError(t, err)
	require.Equal(t, 0, deletedCount)

	foundPath, found := cacheManager.FindExactMatch(expectedHash)
	require.True(t, found)
	require.Equal(t, restoredKeepPath, foundPath)
}

func TestHandleImageServeAcceptsStoredRelativeDestPath(t *testing.T) {
	rootDir, err := os.MkdirTemp("", "web_relative_dest_path")
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	oldWD, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(rootDir))
	defer func() {
		require.NoError(t, os.Chdir(oldWD))
	}()

	relDest := "dest"
	imgPath := filepath.Join(relDest, "2024", "01", "02", "img.jpg")
	expectedBytes := writeJPEG(t, imgPath, 12, 12, color.RGBA{0, 0, 255, 255})

	ws := NewWebServer(nil, relDest)
	imageURL := fmt.Sprintf("/image?path=%s", url.QueryEscape(filepath.ToSlash(imgPath)))
	req := httptest.NewRequest(http.MethodGet, imageURL, nil)
	rr := httptest.NewRecorder()

	ws.handleImageServe(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, expectedBytes, rr.Body.Bytes())
	require.Equal(t, "image/jpeg", strings.Split(rr.Header().Get("Content-Type"), ";")[0])
}

func TestHandleImageServeFallsBackToEmbeddedPreview(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "web_preview_fallback_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	rawPath := filepath.Join(tempDir, "2024", "01", "02", "raw.ARW")
	require.NoError(t, os.MkdirAll(filepath.Dir(rawPath), 0755))
	require.NoError(t, os.WriteFile(rawPath, []byte("raw-bytes"), 0644))

	previewPath := filepath.Join(tempDir, "preview.jpg")
	previewBytes := writeJPEG(t, previewPath, 16, 16, color.RGBA{12, 34, 56, 255})

	ws := NewWebServer(nil, tempDir)
	ws.previewForPath = func(path string) ([]byte, string, error) {
		require.Equal(t, rawPath, path)
		return previewBytes, "image/jpeg", nil
	}

	imageURL := fmt.Sprintf("/image?path=%s", url.QueryEscape(rawPath))
	req := httptest.NewRequest(http.MethodGet, imageURL, nil)
	rr := httptest.NewRecorder()

	ws.handleImageServe(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, previewBytes, rr.Body.Bytes())
	require.Equal(t, "image/jpeg", strings.Split(rr.Header().Get("Content-Type"), ";")[0])
}

func TestHandleGetDuplicatesReturnsPaginationMetadata(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "web_duplicates_pagination_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "cache.db")
	sqliteDB, err := sql.Open("sqlite", dbPath+"?_busy_timeout=5000")
	require.NoError(t, err)
	defer sqliteDB.Close()

	_, err = sqliteDB.Exec(`
		CREATE TABLE file_cache (
			target_path TEXT PRIMARY KEY,
			mmh3_hash TEXT,
			phash TEXT,
			size INTEGER,
			metadata TEXT DEFAULT '{}',
			thumbnails TEXT DEFAULT '[]'
		)
	`)
	require.NoError(t, err)

	_, err = sqliteDB.Exec(`
		INSERT INTO file_cache (target_path, mmh3_hash, phash, size, metadata, thumbnails)
		VALUES
			('b-master.jpg', 'h1', 'p1', 11, '{}', '[{"path":"b-thumb.jpg","metadata":{"size":5}}]'),
			('a-master.jpg', 'h2', 'p2', 22, '{}', '[{"path":"a-thumb.jpg","metadata":{"size":6}}]')
	`)
	require.NoError(t, err)

	ws := NewWebServer(nil, tempDir)
	ws.db = sqliteDB

	req := httptest.NewRequest(http.MethodGet, "/api/duplicates?page=2&limit=1", nil)
	rr := httptest.NewRecorder()

	ws.handleGetDuplicates(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var payload struct {
		Groups     []DuplicateGroup `json:"groups"`
		Page       int              `json:"page"`
		Limit      int              `json:"limit"`
		Total      int              `json:"total"`
		TotalPages int              `json:"totalPages"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &payload))
	require.Equal(t, 2, payload.Page)
	require.Equal(t, 1, payload.Limit)
	require.Equal(t, 2, payload.Total)
	require.Equal(t, 2, payload.TotalPages)
	require.Len(t, payload.Groups, 1)
	require.Equal(t, "b-master.jpg", payload.Groups[0].Master.Path)
}
