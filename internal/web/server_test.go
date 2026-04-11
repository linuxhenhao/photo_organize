package web

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"image"
	"image/color"
	"image/jpeg"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

func TestListenAddrUsesLoopback(t *testing.T) {
	require.Equal(t, "127.0.0.1:8080", listenAddr(8080))
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
