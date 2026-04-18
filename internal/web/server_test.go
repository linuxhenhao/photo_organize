package web

import (
	"archive/tar"
	"bytes"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

func TestValidateListenHostRejectsUnspecifiedAddresses(t *testing.T) {
	for _, host := range []string{"0.0.0.0", "::", "[::]"} {
		err := validateListenHost(host)
		require.Error(t, err, host)
	}
}

func TestValidateListenHostAllowsScopedAddresses(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "192.168.1.10", "localhost"} {
		err := validateListenHost(host)
		require.NoError(t, err, host)
	}
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
	require.NotEmpty(t, rr.Header().Get("X-Resolve-Request-Id"))

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

func TestHandleResolveGroupReportsResolveRequestIDOnBadRequest(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "web_resolve_bad_request_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	ws := NewWebServer(nil, tempDir)

	req := httptest.NewRequest(http.MethodPost, "/api/resolve", strings.NewReader(`{"masterPath":""}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	ws.handleResolveGroup(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)
	require.NotEmpty(t, rr.Header().Get("X-Resolve-Request-Id"))
	require.Contains(t, rr.Body.String(), "resolve_id=")
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

func TestHandleResolveGroupSerializesConcurrentDBWrites(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "web_resolve_concurrent_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cacheManager, err := target.NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cacheManager.Close()

	dbPath := filepath.Join(tempDir, "cache.db")
	sqliteDB, err := sql.Open("sqlite", dbPath+"?_busy_timeout=1")
	require.NoError(t, err)
	defer sqliteDB.Close()

	type groupFixture struct {
		masterPath string
		thumbPath  string
	}

	makeGroup := func(name string, masterFill color.RGBA, thumbFill color.RGBA) groupFixture {
		masterPath := filepath.Join(tempDir, "2024", "01", "02", name+"-master.jpg")
		thumbPath := filepath.Join(tempDir, "thumbnails", "2024", "01", "02", name+"-thumb.jpg")

		writeJPEG(t, masterPath, 24, 24, masterFill)
		writeJPEG(t, thumbPath, 20, 20, thumbFill)

		masterStat, err := os.Stat(masterPath)
		require.NoError(t, err)
		masterMeta := metadata.ExtractImageMetaJson(masterPath)
		thumbMeta := metadata.ExtractImageMetaJson(thumbPath)

		_, err = sqliteDB.Exec(`INSERT INTO file_cache (target_path, mmh3_hash, phash, size, metadata, thumbnails) VALUES (?, ?, ?, ?, ?, ?)`,
			masterPath, "old-hash-"+name, "0000000000000001", masterStat.Size(), masterMeta,
			`[{"path":"`+thumbPath+`","metadata":`+thumbMeta+`}]`)
		require.NoError(t, err)
		cacheManager.SetEntryMemory(masterPath, "old-hash-"+name, 1, masterStat.Size(), masterMeta)

		return groupFixture{
			masterPath: masterPath,
			thumbPath:  thumbPath,
		}
	}

	groupA := makeGroup("a", color.RGBA{255, 0, 0, 255}, color.RGBA{0, 255, 0, 255})
	groupB := makeGroup("b", color.RGBA{0, 0, 255, 255}, color.RGBA{255, 255, 0, 255})

	ws := NewWebServer(cacheManager, tempDir)
	ws.db = sqliteDB
	ws.resolveDBWriteHook = func() {
		time.Sleep(50 * time.Millisecond)
	}

	runResolve := func(masterPath string) *httptest.ResponseRecorder {
		body, err := json.Marshal(map[string]any{
			"keepPaths":  []string{masterPath},
			"masterPath": masterPath,
		})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, "/api/resolve", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		ws.handleResolveGroup(rr, req)
		return rr
	}

	start := make(chan struct{})
	results := make(chan *httptest.ResponseRecorder, 2)

	go func() {
		<-start
		results <- runResolve(groupA.masterPath)
	}()
	go func() {
		<-start
		results <- runResolve(groupB.masterPath)
	}()
	close(start)

	rr1 := <-results
	rr2 := <-results

	require.Equal(t, http.StatusOK, rr1.Code, rr1.Body.String())
	require.Equal(t, http.StatusOK, rr2.Code, rr2.Body.String())

	for _, group := range []groupFixture{groupA, groupB} {
		_, err = os.Stat(group.masterPath)
		require.NoError(t, err)
		_, err = os.Stat(group.thumbPath)
		require.True(t, os.IsNotExist(err))

		var thumbnails string
		err = sqliteDB.QueryRow(`SELECT thumbnails FROM file_cache WHERE target_path = ?`, group.masterPath).Scan(&thumbnails)
		require.NoError(t, err)
		require.Equal(t, "[]", thumbnails)
	}
}

func TestHandleGroupArchiveDownloadIncludesManifestAndFiles(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "web_group_archive_test")
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

	masterPath := filepath.Join(tempDir, "2024", "01", "02", "master.jpg")
	dupPath := filepath.Join(tempDir, "2024", "01", "02", "dup.jpg")
	masterBytes := writeJPEG(t, masterPath, 12, 12, color.RGBA{255, 0, 0, 255})
	dupBytes := writeJPEG(t, dupPath, 10, 10, color.RGBA{0, 255, 0, 255})

	_, err = sqliteDB.Exec(`INSERT INTO file_cache (target_path, mmh3_hash, phash, size, metadata, thumbnails) VALUES (?, ?, ?, ?, ?, ?)`,
		masterPath, "master-hash", "", int64(len(masterBytes)), "{}",
		`[{"path":"`+dupPath+`","metadata":{"size":`+fmt.Sprintf("%d", len(dupBytes))+`}}]`)
	require.NoError(t, err)

	ws := NewWebServer(nil, tempDir)
	ws.db = sqliteDB

	req := httptest.NewRequest(http.MethodGet, "/api/group-archive?masterPath="+url.QueryEscape(masterPath), nil)
	rr := httptest.NewRecorder()

	ws.handleGroupArchiveDownload(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "application/x-tar", rr.Header().Get("Content-Type"))
	require.Contains(t, rr.Header().Get("Content-Disposition"), "attachment;")

	tr := tar.NewReader(bytes.NewReader(rr.Body.Bytes()))
	entries := map[string][]byte{}
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		content, err := io.ReadAll(tr)
		require.NoError(t, err)
		entries[header.Name] = content
	}

	require.Contains(t, entries, "manifest.json")
	require.Contains(t, entries, "2024/01/02/master.jpg")
	require.Contains(t, entries, "2024/01/02/dup.jpg")
	require.Equal(t, masterBytes, entries["2024/01/02/master.jpg"])
	require.Equal(t, dupBytes, entries["2024/01/02/dup.jpg"])

	var manifest groupArchiveManifest
	require.NoError(t, json.Unmarshal(entries["manifest.json"], &manifest))
	require.Equal(t, masterPath, manifest.MasterPath)
	require.Len(t, manifest.Members, 2)
	require.Equal(t, "2024/01/02/master.jpg", manifest.Members[0].ArchivePath)
	require.True(t, manifest.Members[0].IsMaster)
	require.Equal(t, "2024/01/02/dup.jpg", manifest.Members[1].ArchivePath)
	require.False(t, manifest.Members[1].IsMaster)
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

func TestHandleImageServePrefersGeneratedThumbnail(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "web_generated_thumbnail_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	originalPath := filepath.Join(tempDir, "2024", "01", "02", "original.jpg")
	originalBytes := writeJPEG(t, originalPath, 48, 48, color.RGBA{200, 0, 0, 255})

	thumbnailBase := filepath.Join(tempDir, "thumb-cache", "abcdef")
	thumbnailPath := thumbnailBase + "_640_40.jpg"
	thumbnailBytes := writeJPEG(t, thumbnailPath, 24, 24, color.RGBA{0, 200, 0, 255})

	ws := NewWebServer(nil, tempDir)
	ws.thumbnailForPath = func(path string) string {
		require.Equal(t, originalPath, path)
		return thumbnailBase
	}
	ws.thumbnailCandidates = []string{"_640_40.jpg"}

	imageURL := fmt.Sprintf("/image?path=%s", url.QueryEscape(originalPath))
	req := httptest.NewRequest(http.MethodGet, imageURL, nil)
	rr := httptest.NewRecorder()

	ws.handleImageServe(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, thumbnailBytes, rr.Body.Bytes())
	require.NotEqual(t, originalBytes, rr.Body.Bytes())
	require.Equal(t, "image/jpeg", strings.Split(rr.Header().Get("Content-Type"), ";")[0])
}

func TestHandleImageServePrefersUGOSXattrThumbnail(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "web_ugos_thumbnail_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	originalPath := filepath.Join(tempDir, "2024", "01", "02", "original.jpg")
	originalBytes := writeJPEG(t, originalPath, 48, 48, color.RGBA{200, 0, 0, 255})

	thumbnailDir := filepath.Join(tempDir, "@thumbnail", "91", "98")
	stem := "f97f3173c3fb1b618dc4b2c62ecd2d30"
	thumbnailPath := filepath.Join(thumbnailDir, stem+"_640_40.jpg")
	thumbnailBytes := writeJPEG(t, thumbnailPath, 24, 24, color.RGBA{0, 200, 0, 255})

	ws := NewWebServer(nil, tempDir)
	ws.ugosThumbnailMode = true
	ws.xattrForPath = func(path string, name string) (string, error) {
		require.Equal(t, originalPath, path)
		switch name {
		case "user.thumb.dir":
			return thumbnailDir, nil
		case "user.thumb.id":
			return stem + "-1749255935-675edb", nil
		default:
			return "", fmt.Errorf("unexpected xattr %s", name)
		}
	}
	ws.thumbnailForPath = func(string) string {
		t.Fatal("legacy thumbnail lookup should not run in UGOS thumbnail mode")
		return ""
	}

	imageURL := fmt.Sprintf("/image?path=%s", url.QueryEscape(originalPath))
	req := httptest.NewRequest(http.MethodGet, imageURL, nil)
	rr := httptest.NewRecorder()

	ws.handleImageServe(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, thumbnailBytes, rr.Body.Bytes())
	require.NotEqual(t, originalBytes, rr.Body.Bytes())
	require.Equal(t, "image/jpeg", strings.Split(rr.Header().Get("Content-Type"), ";")[0])
}

func TestHandleImageServeFallsBackToOriginalWhenUGOSThumbnailMissing(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "web_ugos_thumbnail_fallback_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	originalPath := filepath.Join(tempDir, "2024", "01", "02", "original.jpg")
	originalBytes := writeJPEG(t, originalPath, 48, 48, color.RGBA{200, 0, 0, 255})

	thumbnailDir := filepath.Join(tempDir, "@thumbnail", "91", "98")
	require.NoError(t, os.MkdirAll(thumbnailDir, 0755))

	ws := NewWebServer(nil, tempDir)
	ws.ugosThumbnailMode = true
	ws.xattrForPath = func(path string, name string) (string, error) {
		require.Equal(t, originalPath, path)
		switch name {
		case "user.thumb.dir":
			return thumbnailDir, nil
		case "user.thumb.id":
			return "f97f3173c3fb1b618dc4b2c62ecd2d30-1749255935-675edb", nil
		default:
			return "", fmt.Errorf("unexpected xattr %s", name)
		}
	}
	ws.thumbnailForPath = func(string) string {
		t.Fatal("legacy thumbnail lookup should not run in UGOS thumbnail mode")
		return ""
	}

	imageURL := fmt.Sprintf("/image?path=%s", url.QueryEscape(originalPath))
	req := httptest.NewRequest(http.MethodGet, imageURL, nil)
	rr := httptest.NewRecorder()

	ws.handleImageServe(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, originalBytes, rr.Body.Bytes())
	require.Equal(t, "image/jpeg", strings.Split(rr.Header().Get("Content-Type"), ";")[0])
}

func TestSynologyThumbnailBasePathFor(t *testing.T) {
	path := "/volume4/photos/album/img.jpg"
	basePath := synologyThumbnailBasePathFor(path)
	require.NotEmpty(t, basePath)

	digest := md5.Sum([]byte(filepath.Clean(path)))
	hashHex := hex.EncodeToString(digest[:])
	expected := filepath.Join("/volume4", "@thumbnail", hashHex[:2], hashHex[2:4], hashHex)

	require.Equal(t, expected, basePath)
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

func TestHandleGetDuplicatesAllowsManualPageSizeWithinMax(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "web_duplicates_page_size_test")
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
			('a-master.jpg', 'h1', 'p1', 11, '{}', '[{"path":"a-thumb.jpg","metadata":{"size":5}}]'),
			('b-master.jpg', 'h2', 'p2', 22, '{}', '[{"path":"b-thumb.jpg","metadata":{"size":6}}]')
	`)
	require.NoError(t, err)

	ws := NewWebServer(nil, tempDir)
	ws.db = sqliteDB

	req := httptest.NewRequest(http.MethodGet, "/api/duplicates?page=1&limit=777", nil)
	rr := httptest.NewRecorder()

	ws.handleGetDuplicates(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var payload struct {
		Limit int `json:"limit"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &payload))
	require.Equal(t, 777, payload.Limit)
}

func TestHandleGetDuplicatesPrefersRawKeepPathWhenResolutionIsWithinTolerance(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "web_duplicates_preferred_keep_test")
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
		VALUES (?, ?, ?, ?, ?, ?)
	`,
		"master.jpg",
		"h1",
		"p1",
		8_000_000,
		`{"width":5208,"height":3476,"size":8000000}`,
		`[{"path":"shot.CR2","metadata":{"width":5184,"height":3456,"size":2000000}}]`,
	)
	require.NoError(t, err)

	ws := NewWebServer(nil, tempDir)
	ws.db = sqliteDB

	req := httptest.NewRequest(http.MethodGet, "/api/duplicates?page=1&limit=50", nil)
	rr := httptest.NewRecorder()

	ws.handleGetDuplicates(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var payload struct {
		Groups []DuplicateGroup `json:"groups"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &payload))
	require.Len(t, payload.Groups, 1)
	require.Equal(t, "master.jpg", payload.Groups[0].Master.Path)
	require.Equal(t, "shot.CR2", payload.Groups[0].PreferredKeepPath)
}

func TestPrewarmThumbnailPathsRunsConcurrently(t *testing.T) {
	ws := NewWebServer(nil, t.TempDir())
	ws.prewarmWorkers = 4

	var active atomic.Int32
	var maxActive atomic.Int32
	ws.thumbnailPathFor = func(path string) string {
		current := active.Add(1)
		defer active.Add(-1)

		for {
			prev := maxActive.Load()
			if current <= prev || maxActive.CompareAndSwap(prev, current) {
				break
			}
		}

		time.Sleep(25 * time.Millisecond)
		return path
	}

	ws.prewarmThumbnailPaths([]string{"a", "b", "c", "d"})

	require.Greater(t, maxActive.Load(), int32(1))
}
