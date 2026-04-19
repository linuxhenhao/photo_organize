package target

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/color"
	"image/jpeg"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/stretchr/testify/require"
)

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func writePatternJPEG(t *testing.T, path string, width int, height int, variant int) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))

	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			switch variant {
			case 1:
				img.SetRGBA(x, y, color.RGBA{
					R: uint8((x * 255) / max(width-1, 1)),
					G: uint8((y * 255) / max(height-1, 1)),
					B: uint8(((x + y) * 255) / max(width+height-2, 1)),
					A: 255,
				})
			default:
				if ((x/8)+(y/8))%2 == 0 {
					img.SetRGBA(x, y, color.RGBA{R: 230, G: 40, B: 40, A: 255})
				} else {
					img.SetRGBA(x, y, color.RGBA{R: 30, G: 30, B: 220, A: 255})
				}
			}
		}
	}

	file, err := os.Create(path)
	require.NoError(t, err)
	defer file.Close()

	require.NoError(t, jpeg.Encode(file, img, &jpeg.Options{Quality: 90}))
}

func copyFileBytes(t *testing.T, src string, dst string) {
	t.Helper()

	data, err := os.ReadFile(src)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0755))
	require.NoError(t, os.WriteFile(dst, data, 0644))
}

func insertCachedMaster(t *testing.T, cm *CacheManager, path string, thumbnails string) {
	t.Helper()

	stat, err := os.Stat(path)
	require.NoError(t, err)

	file, err := buildTargetFile(path, stat, fileCacheRow{}, false)
	require.NoError(t, err)

	_, err = cm.db.Exec(
		`INSERT INTO file_cache (target_path, mmh3_hash, dhash, size, metadata, thumbnails) VALUES (?, ?, ?, ?, ?, ?)`,
		path,
		file.MMH3,
		file.DHashStr,
		file.Size,
		file.Metadata,
		thumbnails,
	)
	require.NoError(t, err)

	cm.SetEntryMemoryWithPresence(path, file.MMH3, file.DHash, file.HasDHash, file.Size, file.Metadata)
}

func captureLogs(t *testing.T, fn func()) string {
	t.Helper()

	var buf bytes.Buffer
	originalWriter := log.Writer()
	originalFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer log.SetOutput(originalWriter)
	defer log.SetFlags(originalFlags)

	fn()

	return buf.String()
}

func TestCleanThumbnailGroupsRehomesInvalidThumbnailToExistingMaster(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "clean_groups_rehome")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	masterAPath := filepath.Join(tempDir, "album-a", "master-a.jpg")
	masterBPath := filepath.Join(tempDir, "album-b", "master-b.jpg")
	thumbPath := filepath.Join(tempDir, "thumbnails", "wrong-group", "thumb.jpg")

	writePatternJPEG(t, masterAPath, 96, 72, 0)
	writePatternJPEG(t, masterBPath, 96, 72, 1)
	copyFileBytes(t, masterBPath, thumbPath)

	thumbMeta := metadata.ExtractImageMetaJson(thumbPath)
	thumbsJSON := marshalThumbnailEntries([]thumbnailEntry{makeThumbnailEntry(thumbPath, "", thumbMeta)})

	cm, err := NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cm.Close()

	insertCachedMaster(t, cm, masterAPath, thumbsJSON)
	insertCachedMaster(t, cm, masterBPath, "[]")

	report, err := CleanThumbnailGroupsWithContext(context.Background(), tempDir, cm, CleanGroupsOptions{Apply: true})
	require.NoError(t, err)
	require.Equal(t, 1, report.GroupsChanged)
	require.Equal(t, 1, report.ThumbnailsRehomed)
	require.Equal(t, 0, report.StandaloneCreated)

	var aThumbsRaw string
	err = cm.db.QueryRow(`SELECT thumbnails FROM file_cache WHERE target_path = ?`, masterAPath).Scan(&aThumbsRaw)
	require.NoError(t, err)
	require.Equal(t, "[]", aThumbsRaw)

	var bThumbsRaw string
	err = cm.db.QueryRow(`SELECT thumbnails FROM file_cache WHERE target_path = ?`, masterBPath).Scan(&bThumbsRaw)
	require.NoError(t, err)

	var bThumbs []thumbnailEntry
	require.NoError(t, json.Unmarshal([]byte(bThumbsRaw), &bThumbs))
	require.Len(t, bThumbs, 1)
	require.Equal(t, thumbPath, bThumbs[0].Path)

	var thumbRowCount int
	err = cm.db.QueryRow(`SELECT COUNT(*) FROM file_cache WHERE target_path = ?`, thumbPath).Scan(&thumbRowCount)
	require.NoError(t, err)
	require.Equal(t, 0, thumbRowCount)
}

func TestCleanThumbnailGroupsDryRunDoesNotPersistChanges(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "clean_groups_dry_run")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	masterAPath := filepath.Join(tempDir, "album-a", "master-a.jpg")
	masterBPath := filepath.Join(tempDir, "album-b", "master-b.jpg")
	thumbPath := filepath.Join(tempDir, "thumbnails", "wrong-group", "thumb.jpg")

	writePatternJPEG(t, masterAPath, 96, 72, 0)
	writePatternJPEG(t, masterBPath, 96, 72, 1)
	copyFileBytes(t, masterBPath, thumbPath)

	thumbMeta := metadata.ExtractImageMetaJson(thumbPath)
	thumbsJSON := marshalThumbnailEntries([]thumbnailEntry{makeThumbnailEntry(thumbPath, "", thumbMeta)})

	cm, err := NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cm.Close()

	insertCachedMaster(t, cm, masterAPath, thumbsJSON)
	insertCachedMaster(t, cm, masterBPath, "[]")

	report, err := CleanThumbnailGroupsWithContext(context.Background(), tempDir, cm, CleanGroupsOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, report.GroupsChanged)
	require.Equal(t, 1, report.ThumbnailsRehomed)

	var aThumbsRaw string
	err = cm.db.QueryRow(`SELECT thumbnails FROM file_cache WHERE target_path = ?`, masterAPath).Scan(&aThumbsRaw)
	require.NoError(t, err)
	require.Equal(t, thumbsJSON, aThumbsRaw)

	var bThumbsRaw string
	err = cm.db.QueryRow(`SELECT thumbnails FROM file_cache WHERE target_path = ?`, masterBPath).Scan(&bThumbsRaw)
	require.NoError(t, err)
	require.Equal(t, "[]", bThumbsRaw)
}

func TestCleanThumbnailGroupsRestoresStandaloneWhenNoExistingMasterMatches(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "clean_groups_standalone")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	masterPath := filepath.Join(tempDir, "album-a", "master.jpg")
	thumbPath := filepath.Join(tempDir, "thumbnails", "orphan", "thumb.jpg")

	writePatternJPEG(t, masterPath, 96, 72, 0)
	writePatternJPEG(t, thumbPath, 120, 90, 1)

	thumbMeta := metadata.ExtractImageMetaJson(thumbPath)
	thumbsJSON := marshalThumbnailEntries([]thumbnailEntry{makeThumbnailEntry(thumbPath, "", thumbMeta)})

	cm, err := NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cm.Close()

	insertCachedMaster(t, cm, masterPath, thumbsJSON)

	report, err := CleanThumbnailGroupsWithContext(context.Background(), tempDir, cm, CleanGroupsOptions{Apply: true})
	require.NoError(t, err)
	require.Equal(t, 1, report.StandaloneCreated)

	var thumbRowCount int
	err = cm.db.QueryRow(`SELECT COUNT(*) FROM file_cache WHERE target_path = ?`, thumbPath).Scan(&thumbRowCount)
	require.NoError(t, err)
	require.Equal(t, 1, thumbRowCount)

	thumbHash, err := hasher.CalculateHash(thumbPath)
	require.NoError(t, err)

	foundPath, found := cm.FindExactMatch(thumbHash)
	require.True(t, found)
	require.Equal(t, thumbPath, foundPath)
}

func TestCleanThumbnailGroupsStandaloneLogIncludesCaseDetails(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "clean_groups_standalone_log")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	masterPath := filepath.Join(tempDir, "album-a", "master.jpg")
	thumbPath := filepath.Join(tempDir, "thumbnails", "orphan", "thumb.jpg")

	writePatternJPEG(t, masterPath, 96, 72, 0)
	writePatternJPEG(t, thumbPath, 120, 90, 1)

	thumbMeta := metadata.ExtractImageMetaJson(thumbPath)
	thumbsJSON := marshalThumbnailEntries([]thumbnailEntry{makeThumbnailEntry(thumbPath, "", thumbMeta)})

	cm, err := NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cm.Close()

	insertCachedMaster(t, cm, masterPath, thumbsJSON)

	logs := captureLogs(t, func() {
		_, err = CleanThumbnailGroupsWithContext(context.Background(), tempDir, cm, CleanGroupsOptions{})
	})
	require.NoError(t, err)

	require.Contains(t, logs, `cleangroups: event="standalone"`)
	require.Contains(t, logs, `action="restore_standalone"`)
	require.Contains(t, logs, `mode="dry-run"`)
	require.Contains(t, logs, `source_master="`+masterPath+`"`)
	require.Contains(t, logs, `path="`+thumbPath+`"`)
	require.Contains(t, logs, `rehome_reason="no_match_found"`)
	require.True(t, strings.Contains(logs, `dimensions="120x90"`) || strings.Contains(logs, `dimensions="90x120"`))
}
