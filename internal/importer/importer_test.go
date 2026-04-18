package importer

import (
	"image"
	"image/color"
	"image/jpeg"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/linuxhenhao/photo_organize/internal/target"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

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

func writeJPEGFixturePair(t *testing.T, masterPath string, thumbPath string) {
	t.Helper()
	writeSizedJPEGWithQuality(t, masterPath, 96, 72, 90)
	writeSizedJPEGWithQuality(t, thumbPath, 48, 36, 70)
}

func buildTaskFromPath(t *testing.T, sourcePath string, targetDir string) ImportTask {
	t.Helper()

	stat, err := os.Stat(sourcePath)
	require.NoError(t, err)
	mmh3, err := hasher.CalculateHash(sourcePath)
	require.NoError(t, err)
	phash, err := hasher.CalculatePHash(sourcePath)
	require.NoError(t, err)

	return ImportTask{
		SourcePath: sourcePath,
		TargetDir:  targetDir,
		FileName:   filepath.Base(sourcePath),
		Size:       stat.Size(),
		MMH3Hash:   mmh3,
		PHash:      hasher.PHashToString(phash),
	}
}

func TestTargetDirRoot(t *testing.T) {
	tests := []struct {
		target   string
		expected string
	}{
		{
			target:   filepath.FromSlash("/photos/2023/12/20"),
			expected: "/photos",
		},
		{
			target:   filepath.Join("data", "2024", "01", "01"),
			expected: "data",
		},
	}

	for _, tt := range tests {
		got := targetDirRoot(tt.target)
		assert.Equal(t, filepath.Clean(tt.expected), filepath.Clean(got))
	}
}

func TestCopyFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "photo_organize_copy_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	src := filepath.Join(tempDir, "src.txt")
	dst := filepath.Join(tempDir, "dst.txt")
	content := []byte("hello world")

	err = os.WriteFile(src, content, 0644)
	require.NoError(t, err)

	err = copyFile(src, dst)
	require.NoError(t, err)

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	assert.Equal(t, content, got)
}

func TestImportWorkerRollsBackReservedEntryOnCopyFailure(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "photo_organize_import_failure_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cacheManager, err := target.NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cacheManager.Close()
	coordinator := newImportCoordinator(cacheManager)

	tasks := make(chan ImportTask, 1)
	var wg sync.WaitGroup
	var successCount int32
	var failCount int32

	wg.Add(1)
	go importWorker(tasks, &wg, &successCount, &failCount, coordinator)

	tasks <- ImportTask{
		SourcePath: filepath.Join(tempDir, "missing.jpg"),
		TargetDir:  tempDir,
		FileName:   "missing.jpg",
		Size:       42,
		MMH3Hash:   "missing-hash",
		PHash:      "00000000000000ff",
	}
	close(tasks)
	wg.Wait()

	targetPath := filepath.Join(tempDir, "missing.jpg")
	require.Equal(t, int32(0), atomic.LoadInt32(&successCount))
	require.Equal(t, int32(1), atomic.LoadInt32(&failCount))
	require.False(t, cacheManager.IsCached(targetPath))

	_, found := cacheManager.FindExactMatch("missing-hash")
	require.False(t, found)
	require.Empty(t, cacheManager.SearchPHash(0xff, 0))
}

func TestImportCoordinatorWaitsForSimilarInFlightTask(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "photo_organize_import_wait_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cacheManager, err := target.NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cacheManager.Close()

	thumbSource := filepath.Join(tempDir, "source", "thumb.jpg")
	masterSource := filepath.Join(tempDir, "source", "master.jpg")
	writeJPEGFixturePair(t, masterSource, thumbSource)

	coordinator := newImportCoordinator(cacheManager)
	targetDir := filepath.Join(tempDir, "2023", "05", "01")
	thumbTask := buildTaskFromPath(t, thumbSource, targetDir)
	masterTask := buildTaskFromPath(t, masterSource, targetDir)

	firstPlan := coordinator.planTask(thumbTask, metadata.ExtractImageMetaJson(thumbTask.SourcePath))
	require.Equal(t, importPlanCopyMaster, firstPlan.action)

	secondPlan := coordinator.planTask(masterTask, metadata.ExtractImageMetaJson(masterTask.SourcePath))
	require.Equal(t, importPlanCopyMaster, secondPlan.action)
	require.NotNil(t, secondPlan.reservation)

	coordinator.cancelReservation(firstPlan.reservation)
	coordinator.cancelReservation(secondPlan.reservation)
}

func TestImportCoordinatorDropsMissingCommittedExactMatch(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "photo_organize_import_stale_exact_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cacheManager, err := target.NewCacheManager(tempDir, 1)
	require.NoError(t, err)
	defer cacheManager.Close()

	sourcePath := filepath.Join(tempDir, "source", "img.jpg")
	writeSizedJPEGWithQuality(t, sourcePath, 96, 72, 90)
	task := buildTaskFromPath(t, sourcePath, filepath.Join(tempDir, "2023", "05", "01"))
	cacheManager.AddEntryWithPresence(filepath.Join(tempDir, "missing.jpg"), task.MMH3Hash, 0, false, task.Size, "{}")

	coordinator := newImportCoordinator(cacheManager)
	plan := coordinator.planTask(task, metadata.ExtractImageMetaJson(task.SourcePath))
	require.Equal(t, importPlanCopyMaster, plan.action)
	require.NotNil(t, plan.reservation)

	_, found := cacheManager.FindExactMatch(task.MMH3Hash)
	require.False(t, found)

	coordinator.cancelReservation(plan.reservation)
}
