package importer

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/linuxhenhao/photo_organize/internal/target"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		// On Windows, filepath.Dir will use backslashes.
		// Normalize to forward slashes for the test logic or use filepath.FromSlash
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

	tasks := make(chan ImportTask, 1)
	var wg sync.WaitGroup
	var successCount int32
	var failCount int32

	wg.Add(1)
	go importWorker(tasks, &wg, &successCount, &failCount, cacheManager)

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
