package target

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheManager_Concurrency(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "photo_organize_concurrency_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cm, err := NewCacheManager(tempDir, 100)
	require.NoError(t, err)
	defer cm.Close()

	const numWorkers = 100
	const phash = uint64(0xDEADBEEF)

	var uniqueCount int32
	var matchCount int32
	var wg sync.WaitGroup

	// Launch 100 workers trying to add the SAME phash simultaneously
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			path := "path.jpg"
			match := cm.CheckAndAddPerceptualMatch(phash, path, 1000, "hash1")
			if match == nil {
				atomic.AddInt32(&uniqueCount, 1)
			} else {
				atomic.AddInt32(&matchCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// CRITICAL: Exactly ONE goroutine should have been told it was unique.
	// All others should have found the first one as a match.
	assert.Equal(t, int32(1), atomic.LoadInt32(&uniqueCount), "Exactly one worker should have added the unique phash")
	assert.Equal(t, int32(numWorkers-1), atomic.LoadInt32(&matchCount), "All other workers should have found a match")
}

func TestBKTree_Concurrency(t *testing.T) {
	cm, err := NewCacheManager(os.TempDir(), 100)
	require.NoError(t, err)
	defer cm.Close()

	const numWorkers = 50
	var wg sync.WaitGroup

	// Stress test parallel adds of DIFFERENT hashes
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cm.AddEntry("path", "", uint64(id), 100, "{}")
			_ = cm.DHashTree.Search(uint64(id), 0)
		}(i)
	}

	wg.Wait()
}
