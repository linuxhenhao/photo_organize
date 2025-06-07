package main

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// InitTargetDirCache scans a target directory, calculates the hash for each file
// in parallel, and adds the hash-path pairs to the provided CacheManager.
// This is useful for building the cache from an existing collection of files.
func InitTargetDirCache(targetDir string, cm *CacheManager) {
	log.Printf("Initializing cache for target directory: %s in parallel...", targetDir)

	// Channel to send file paths from the discovery goroutine to hashing workers.
	pathsToHash := make(chan string, copyWorkers*4) // Buffer the channel to avoid blocking producer
	var wg sync.WaitGroup                           // WaitGroup to coordinate discovery and hashing workers

	// Use an atomic counter for thread-safe tracking of processed files.
	var processedInInitCount int64

	// 1. Discovery Goroutine (Producer): Walks the directory and sends file paths to the channel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(pathsToHash) // Close the channel when all paths have been discovered
		err := filepath.Walk(targetDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				log.Printf("Error accessing path %s during walk: %v", path, err)
				return nil // Continue walking even if an error occurs for one path
			}
			if info.IsDir() {
				return nil // Skip directories
			}
			// Only send regular files to the channel.
			pathsToHash <- path
			return nil
		})
		if err != nil {
			log.Printf("Error walking target directory %s: %v", targetDir, err)
		}
	}()

	// 2. Hashing Worker Goroutines (Consumers): Read paths, calculate hashes, and add to cache.
	// The number of hashing workers can be adjusted based on CPU cores and I/O capacity.
	// Using `copyWorkers` here for consistency, but you might want `runtime.NumCPU()` or more.
	hashingWorkers := copyWorkers

	for i := 0; i < hashingWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for path := range pathsToHash {
				if cm.IsCached(path) {
					continue
				}
				// Optimization: Check the in-memory cache before calculating the hash
				// to avoid redundant hash computations for files already known.
				fileHash, hashErr := calculateHash(path)
				if hashErr != nil {
					log.Printf("Failed to calculate hash for file %s: %v. Skipping.", path, hashErr)
					continue
				}

				// Check if this hash is already in the in-memory cache, potentially avoiding an `AddEntry` call
				// if it's already there with the exact path (though `AddEntry` handles idempotency for the map).
				if cachedPath, ok := cm.cache.Load(fileHash); ok {
					if cachedPathStr, isString := cachedPath.(string); isString && cachedPathStr == path {
						// File with this hash and path is already in cache, no need to re-add.
						atomic.AddInt64(&processedInInitCount, 1) // Still count it as processed
						continue
					}
				}

				// If not in cache, or if the cached path differs (e.g., same hash, different file location), add it.
				cm.AddEntry(fileHash, path)
				atomic.AddInt64(&processedInInitCount, 1) // Atomically increment the counter
			}
		}(i)
	}

	wg.Wait() // Wait for all discovery and hashing goroutines to complete.

	log.Printf("Finished initializing cache for target directory. %d files processed.", atomic.LoadInt64(&processedInInitCount))
}
