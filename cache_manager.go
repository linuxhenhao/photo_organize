package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

// CacheManager handles the MMH3 hash cache operations, managing the
// in-memory map and appending to the persistent cache file.
type CacheManager struct {
	file        *os.File      // The opened cache file (mmh3_hash_cache.txt)
	writer      *bufio.Writer // Buffered writer for appending to the file
	mutex       sync.Mutex    // Mutex to protect concurrent access to file and cache map
	cache       sync.Map      // In-memory cache: mmh3_hash -> targetFilePath
	revertCache sync.Map      // In-memory cache: path -> mmh3_hash
	count       int           // Counter for triggering batch writes/syncs
	batchSize   int           // Configurable batch size for flushing/syncing
}

// NewCacheManager initializes and returns a new CacheManager instance.
// It loads existing entries from the specified cache file and opens it for appending.
func NewCacheManager(cacheFilePath string, batchSize int) (*CacheManager, error) {
	cm := &CacheManager{
		cache:     sync.Map{}, // Initialize the in-memory map
		batchSize: batchSize,  // Set the batch size for periodic flushing
	}

	// 1. Load existing entries into the in-memory cache from the file.
	var loadedCount int
	if _, err := os.Stat(cacheFilePath); err == nil { // Check if the cache file already exists
		file, err := os.Open(cacheFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to open existing hash cache file for reading: %w", err)
		}
		defer file.Close() // Ensure the file is closed after reading

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, ",", 2) // Split only on the first comma
			if len(parts) == 2 {
				cm.cache.Store(parts[0], parts[1]) // Store hash -> target path
				cm.revertCache.Store(parts[1], parts[0])
				loadedCount++
			} else {
				// Log a warning for any lines that don't conform to the expected format.
				log.Printf("Skipping malformed line in hash cache: %s", line)
			}
		}
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("error reading hash cache file: %w", err)
		}
	}
	log.Printf("Loaded %d entries from existing hash cache.", loadedCount)

	// 2. Open the cache file in append mode for writing new entries.
	// os.O_APPEND ensures new writes go to the end of the file.
	// os.O_CREATE creates the file if it doesn't exist.
	// os.O_WRONLY opens the file for writing only.
	var err error
	cm.file, err = os.OpenFile(cacheFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open hash cache file for appending: %w", err)
	}
	// Create a buffered writer for efficient writes to the file.
	cm.writer = bufio.NewWriter(cm.file)
	return cm, nil
}

func (cm *CacheManager) IsCached(targetPath string) bool {
	_, ok := cm.revertCache.Load(targetPath)
	return ok
}

// AddEntry adds a new entry to the in-memory cache and appends it to the buffered writer.
// It also triggers a flush of the buffer and a sync to disk if the processed count
// reaches the configured batch size. This method is safe for concurrent use.
func (cm *CacheManager) AddEntry(hash, targetPath string) {
	cm.mutex.Lock()         // Acquire mutex to protect shared resources
	defer cm.mutex.Unlock() // Release mutex when method exits

	// Store the new entry in the in-memory hashCache.
	cm.cache.Store(hash, targetPath)
	cm.revertCache.Store(targetPath, hash)

	// Format the entry as "hash,target_path\n" and write it to the buffered writer.
	line := fmt.Sprintf("%s,%s\n", hash, targetPath)
	if _, err := cm.writer.WriteString(line); err != nil {
		log.Printf("Failed to write to hash cache file for [%s]: %v", targetPath, err)
	}

	// Increment the internal processed count.
	cm.count++

	// If the count reaches a multiple of the batchSize, flush the buffer
	// and sync the underlying file to disk for persistence.
	if cm.count%cm.batchSize == 0 {
		if err := cm.writer.Flush(); err != nil {
			log.Printf("Failed to flush hash cache writer: %v", err)
		}
		if err := cm.file.Sync(); err != nil { // Ensures data is written to physical disk
			log.Printf("Failed to sync hash cache file: %v", err)
		}
	}
}

// Close performs a final flush of the buffered writer, syncs the file to disk,
// and closes the underlying file handle. This ensures all buffered writes are
// persisted and resources are released. This method is safe for concurrent use.
func (cm *CacheManager) Close() error {
	cm.mutex.Lock()         // Acquire mutex for final operations
	defer cm.mutex.Unlock() // Release mutex

	var finalErr error // To collect any errors during shutdown

	// Flush any remaining data in the buffer to the file.
	if err := cm.writer.Flush(); err != nil {
		log.Printf("Final flush of hash cache writer failed: %v", err)
		finalErr = err
	}
	// Sync the file to ensure all data is written to physical disk.
	if err := cm.file.Sync(); err != nil {
		log.Printf("Final sync of hash cache file failed: %v", err)
		if finalErr == nil { // Only capture if no error has occurred yet
			finalErr = err
		}
	}
	// Close the file handle.
	if err := cm.file.Close(); err != nil {
		log.Printf("Failed to close hash cache file: %v", err)
		if finalErr == nil { // Only capture if no error has occurred yet
			finalErr = err
		}
	}
	return finalErr // Return the first error encountered during closing
}
