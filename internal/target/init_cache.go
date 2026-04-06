package target

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
)

const copyWorkers = 10

// moveFileToThumbnails moves a file retaining its relative structure inside a thumbnails folder.
func moveFileToThumbnails(baseDir, filePath string) string {
	rel, err := filepath.Rel(baseDir, filePath)
	if err != nil {
		log.Printf("Failed to get relative path for %s: %v", filePath, err)
		return filePath
	}
	thumbTarget := filepath.Join(baseDir, "thumbnails", rel)
	os.MkdirAll(filepath.Dir(thumbTarget), 0755)
	
	err = os.Rename(filePath, thumbTarget)
	if err != nil {
		log.Printf("Failed to move %s to thumbnails: %v", filePath, err)
		return filePath
	} else {
		log.Printf("Moved thumbnail-sized duplicate to %s", thumbTarget)
		return thumbTarget
	}
}

// InitTargetDirCache scans a target directory, calculates the hash for each file
// and checks for existing duplicates in the target space.
func InitTargetDirCache(targetDir string, cm *CacheManager) {
	log.Printf("Initializing cache for target directory: %s in parallel...", targetDir)

	pathsToHash := make(chan string, copyWorkers*4)
	var wg sync.WaitGroup

	var processedInInitCount int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(pathsToHash)
		err := filepath.Walk(targetDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if info.IsDir() {
				return nil
			}
			// Skip the thumbnails folder to avoid recursive thumb checking and DB insertion
			if strings.Contains(path, filepath.Join(targetDir, "thumbnails")) || strings.Contains(path, "cache.db") || strings.Contains(path, "mmh3_hash_cache.txt") {
				return nil
			}
			pathsToHash <- path
			return nil
		})
		if err != nil {
			log.Printf("Error walking target directory %s: %v", targetDir, err)
		}
	}()

	hashingWorkers := copyWorkers
	for i := 0; i < hashingWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range pathsToHash {
				info, exists := cm.GetCachedInfo(path)
				
				// Migration check:
				// 1. If it's an image but has no phash -> it's an old entry, re-hash it.
				// 2. If it has no metadata -> it's an old entry, re-extract it.
				needsPHash := exists && info.PHash == "" && hasher.IsImageForPHash(path)
				needsMeta := exists && !info.HasMeta
				
				if exists && !needsPHash && !needsMeta {
					continue
				}

				stat, err := os.Stat(path)
				if err != nil {
					continue
				}
				size := stat.Size()

				fileHash := info.MMH3
				if fileHash == "" {
					fileHash, _ = hasher.CalculateHash(path)
				}
				
				var phash uint64
				var phashErr error = errors.New("not an image")
				if hasher.IsImageForPHash(path) {
					if info.PHash != "" {
						phash, phashErr = hasher.StringToPHash(info.PHash)
					} else {
						phash, phashErr = hasher.CalculatePHash(path)
					}
				}

				// Check for perceptual duplicate in the target folder
				var match *hasher.MatchResult
				if phashErr == nil {
					match = cm.CheckAndAddPerceptualMatch(phash, path, size, fileHash)
				}

				if match != nil {
					// Ensure the match actually exists on disk
					if _, stErr := os.Stat(match.Path); stErr == nil {
						// If sizes differ, the smaller is a thumbnail.
						if size < match.Size {
							thumbPath := moveFileToThumbnails(targetDir, path)
							thumbMeta := metadata.ExtractImageMetaJson(thumbPath)
							// Record link in cache
							cm.AppendThumbnailToMaster(match.Path, thumbPath, thumbMeta)
						} else if size > match.Size {
							// Existing is smaller, move existing
							log.Printf("Found superior duplicate in target. Moving old [%s] to thumbnails and updating cache...", match.Path)
							thumbPath := moveFileToThumbnails(targetDir, match.Path)
							thumbMeta := metadata.ExtractImageMetaJson(thumbPath)
							// Update old from cache to be a thumbnail linking to the new master
							cm.DeleteEntry(match.Path, "")
							masterMeta := metadata.ExtractImageMetaJson(path)
							cm.AddEntry(path, fileHash, phash, size, masterMeta) // Record new as master
							cm.AppendThumbnailToMaster(path, thumbPath, thumbMeta) // Record old as thumbnail of new
						}
					}
				} else if phashErr == nil {
					// Unique image found during scan, record with metadata
					masterMeta := metadata.ExtractImageMetaJson(path)
					cm.AddEntry(path, fileHash, phash, size, masterMeta)
				} else if phashErr != nil {
					// If not an image (no phash), just add it to normal cache as master
					masterMeta := metadata.ExtractImageMetaJson(path)
					cm.AddEntry(path, fileHash, 0, size, masterMeta)
				}

				atomic.AddInt64(&processedInInitCount, 1)
			}
		}()
	}

	wg.Wait()
	log.Printf("Finished initializing cache for target directory. %d files processed.", atomic.LoadInt64(&processedInInitCount))
}
