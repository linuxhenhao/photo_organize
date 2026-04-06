package importer

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linuxhenhao/photo_organize/internal/db"
	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/linuxhenhao/photo_organize/internal/target"
)

const copyWorkers = 10

type ImportTask struct {
	SourcePath string
	TargetDir  string
	FileName   string
	Size       int64
	MMH3Hash   string
	PHash      string
}

// moveFileToThumbnails moves a file preserving its relative target path structure under "thumbnails"
func moveFileToThumbnails(baseDir, filePath string) string {
	rel, err := filepath.Rel(baseDir, filePath)
	if err != nil {
		log.Printf("Failed to resolve relative path for thumbnail fallback: %s", filePath)
		return filePath
	}
	thumbDir := filepath.Join(baseDir, "thumbnails", filepath.Dir(rel))
	if err := os.MkdirAll(thumbDir, 0755); err != nil {
		log.Printf("Failed to create thumbnail wrapper dir %s: %v", thumbDir, err)
		return filePath
	}
	newPath := filepath.Join(thumbDir, filepath.Base(filePath))
	err = os.Rename(filePath, newPath)
	if err != nil {
		log.Printf("Failed to move thumbnail %s -> %s: %v", filePath, newPath, err)
		return filePath
	}
	log.Printf("Moved smaller identical visual to thumbnails: %s", newPath)
	return newPath
}

// copyFile copies a single file from src to dst.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if e := out.Close(); e != nil && err == nil {
			err = e
		}
	}()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}

	err = out.Sync()
	return err
}

func importWorker(tasks <-chan ImportTask, wg *sync.WaitGroup, successCount *int32, failCount *int32, cacheManager *target.CacheManager) {
	defer wg.Done()
	for task := range tasks {
		finalTargetPath := filepath.Join(task.TargetDir, task.FileName)

		if cacheManager.IsCached(finalTargetPath) {
			continue // Perfect name match cached
		}

		if _, found := cacheManager.FindExactMatch(task.MMH3Hash); found {
			continue // Perfect file match cached
		}

		// Optimization 2: Perceptual Deduplication using PHash
		// If we encounter a visually similar image, we need to decide what to do.
		var match *hasher.MatchResult
		if task.PHash != "" && task.PHash != "UNSUPPORTED" && task.PHash != "NOT_IMAGE" {
			if pv, parseErr := hasher.StringToPHash(task.PHash); parseErr == nil && pv != 0 {
				match = cacheManager.CheckAndAddPerceptualMatch(pv, finalTargetPath, task.Size, task.MMH3Hash)
			}
		}

		if match != nil {
			// Compare size. Keep the larger one as original.
			existingStat, err := os.Stat(match.Path)
			if err == nil {
				if task.Size > existingStat.Size() {
					log.Printf("Found superior visual duplicate. Source is larger. Moving old [%s] to thumbnails and replacing...", match.Path)
					destDir := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(match.Path))))
					thumbPath := moveFileToThumbnails(destDir, match.Path)
					thumbMeta := metadata.ExtractImageMetaJson(thumbPath)
					
					// Since we replaced it, add the new one as the primary.
					pv, _ := hasher.StringToPHash(task.PHash)
					masterMeta := metadata.ExtractImageMetaJson(task.SourcePath)
					cacheManager.AddEntry(finalTargetPath, task.MMH3Hash, pv, task.Size, masterMeta)
					// Additionally, link the old thumbnail to this new primary
					cacheManager.AppendThumbnailToMaster(finalTargetPath, thumbPath, thumbMeta)
				} else {
					// new coming file is smaller or equal -> it is the thumbnail. Move it immediately to thumbnails structure locally.
					log.Printf("Found visual duplicate. Source [%s] is smaller. Rerouting to thumbnails dir...", task.SourcePath)
					thumbDir := filepath.Join(targetDirRoot(task.TargetDir), "thumbnails", filepath.Base(filepath.Dir(filepath.Dir(task.TargetDir))), filepath.Base(filepath.Dir(task.TargetDir)), filepath.Base(task.TargetDir))
					if err := os.MkdirAll(thumbDir, 0755); err != nil {
						log.Printf("Failed to create thumb dir: %v", err)
						continue
					}
					finalTargetPath = filepath.Join(thumbDir, task.FileName)
					
					// Record thumbnail in cache via append to original
					thumbMeta := metadata.ExtractImageMetaJson(task.SourcePath)
					cacheManager.AppendThumbnailToMaster(match.Path, finalTargetPath, thumbMeta)
				}
			}
		} else {
			// No perceptual match, handle standard naming conflict if path already exists from other worker
			if _, err := os.Stat(finalTargetPath); err == nil {
				ext := filepath.Ext(task.FileName)
				nameWithoutExt := strings.TrimSuffix(task.FileName, ext)
				suffix := 1
				for {
					newFileName := fmt.Sprintf("%s-%d%s", nameWithoutExt, suffix, ext)
					finalTargetPath = filepath.Join(task.TargetDir, newFileName)
					if _, err := os.Stat(finalTargetPath); os.IsNotExist(err) {
						if !cacheManager.IsCached(finalTargetPath) {
							break
						}
					}
					suffix++
				}
				log.Printf("Conflict resolved for [%s], using new name [%s]", task.SourcePath, filepath.Base(finalTargetPath))
			}
			
			// Different perceptual hash -> keep as unique file
			pv, _ := hasher.StringToPHash(task.PHash)
			masterMeta := metadata.ExtractImageMetaJson(task.SourcePath)
			cacheManager.AddEntry(finalTargetPath, task.MMH3Hash, pv, task.Size, masterMeta)
		}

		if err := copyFile(task.SourcePath, finalTargetPath); err != nil {
			log.Printf("Failed to import [%s]: %v", task.SourcePath, err)
			atomic.AddInt32(failCount, 1)
			continue
		}

		log.Printf("Successfully imported: [%s] -> [%s]", task.SourcePath, finalTargetPath)
		atomic.AddInt32(successCount, 1)
	}
}

// targetDirRoot extracts the root 'destDir' by trimming YYYY/MM/DD
func targetDirRoot(targetDir string) string {
	return filepath.Dir(filepath.Dir(filepath.Dir(targetDir)))
}

// HandleImport manages the photo import process from a SQLite database
// to a destination directory, using caches for efficiency.
func HandleImport(dbPath string, destDir string) {
	log.Printf("Importing using db<%s> to destDir<%s>\n", dbPath, destDir)
	sqliteDB, err := sql.Open("sqlite", dbPath+"?_busy_timeout=5000")
	if err != nil {
		log.Fatalf("Failed to open database '%s': %v", dbPath, err)
	}
	defer sqliteDB.Close()

	if err := os.MkdirAll(destDir, 0755); err != nil {
		log.Fatalf("Failed to create destination directory [%s]: %v", destDir, err)
	}

	cacheManager, err := target.NewCacheManager(destDir, 100)
	if err != nil {
		log.Fatalf("Failed to create CacheManager: %v", err)
	}
	defer cacheManager.Close()

	if err := db.InitDB(sqliteDB); err != nil {
		log.Fatalf("Warning: couldn't apply db migrations during import: %v", err)
	}

	tasks := make(chan ImportTask, copyWorkers*2)
	var wg sync.WaitGroup
	var successCount, failCount int32

	for i := 0; i < copyWorkers; i++ {
		wg.Add(1)
		go importWorker(tasks, &wg, &successCount, &failCount, cacheManager)
	}

	rows, err := sqliteDB.Query(`
        SELECT p.source_path, p.size, p.create_time, p.mmh3_hash, p.phash
        FROM photos p
        JOIN (
            SELECT group_id, MIN(source_path) as min_source_path
            FROM photos
            GROUP BY group_id
        ) m ON p.group_id = m.group_id AND p.source_path = m.min_source_path
    `)
	if err != nil {
		log.Fatalf("Failed to query database for import: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var sourcePath, createTimeStr, mmh3Hash, phash string
		var size int64
		if err := rows.Scan(&sourcePath, &size, &createTimeStr, &mmh3Hash, &phash); err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		if mmh3Hash == "" {
			log.Printf("Warning: Skipping [%s] because hash is empty. Run scan again to update hashes.", sourcePath)
			continue
		}

		createTime, err := time.Parse(time.RFC3339, createTimeStr)
		if err != nil {
			log.Printf("Failed to parse create time for [%s], skip: %v", sourcePath, err)
			continue
		}

		yearDir := fmt.Sprintf("%04d", createTime.Year())
		monthDir := fmt.Sprintf("%02d", createTime.Month())
		dayDir := fmt.Sprintf("%02d", createTime.Day())

		targetDirFull := filepath.Join(destDir, yearDir, monthDir, dayDir)
		if err := os.MkdirAll(targetDirFull, 0755); err != nil {
			log.Printf("Failed to create directory [%s]: %v", targetDirFull, err)
			continue
		}

		fileName := filepath.Base(sourcePath)

		tasks <- ImportTask{
			SourcePath: sourcePath,
			TargetDir:  targetDirFull,
			FileName:   fileName,
			Size:       size,
			MMH3Hash:   mmh3Hash,
			PHash:      phash,
		}
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating over database rows: %v", err)
	}

	close(tasks)
	wg.Wait()

	log.Printf("Import finished. Success: %d, Failed: %d\n", successCount, failCount)
}
