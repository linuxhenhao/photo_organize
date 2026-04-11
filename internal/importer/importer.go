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
	"github.com/linuxhenhao/photo_organize/internal/dedupe"
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

func resolveAvailableTargetPath(targetDir, fileName string, cacheManager *target.CacheManager) string {
	candidate := filepath.Join(targetDir, fileName)
	if !cacheManager.IsCached(candidate) {
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
	}

	ext := filepath.Ext(fileName)
	nameWithoutExt := strings.TrimSuffix(fileName, ext)
	suffix := 1
	for {
		candidate = filepath.Join(targetDir, fmt.Sprintf("%s-%d%s", nameWithoutExt, suffix, ext))
		if cacheManager.IsCached(candidate) {
			suffix++
			continue
		}
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
		suffix++
	}
}

type confirmedImportMatch struct {
	match           hasher.MatchResult
	preferCandidate bool
}

func findConfirmedImportMatch(task ImportTask, sourceMeta string, cacheManager *target.CacheManager, phash uint64) *confirmedImportMatch {
	matches := cacheManager.SearchPHash(phash, dedupe.CandidateSearchDistance)
	for _, candidate := range matches {
		if _, err := os.Stat(candidate.Path); err != nil {
			log.Printf("Removing stale perceptual match entry for missing path [%s]", candidate.Path)
			cacheManager.DeleteEntry(candidate.Path)
			continue
		}

		existingMeta := metadata.ExtractImageMetaJson(candidate.Path)
		decision, err := dedupe.EvaluateThumbnailMatch(task.SourcePath, sourceMeta, task.Size, candidate.Path, existingMeta, candidate.Size)
		if err != nil {
			if _, statErr := os.Stat(candidate.Path); statErr != nil {
				log.Printf("Removing stale perceptual match entry for missing path [%s]", candidate.Path)
				cacheManager.DeleteEntry(candidate.Path)
				continue
			}
			log.Printf("Failed to confirm visual duplicate [%s] against [%s]: %v", task.SourcePath, candidate.Path, err)
			continue
		}
		if decision.Confirmed {
			return &confirmedImportMatch{
				match:           candidate,
				preferCandidate: decision.PreferCandidate,
			}
		}
	}

	return nil
}

func importWorker(tasks <-chan ImportTask, wg *sync.WaitGroup, successCount *int32, failCount *int32, cacheManager *target.CacheManager) {
	defer wg.Done()
	for task := range tasks {
		finalTargetPath := resolveAvailableTargetPath(task.TargetDir, task.FileName, cacheManager)

		if cacheManager.IsCached(finalTargetPath) {
			continue // Perfect name match cached
		}

		if _, found := cacheManager.FindExactMatch(task.MMH3Hash); found {
			continue // Perfect file match cached
		}

		sourceMeta := ""
		getSourceMeta := func() string {
			if sourceMeta == "" {
				sourceMeta = metadata.ExtractImageMetaJson(task.SourcePath)
			}
			return sourceMeta
		}

		var phashValue uint64
		var hasPHash bool
		var reservedUnique bool
		var match *confirmedImportMatch
		if task.PHash != "" && task.PHash != "UNSUPPORTED" && task.PHash != "NOT_IMAGE" {
			if pv, parseErr := hasher.StringToPHash(task.PHash); parseErr == nil {
				phashValue = pv
				hasPHash = true
				initialMatch := cacheManager.CheckAndAddPerceptualMatchWithPresence(pv, true, finalTargetPath, task.Size, task.MMH3Hash)
				if initialMatch == nil {
					reservedUnique = true
				} else {
					match = findConfirmedImportMatch(task, getSourceMeta(), cacheManager, pv)
					if match == nil {
						cacheManager.AddEntryWithPresence(finalTargetPath, task.MMH3Hash, phashValue, true, task.Size, "{}")
						reservedUnique = true
					}
				}
			}
		}

		if match != nil {
			_, err := os.Stat(match.match.Path)
			if err == nil {
				if match.preferCandidate {
					cacheManager.AddEntryWithPresence(finalTargetPath, task.MMH3Hash, phashValue, true, task.Size, "{}")
					if err := copyFile(task.SourcePath, finalTargetPath); err != nil {
						cacheManager.DeleteEntry(finalTargetPath)
						log.Printf("Failed to import [%s]: %v", task.SourcePath, err)
						atomic.AddInt32(failCount, 1)
						continue
					}

					log.Printf("Found superior confirmed visual duplicate. Promoting [%s] over [%s].", task.SourcePath, match.match.Path)
					destDir := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(match.match.Path))))
					thumbPath := moveFileToThumbnails(destDir, match.match.Path)
					cacheManager.AddEntryWithPresence(finalTargetPath, task.MMH3Hash, phashValue, true, task.Size, getSourceMeta())

					if thumbPath == match.match.Path {
						log.Printf("Keeping both files because old master [%s] could not be moved to thumbnails", match.match.Path)
					} else {
						thumbMeta := metadata.ExtractImageMetaJson(thumbPath)
						cacheManager.DeleteEntry(match.match.Path)
						cacheManager.AppendThumbnailToMaster(finalTargetPath, thumbPath, thumbMeta)
					}
				} else {
					log.Printf("Found confirmed visual duplicate. Rerouting [%s] to thumbnails under [%s].", task.SourcePath, match.match.Path)
					thumbDir := filepath.Join(targetDirRoot(task.TargetDir), "thumbnails", filepath.Base(filepath.Dir(filepath.Dir(task.TargetDir))), filepath.Base(filepath.Dir(task.TargetDir)), filepath.Base(task.TargetDir))
					if err := os.MkdirAll(thumbDir, 0755); err != nil {
						log.Printf("Failed to create thumb dir: %v", err)
						continue
					}
					finalTargetPath = resolveAvailableTargetPath(thumbDir, task.FileName, cacheManager)

					if err := copyFile(task.SourcePath, finalTargetPath); err != nil {
						log.Printf("Failed to import [%s]: %v", task.SourcePath, err)
						atomic.AddInt32(failCount, 1)
						continue
					}

					cacheManager.AppendThumbnailToMaster(match.match.Path, finalTargetPath, getSourceMeta())
				}
			} else {
				log.Printf("Failed to stat perceptual match [%s]: %v", match.match.Path, err)
				atomic.AddInt32(failCount, 1)
				continue
			}
		} else {
			if finalTargetPath != filepath.Join(task.TargetDir, task.FileName) {
				log.Printf("Conflict resolved for [%s], using new name [%s]", task.SourcePath, filepath.Base(finalTargetPath))
			}

			if !hasPHash {
				cacheManager.AddEntry(finalTargetPath, task.MMH3Hash, 0, task.Size, "{}")
				reservedUnique = true
			}
		}

		if match == nil {
			if err := copyFile(task.SourcePath, finalTargetPath); err != nil {
				if reservedUnique {
					cacheManager.DeleteEntry(finalTargetPath)
				}
				log.Printf("Failed to import [%s]: %v", task.SourcePath, err)
				atomic.AddInt32(failCount, 1)
				continue
			}

			cacheManager.AddEntryWithPresence(finalTargetPath, task.MMH3Hash, phashValue, hasPHash, task.Size, getSourceMeta())
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
