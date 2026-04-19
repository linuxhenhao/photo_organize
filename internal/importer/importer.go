package importer

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linuxhenhao/photo_organize/internal/db"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/linuxhenhao/photo_organize/internal/precompute"
	"github.com/linuxhenhao/photo_organize/internal/target"
)

const copyWorkers = 10

type ImportTask struct {
	SourcePath string
	TargetDir  string
	FileName   string
	Size       int64
	MMH3Hash   string
	DHash      string
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

func importWorker(tasks <-chan ImportTask, wg *sync.WaitGroup, successCount *int32, failCount *int32, coordinator *importCoordinator) {
	defer wg.Done()
	ctx := context.Background()
	for task := range tasks {
		sourceMeta := ""
		if _, hasDHash := parseTaskDHash(task); hasDHash {
			sourceMeta = metadata.ExtractImageMetaJson(task.SourcePath)
		}

		for {
			plan := coordinator.planTask(ctx, task, sourceMeta)
			switch plan.action {
			case importPlanSkip:
				goto nextTask
			case importPlanWait:
				<-plan.waitCh
				continue
			}

			finalTargetPath := plan.reservation.finalPath
			if err := os.MkdirAll(filepath.Dir(finalTargetPath), 0755); err != nil {
				coordinator.cancelReservation(plan.reservation)
				log.Printf("Failed to create directory for [%s]: %v", finalTargetPath, err)
				atomic.AddInt32(failCount, 1)
				goto nextTask
			}

			if err := copyFile(task.SourcePath, finalTargetPath); err != nil {
				coordinator.cancelReservation(plan.reservation)
				log.Printf("Failed to import [%s]: %v", task.SourcePath, err)
				atomic.AddInt32(failCount, 1)
				goto nextTask
			}

			switch plan.action {
			case importPlanCopyThumbnail:
				log.Printf("Found confirmed derived variant. Rerouting [%s] to thumbnails under [%s].", task.SourcePath, plan.reservation.committedMatchPath)
			case importPlanCopyMaster:
				if finalTargetPath != filepath.Join(task.TargetDir, task.FileName) {
					log.Printf("Conflict resolved for [%s], using new name [%s]", task.SourcePath, filepath.Base(finalTargetPath))
				}
			}

			committedPath := coordinator.commitReservation(plan.reservation)
			log.Printf("Successfully imported: [%s] -> [%s]", task.SourcePath, committedPath)
			atomic.AddInt32(successCount, 1)
			goto nextTask
		}

	nextTask:
		continue
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

	// Visual feature cache shares the same SQLite file but uses its own
	// connection so CacheManager can keep its async writer semantics.
	cacheDBPath := filepath.Join(destDir, "cache.db")
	cacheDB, err := sql.Open("sqlite", cacheDBPath+"?_busy_timeout=5000")
	if err != nil {
		log.Fatalf("Failed to open cache db '%s' for feature resolver: %v", cacheDBPath, err)
	}
	cacheDB.Exec(`PRAGMA synchronous = OFF`)
	cacheDB.Exec(`PRAGMA journal_mode = WAL`)
	defer cacheDB.Close()

	featureResolver, err := precompute.NewResolver(context.Background(), cacheDB)
	if err != nil {
		log.Fatalf("Failed to create feature resolver: %v", err)
	}
	defer featureResolver.Close()

	if err := db.InitDB(sqliteDB); err != nil {
		log.Fatalf("Warning: couldn't apply db migrations during import: %v", err)
	}

	tasks := make(chan ImportTask, copyWorkers*2)
	var wg sync.WaitGroup
	var successCount, failCount int32
	coordinator := newImportCoordinator(cacheManager, featureResolver)

	for i := 0; i < copyWorkers; i++ {
		wg.Add(1)
		go importWorker(tasks, &wg, &successCount, &failCount, coordinator)
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
		var sourcePath, createTimeStr, mmh3Hash, dhashStr string
		var size int64
		if err := rows.Scan(&sourcePath, &size, &createTimeStr, &mmh3Hash, &dhashStr); err != nil {
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
			DHash:      dhashStr,
		}
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating over database rows: %v", err)
	}

	close(tasks)
	wg.Wait()

	log.Printf("Import finished. Success: %d, Failed: %d\n", successCount, failCount)
}
