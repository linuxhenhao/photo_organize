package scanner

import (
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linuxhenhao/photo_organize/internal/db"
	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
)

const dbWorkers = 10

// Insert represents a database insertion job
type Insert struct {
	SQL        string
	Path       string
	Size       int64
	CreateTime time.Time
	MimeType   string
}

// HandleScan initiates the scanning of given directories to update the database
func HandleScan(dbPath string, dirs []string) {
	log.Printf("scan dirs<%v> into db<%s>\n", dirs, dbPath)
	sqliteDB, err := sql.Open("sqlite", dbPath+"?_busy_timeout=5000")
	if err != nil {
		log.Fatalf("Failed to open database '%s': %v", dbPath, err)
	}
	defer sqliteDB.Close()

	if err := db.InitDB(sqliteDB); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	existingPaths, err := db.LoadExistingPaths(sqliteDB)
	if err != nil {
		log.Fatalf("Failed to load existing paths from database: %v", err)
	}

	files := make(chan string, dbWorkers*2)
	var wg sync.WaitGroup
	var insertWg sync.WaitGroup
	log.Printf("Starting %d worker goroutines for scanning...", dbWorkers)
	insertChan := make(chan Insert, 100)
	
	for i := 0; i < dbWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for file := range files {
				processFile(file, insertChan)
			}
		}(i)
	}
	
	insertWg.Add(1)
	go insertData(sqliteDB, insertChan, &insertWg)
	
	var skipCnt int32
	for _, dir := range dirs {
		log.Printf("Scanning directory: %s", dir)
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				log.Printf("Error accessing path [%s]: %v. Skipping.", path, err)
				return filepath.SkipDir
			}
			if !info.IsDir() {
				if _, exists := existingPaths[path]; exists {
					atomic.AddInt32(&skipCnt, 1)
					return nil
				}
				files <- path
			}
			return nil
		})
		if err != nil {
			log.Printf("Error walking directory [%s]: %v", dir, err)
		}
	}

	close(files)
	wg.Wait()
	close(insertChan)
	insertWg.Wait()
	log.Printf("File metadata scanning complete, %d files skipped\n", skipCnt)

	log.Println("Calculating and updating hashes for files...")
	if err := hasher.UpdateHashes(sqliteDB); err != nil {
		log.Fatalf("Failed to update hashes: %v", err)
	}
	
	log.Println("Assigning group_ids based on mmh3_hash...")
	if err := hasher.AssignGroupIDs(sqliteDB); err != nil {
		log.Fatalf("Failed to assign group IDs: %v", err)
	}
	
	log.Println("Scan command finished successfully.")
}

func processFile(path string, insertChan chan<- Insert) {
	stat, err := os.Stat(path)
	if err != nil {
		log.Printf("Failed to get stat for file [%s]: %v", path, err)
		return
	}

	createTime, mimeType, err := metadata.GetMetadata(path, stat)
	if err != nil {
		log.Printf("Failed to determine creation time for [%s], using modification time: %v", path, err)
		createTime = stat.ModTime()
	}

	insertChan <- Insert{
		SQL:        `INSERT OR IGNORE INTO photos(source_path, size, create_time, mime_type) VALUES(?, ?, ?, ?)`,
		Path:       path,
		Size:       stat.Size(),
		CreateTime: createTime,
		MimeType:   mimeType,
	}
}

func insertData(sqliteDB *sql.DB, insertChan <-chan Insert, wg *sync.WaitGroup) {
	defer wg.Done()
	
	const batchSize = 100
	var count int
	
	tx, err := sqliteDB.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction for insertion: %v", err)
		return
	}

	for i := range insertChan {
		_, err := tx.Exec(i.SQL, i.Path, i.Size, i.CreateTime.Format(time.RFC3339), i.MimeType)
		if err != nil {
			log.Printf("write db line failed: %s: %v\n", i.Path, err)
			continue
		}
		
		count++
		if count >= batchSize {
			if err := tx.Commit(); err != nil {
				log.Printf("Failed to commit batch insertion: %v", err)
			}
			tx, err = sqliteDB.Begin()
			if err != nil {
				log.Printf("Failed to begin next transaction for insertion: %v", err)
				return
			}
			count = 0
		}
	}
	
	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit final batch insertion: %v", err)
	}
}
