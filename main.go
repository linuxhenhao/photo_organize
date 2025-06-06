package main

import (
	"bytes"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/murmur3"
	_ "modernc.org/sqlite" // Import SQLite driver
)

const (
	dbWorkers   = 10  // Number of goroutines for file processing during scan
	copyWorkers = 10  // Number of goroutines for file copying during import
	batchSize   = 200 // Database transaction batch size / stats.txt flush interval
)

// stringArrayFlag implements flag.Value for parsing comma-separated strings
type stringArrayFlag []string

func (s *stringArrayFlag) String() string {
	return strings.Join(*s, ",")
}

func (s *stringArrayFlag) Set(value string) error {
	if value == "" {
		return errors.New("source directories cannot be empty")
	}
	*s = strings.Split(value, ",")
	return nil
}

type Insert struct {
	SQL        string
	Path       string
	Size       int64
	CreateTime time.Time
}

func main() {
	var dbPath string  // SQLite database path
	var destDir string // Target directory for import

	scanCmd := flag.NewFlagSet("scan", flag.ExitOnError)
	scanCmd.StringVar(&dbPath, "db", "photos.db", "SQLite database path")
	var parsedSourceDirs stringArrayFlag
	scanCmd.Var(&parsedSourceDirs, "src", "Source directories, comma-separated (e.g., /path/to/photos1,/path/to/photos2)")

	importCmd := flag.NewFlagSet("import", flag.ExitOnError)
	importCmd.StringVar(&dbPath, "db", "photos.db", "SQLite database path")
	importCmd.StringVar(&destDir, "dest", "", "Target directory for import (e.g., /path/to/organized_photos)")

	if len(os.Args) < 2 {
		fmt.Println("Usage: photo-organizer <command> [options]")
		fmt.Println("Commands: scan, import")
		fmt.Println("\nScan command options:")
		scanCmd.PrintDefaults()
		fmt.Println("\nImport command options:")
		importCmd.PrintDefaults()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "scan":
		scanCmd.Parse(os.Args[2:])
		if len(parsedSourceDirs) == 0 {
			log.Fatal("Scan command requires at least one source directory specified with -src.")
		}
		handleScan(dbPath, parsedSourceDirs)
	case "import":
		importCmd.Parse(os.Args[2:])
		if destDir == "" {
			log.Fatal("Import command requires a target directory specified with -dest.")
		}
		handleImport(dbPath, destDir)
	default:
		log.Fatalf("Invalid command: %s\nUsage: photo-organizer <command> [options]", os.Args[1])
	}
}

func initDB(db *sql.DB) error {
	// 设置无事务模式的优化参数
	_, err := db.Exec(`PRAGMA synchronous = OFF`)
	if err != nil {
		log.Printf("Warning: Failed to set synchronous mode: %v", err)
	}
	_, err = db.Exec(`PRAGMA journal_mode = MEMORY`)
	if err != nil {
		log.Printf("Warning: Failed to set journal mode: %v", err)
	}
	_, err = db.Exec(`PRAGMA cache_size = -64000`) // 64MB cache
	if err != nil {
		log.Printf("Warning: Failed to set cache size: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS photos (
			source_path TEXT PRIMARY KEY,
			size INTEGER,
			create_time TEXT,
			mmh3_hash TEXT DEFAULT '',
			group_id INTEGER DEFAULT 0
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create database table: %w", err)
	}
	return nil
}

// loadExistingPaths reads all source_path entries from the database into a map for quick lookups.
func loadExistingPaths(db *sql.DB) (map[string]bool, error) {
	log.Println("Loading existing file paths from the database to prevent re-scanning...")
	rows, err := db.Query("SELECT source_path FROM photos")
	if err != nil {
		return nil, fmt.Errorf("failed to query existing paths: %w", err)
	}
	defer rows.Close()

	existingPaths := make(map[string]bool)
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			log.Printf("Warning: failed to scan an existing path from the database: %v", err)
			continue
		}
		existingPaths[path] = true
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during iteration of existing paths: %w", err)
	}

	log.Printf("Loaded %d existing file paths. These will be skipped.", len(existingPaths))
	return existingPaths, nil
}

func handleScan(dbPath string, dirs []string) {
	log.Printf("scan dirs<%v> into db<%s>\n", dirs, dbPath)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Failed to open database '%s': %v", dbPath, err)
	}
	defer db.Close()

	if err := initDB(db); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	// NEW CODE BLOCK STARTS HERE
	existingPaths, err := loadExistingPaths(db)
	if err != nil {
		log.Fatalf("Failed to load existing paths from database: %v", err)
	}

	files := make(chan string, dbWorkers*2) // Buffered channel
	var wg sync.WaitGroup
	var insertWg sync.WaitGroup
	log.Printf("Starting %d worker goroutines for scanning...", dbWorkers)
	insertChan := make(chan Insert, 100)
	for i := 0; i < dbWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// log.Printf("Worker %d started", workerID)
			for file := range files {
				processFile(file, insertChan)
			}
			// log.Printf("Worker %d finished", workerID)
		}(i)
	}
	insertWg.Add(1)
	go insertData(db, insertChan, &insertWg)
	var skipCnt int32
	for _, dir := range dirs {
		log.Printf("Scanning directory: %s", dir)
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				log.Printf("Error accessing path [%s]: %v. Skipping.", path, err)
				return filepath.SkipDir // If dir itself is inaccessible, skip it
			}
			if !info.IsDir() {
				// 检查文件是否已在数据库中
				if _, exists := existingPaths[path]; exists {
					atomic.AddInt32(&skipCnt, 1)
					return nil // 跳过此文件，因为它已在数据库中
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

	log.Println("Calculating and updating mmh3_hash for files with identical sizes...")
	if err := updateHashes(db); err != nil {
		log.Fatalf("Failed to update hashes: %v", err)
	}
	log.Println("Assigning group_ids based on mmh3_hash...")
	if err := assignGroupIDs(db); err != nil {
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

	createTime, err := getCreateTime(path, stat)
	if err != nil {
		log.Printf("Failed to determine creation time for [%s], using modification time: %v", path, err)
		createTime = stat.ModTime() // Fallback to modification time
	}

	insertChan <- Insert{
		SQL:        `INSERT OR IGNORE INTO photos(source_path, size, create_time) VALUES(?, ?, ?)`,
		Path:       path,
		Size:       stat.Size(),
		CreateTime: createTime,
	}
}

func insertData(db *sql.DB, insertChan <-chan Insert, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := range insertChan {
		_, err := db.Exec(i.SQL, i.Path, i.Size, i.CreateTime.Format(time.RFC3339))
		if err != nil {
			log.Printf("write db line failed: %s\n", i.Path)
		}
	}
}

func extractTimeFromFilename(path string) (time.Time, error) {
	base := filepath.Base(path)
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(\d{4})/(\d{2})/(\d{2})`), // YYYY-MM-DD
		regexp.MustCompile(`(\d{4})-(\d{2})-(\d{2})`), // YYYY-MM-DD
		regexp.MustCompile(`(\d{4})_(\d{2})_(\d{2})`), // YYYY_MM_DD
		regexp.MustCompile(`(\d{4})(\d{2})(\d{2})`),   // YYYYMMDD
		// Add more patterns if needed, e.g., with time components
		// regexp.MustCompile(`(\d{4})(\d{2})(\d{2})[-_]?(\d{2})(\d{2})(\d{2})`), // YYYYMMDD-HHMMSS
	}

	for _, re := range patterns {
		matches := re.FindStringSubmatch(base)
		if len(matches) >= 4 {
			year, _ := strconv.Atoi(matches[1])
			month, _ := strconv.Atoi(matches[2])
			day, _ := strconv.Atoi(matches[3])

			currentYear := time.Now().Year()
			if year < 1900 || year > currentYear+5 { // Allow a bit into the future for camera clock issues
				log.Printf("Filename contains an unlikely year %d for file [%s]. Range: 1900-%d. Skipping this pattern match.", year, path, currentYear+5)
				continue
			}
			if month >= 1 && month <= 12 && day >= 1 && day <= 31 {
				// Basic validation, time.Date will do more thorough checks
				return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local), nil
			}
		}
	}
	return time.Time{}, errors.New("no valid date format found in filename")
}

// getCreateTime tries to get file creation time.
// Priority: exiftool, filename, stat birth time, stat mod time.
func getCreateTime(path string, fi os.FileInfo) (time.Time, error) {
	// 1. Try exiftool
	cmd := exec.Command("exiftool", "-m", "-d", "%Y-%m-%dT%H:%M:%S", // RFC3339 format
		"-CreateDate", "-DateTimeOriginal", "-MediaCreateDate", "-TrackCreateDate",
		"-SubSecCreateDate", "-SubSecDateTimeOriginal", // Include sub-second precision tags
		"-T", "-fast", path) // -fast for performance
	output, err := cmd.Output()

	if err == nil {
		fields := strings.Fields(string(bytes.TrimSpace(output))) // exiftool -T outputs tab-separated, Fields handles multiple spaces/tabs
		for _, field := range fields {
			if field == "-" { // exiftool uses "-" for missing tags
				continue
			}
			// Try parsing without timezone if RFC3339 fails (some tools might output local time without tz)
			// Example: 2023:01:01 10:00:00
			layouts := []string{
				"2006-01-02T15:04:05",
			}
			for _, layout := range layouts {
				if t, errParseLocal := time.ParseInLocation(layout, field, time.Local); errParseLocal == nil {
					return t, nil
				}
			}

		}
		// If loop finishes, no valid date was parsed from exiftool output
	} else {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("exiftool command failed for [%s] with exit code %d. Stderr: %s. Trying other methods.", path, exitErr.ExitCode(), string(exitErr.Stderr))
		} else if errors.Is(err, exec.ErrNotFound) {
			log.Printf("exiftool command not found. Ensure it's installed and in PATH. Skipping exiftool for [%s].", path)
		} else {
			log.Printf("exiftool execution failed for [%s]: %v. Trying other methods.", path, err)
		}
	}

	// 2. Try from filename
	if t, err := extractTimeFromFilename(path); err == nil {
		return t, nil
	}

	// 3. Try stat birth time (platform-specific)
	if t, ok := getStatBirthTime(fi); ok && !t.IsZero() {
		return t, nil
	}

	// 4. Fallback to file modification time (already done in processFile if this returns an error)
	return time.Time{}, errors.New("all methods to get creation time failed")
}

type importTask struct {
	SourcePath string
	TargetDir  string
	FileName   string
	MMH3Hash   string // mmh3_hash from DB, used to compare with existing target file
}

func handleImport(dbPath string, destDir string) {
	log.Printf("import using db<%s> to destDir<%s>\n", dbPath, destDir)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Failed to open database '%s': %v", dbPath, err)
	}
	defer db.Close()

	if err := os.MkdirAll(destDir, 0755); err != nil {
		log.Fatalf("Failed to create destination directory [%s]: %v", destDir, err)
	}

	statsFilePath := filepath.Join(destDir, "stats.txt") // Place stats.txt inside the destination
	statsFile, err := os.OpenFile(statsFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open stats.txt file [%s]: %v", statsFilePath, err)
	}
	defer statsFile.Close()

	var statsMux sync.Mutex
	processedInImportCount := 0 // Counts successful copies and skips for flushing stats.txt

	// Load already processed files from stats.txt to avoid re-processing
	// This makes the import somewhat resumable by skipping already handled files.
	var alreadyProcessed sync.Map
	statsContent, err := os.ReadFile(statsFilePath)
	if err == nil {
		lines := strings.Split(string(statsContent), "\n")
		for _, line := range lines {
			trimmedLine := strings.TrimSpace(line)
			if trimmedLine != "" {
				// Lines are like "/path/to/target/file.jpg" or "/path/to/target/file.jpg (skipped)"
				parts := strings.Split(trimmedLine, " ")
				if len(parts) > 0 {
					alreadyProcessed.Store(parts[0], true)
				}
			}
		}
		count := 0
		alreadyProcessed.Range(func(k, v any) bool {
			count++
			return false
		})
		log.Printf("Loaded %d entries from existing stats.txt. These will be skipped if encountered again.", count)
	}

	tasks := make(chan importTask, copyWorkers*2)
	var wg sync.WaitGroup

	log.Printf("Starting %d worker goroutines for importing files...", copyWorkers)
	for i := 0; i < copyWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range tasks {
				year, month, day := "", "", ""
				// The task struct needs the create_time for path generation, not just mmh3_hash for comparison
				// For now, let's assume we will query it again or add it to the task struct.
				// This part needs to be fixed by adding CreateTime to importTask.
				// For a quick fix, we re-query based on SourcePath for CreateTime if not passed.
				// Better: add CreateTime to importTask struct and populate it in the main query loop.
				// Let's assume createTime is part of the task or re-fetched for now.
				// For the purpose of this example, we'll use a dummy time if not provided.
				//
				// Correct approach: Add CreateTime to importTask
				// var createTimeForPath time.Time
				// query create_time from db using task.SourcePath if not available in task
				// For this example, let's just log a warning.
				// log.Printf("Warning: CreateTime for path determination is not directly in task. Needs fix.")

				// Construct target path based on CreateTime from DB
				var createTime time.Time
				row := db.QueryRow("SELECT create_time FROM photos WHERE source_path = ?", task.SourcePath)
				var createTimeStr string
				if err := row.Scan(&createTimeStr); err != nil {
					log.Printf("Error re-fetching create_time for [%s]: %v. Skipping.", task.SourcePath, err)
					continue
				}
				createTime, parseErr := time.Parse(time.RFC3339, createTimeStr)
				if parseErr != nil {
					log.Printf("Error parsing create_time [%s] for [%s]: %v. Skipping.", createTimeStr, task.SourcePath, parseErr)
					continue
				}

				year = createTime.Format("2006")
				month = createTime.Format("01")
				day = createTime.Format("02")
				targetSubDir := filepath.Join(destDir, year, month, day)

				if err := os.MkdirAll(targetSubDir, 0755); err != nil {
					log.Printf("Failed to create target subdirectory [%s] for [%s]: %v", targetSubDir, task.SourcePath, err)
					continue
				}

				originalFileName := task.FileName
				currentTargetFilePath := filepath.Join(targetSubDir, originalFileName)

				// Check if this exact target path was already processed (from stats.txt)
				if _, ok := alreadyProcessed.Load(task.SourcePath); ok {
					log.Printf("Already processed (from stats.txt): %s. Skipping.", task.SourcePath)
					continue
				}

				finalTargetFilePath := currentTargetFilePath
				fileNameToUse := originalFileName

				if _, statErr := os.Stat(finalTargetFilePath); !os.IsNotExist(statErr) {
					var sourceHashForComparison string
					var errSourceHash error

					if task.MMH3Hash != "" { // Use pre-computed hash from DB if available
						sourceHashForComparison = task.MMH3Hash
					} else { // Source hash not in DB (e.g., unique size file), calculate it now
						log.Printf("Source file [%s] hash not in DB. Calculating now for comparison with existing target [%s].", task.SourcePath, finalTargetFilePath)
						sourceHashForComparison, errSourceHash = calculateHash(task.SourcePath)
						if errSourceHash != nil {
							log.Printf("Failed to calculate hash for source file [%s] during import comparison: %v. Will proceed with rename.", task.SourcePath, errSourceHash)
							// Proceed to renaming as we can't confirm sameness
						}
					}

					if errSourceHash == nil { // Only proceed if we have a source hash (either from DB or calculated now)
						existingTargetFileHash, errTargetHash := calculateHash(finalTargetFilePath)
						if errTargetHash != nil {
							log.Printf("Failed to calculate hash for existing target file [%s]: %v. Will proceed with rename.", finalTargetFilePath, errTargetHash)
							// Proceed to renaming as we can't confirm sameness
						} else {
							if sourceHashForComparison == existingTargetFileHash {
								log.Printf("Target file [%s] exists and content matches source [%s] (hash: %s). Skipping copy.", finalTargetFilePath, task.SourcePath, sourceHashForComparison)
								statsMux.Lock()
								if _, err := statsFile.WriteString(fmt.Sprintf("%s (skipped - content matched)\n", task.SourcePath)); err != nil {
									log.Printf("Failed to write skip (content matched) to stats.txt for [%s]: %v", task.SourcePath, err)
								}
								processedInImportCount++
								if processedInImportCount%batchSize == 0 {
									if err := statsFile.Sync(); err != nil { // Check error for Sync
										log.Printf("stats.txt sync failed after skip: %v", err)
									}
								}
								alreadyProcessed.Store(task.SourcePath, true) // Mark as processed for this run
								statsMux.Unlock()
								continue // Skip to next task
							}
							log.Printf("Target file [%s] exists but content differs from source [%s]. Proceeding with rename.", finalTargetFilePath, task.SourcePath)
						}
					}
					// If we reach here, it means:
					// 1. errSourceHash was not nil (failed to get source hash on-the-fly) OR
					// 2. errTargetHash was not nil (failed to get target hash) OR
					// 3. Hashes were successfully calculated but are different.
					// All these cases lead to renaming.

					// Renaming logic:
					counter := 1
					for {
						ext := filepath.Ext(originalFileName)
						nameWithoutExt := strings.TrimSuffix(originalFileName, ext)
						fileNameToUse = fmt.Sprintf("%s-%d%s", nameWithoutExt, counter, ext)
						finalTargetFilePath = filepath.Join(targetSubDir, fileNameToUse)

						if _, err := os.Stat(finalTargetFilePath); os.IsNotExist(err) {
							break // Found a unique name
						}
						counter++
						if counter > 1000 { // Safety break
							log.Printf("Could not find a unique filename for [%s] in [%s] after 1000 attempts. Skipping this file.", originalFileName, targetSubDir)
							goto nextTask // Using goto to break out of outer loop for this task.
						}
					}
				}

				if err := copyFile(task.SourcePath, finalTargetFilePath); err != nil {
					log.Printf("Failed to copy file [%s] to [%s]: %v", task.SourcePath, finalTargetFilePath, err)
					continue
				}
				log.Printf("Successfully imported: [%s] -> [%s]", task.SourcePath, finalTargetFilePath)

				statsMux.Lock()
				if _, err := statsFile.WriteString(task.SourcePath + "\n"); err != nil {
					log.Printf("Failed to write to stats.txt for [%s]: %v", task.SourcePath, err)
				}
				processedInImportCount++
				if processedInImportCount%batchSize == 0 {
					if err := statsFile.Sync(); err != nil {
						log.Printf("stats.txt sync failed: %v", err)
					}
				}
				statsMux.Unlock()
				alreadyProcessed.Store(task.SourcePath, true) // Mark as processed for this run

			nextTask: // Label for the goto statement
			}
		}(i)
	}

	// Query for files to import
	// group_id = 0 OR (group_id != 0 AND it's the one with MIN(mmh3_hash) for that group_id)
	rows, err := db.Query(`
		SELECT source_path, create_time, mmh3_hash, group_id
		FROM photos
		WHERE group_id = 0
		UNION ALL
		SELECT p1.source_path, p1.create_time, p1.mmh3_hash, p1.group_id
		FROM photos p1
		INNER JOIN (
			SELECT group_id, MIN(source_path) AS min_path
			FROM photos
			WHERE group_id != 0 AND mmh3_hash != '' /* Ensure hash is not empty for grouped items */
			GROUP BY group_id
		) AS p2 ON p1.group_id = p2.group_id AND p1.source_path = p2.min_path
		ORDER BY create_time ASC; 
	`) // Order by create_time for potentially more sequential disk access pattern.
	if err != nil {
		log.Fatalf("Failed to query photos for import: %v", err)
	}
	defer rows.Close()

	log.Printf("Querying database and sending import tasks to workers...")
	filesToImportCount := 0
	for rows.Next() {
		var sourcePath, createTimeStr, mmh3Hash string
		var groupID int
		if err := rows.Scan(&sourcePath, &createTimeStr, &mmh3Hash, &groupID); err != nil {
			log.Printf("Failed to scan database row for import: %v", err)
			continue
		}
		filesToImportCount++
		tasks <- importTask{
			SourcePath: sourcePath,
			// TargetDir will be constructed by worker using createTimeStr
			FileName: filepath.Base(sourcePath),
			MMH3Hash: mmh3Hash,
			// IMPORTANT: CreateTimeStr should be passed to task to avoid re-query.
			// The current worker re-queries it. For full optimization, add CreateTimeStr to importTask.
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Error during rows iteration for import: %v", err)
	}
	log.Printf("Found %d files to potentially import. Processing...", filesToImportCount)

	close(tasks)
	wg.Wait()

	statsMux.Lock() // Final sync
	if err := statsFile.Sync(); err != nil {
		log.Printf("stats.txt final sync failed: %v", err)
	}
	statsMux.Unlock()
	log.Println("Import command finished.")
}

func updateHashes(db *sql.DB) error {
	// 1. 查询所有需要更新的文件路径
	var paths []string
	rows, err := db.Query(`
		SELECT p.source_path
		FROM photos p
		JOIN (SELECT size FROM photos GROUP BY size HAVING COUNT(*) > 1) AS samesize
		ON p.size = samesize.size
		WHERE p.mmh3_hash = '';
	`)
	if err != nil {
		return fmt.Errorf("failed to query for files needing hash update: %w", err)
	}

	// 2. 将路径存入内存
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			log.Printf("Failed to scan path for hash update: %v", err)
			continue
		}
		paths = append(paths, path)
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error during path collection: %w", err)
	}

	// 3. 对收集到的路径进行哈希更新
	count := 0
	log.Println("Processing files for hash calculation...")
	for _, path := range paths {
		hash, errCalc := calculateHash(path)
		if errCalc != nil {
			log.Printf("Hash calculation failed for [%s]: %v. Skipping hash update for this file.", path, errCalc)
			continue
		}

		if _, err = db.Exec(`UPDATE photos SET mmh3_hash = ? WHERE source_path = ?`, hash, path); err != nil {
			log.Printf("Failed to update hash for [%s]: %v", path, err)
			continue
		}
		count++
		if count%batchSize == 0 {
			log.Printf("Updated hashes for %d files...", count)
		}
	}

	log.Printf("Finished updating hashes for %d files", count)
	return nil
}

func calculateHash(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file [%s] for hashing: %w", path, err)
	}
	defer file.Close()

	// Using New128 for a potentially better collision resistance, though murmur3.New64 is often fine.
	// The requirement specified mmh3_hash, implying MurmurHash3. The output size isn't fixed.
	// Let's stick to 64-bit as in the original code for consistency with `Sum64()`.
	hasher := murmur3.New64()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", fmt.Errorf("failed to copy file content to hasher for [%s]: %w", path, err)
	}
	return fmt.Sprintf("%x", hasher.Sum64()), nil
}

func assignGroupIDs(db *sql.DB) error {
	// Set group_id = 0 for files with empty mmh3_hash (includes those that failed hashing)
	if _, err := db.Exec(`UPDATE photos SET group_id = 0 WHERE mmh3_hash = '' OR mmh3_hash IS NULL`); err != nil {
		return fmt.Errorf("failed to reset group_id for empty hashes: %w", err)
	}

	// 1. 查询所有不同的哈希值
	var hashes []string
	rows, err := db.Query(`SELECT DISTINCT mmh3_hash FROM photos WHERE mmh3_hash != '' ORDER BY mmh3_hash`)
	if err != nil {
		return fmt.Errorf("failed to query distinct hashes for group ID assignment: %w", err)
	}

	// 2. 将哈希值存入内存
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			log.Printf("Failed to scan hash for group ID assignment: %v", err)
			continue
		}
		hashes = append(hashes, hash)
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error during hash collection: %w", err)
	}

	// 3. 为每个哈希值分配组ID
	groupID := 1
	count := 0
	log.Println("Processing distinct hashes for group ID assignment...")
	for _, hash := range hashes {
		if _, err = db.Exec(`UPDATE photos SET group_id = ? WHERE mmh3_hash = ?`, groupID, hash); err != nil {
			log.Printf("Failed to update group_id for hash [%s]: %v", hash, err)
			continue
		}
		groupID++
		count++
		if count%batchSize == 0 {
			log.Printf("Assigned group IDs for %d unique hashes...", count)
		}
	}

	log.Printf("Finished assigning group IDs for %d groups", count)
	return nil
}

func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file [%s]: %w", src, err)
	}
	defer sourceFile.Close()

	// Create destination file. If it exists, it will be truncated.
	destFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file [%s]: %w", dst, err)
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return fmt.Errorf("failed to copy content from [%s] to [%s]: %w", src, dst, err)
	}

	// Ensure data is written to stable storage
	return destFile.Sync()
}
