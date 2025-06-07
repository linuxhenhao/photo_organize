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
	hashWorkers = 10  // Number of goroutines for hashing during import
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

	initCacheCmd := flag.NewFlagSet("init_cache", flag.ExitOnError)
	initCacheCmd.StringVar(&destDir, "dest", "", "Target directory for import (e.g., /path/to/organized_photos)")

	if len(os.Args) < 2 {
		fmt.Println("Usage: photo-organizer <command> [options]")
		fmt.Println("Commands: scan, import, initcache")
		fmt.Println("\nScan command options:")
		scanCmd.PrintDefaults()
		fmt.Println("\nImport command options:")
		importCmd.PrintDefaults()
		fmt.Println("\nInitCache command options:")
		initCacheCmd.PrintDefaults()
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
	case "initcache":
		initCacheCmd.Parse(os.Args[2:])
		if destDir == "" {
			log.Fatal("InitCache command requires a target directory specified with -dest.")
		}
		handleInitCache(destDir)
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

func handleInitCache(destDir string) {
	// Initialize theCacheManager for the MMH3 hash cache file.
	hashCacheFilePath := filepath.Join(destDir, "mmh3_hash_cache.txt")
	cacheManager, err := NewCacheManager(hashCacheFilePath, batchSize)
	if err != nil {
		log.Printf("Error: CacheManager init failed, err=%v", err)
		return
	}
	InitTargetDirCache(destDir, cacheManager)
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

// validateDate checks if the given year, month, and day form a valid date
func validateDate(year, month, day int, path string) (bool, error) {
	currentYear := time.Now().Year()
	if year < 1900 || year > currentYear+5 { // Allow a bit into the future for camera clock issues
		return false, fmt.Errorf("unlikely year %d for file [%s]. Range: 1900-%d", year, path, currentYear+5)
	}
	if month < 1 || month > 12 {
		return false, fmt.Errorf("invalid month %d", month)
	}
	if day < 1 || day > 31 {
		return false, fmt.Errorf("invalid day %d", day)
	}
	return true, nil
}

func extractTimeFromFilename(path string) (time.Time, error) {
	// First try to match the filename
	base := filepath.Base(path)
	patterns := []*regexp.Regexp{
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

			if valid, err := validateDate(year, month, day, path); valid {
				// Basic validation passed, time.Date will do more thorough checks
				return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local), nil
			} else {
				log.Printf("Date validation failed in filename: %v", err)
				continue
			}
		}
	}

	// If no match in filename, try to match directory path components
	dirPath := filepath.Dir(path)
	dirComponents := strings.Split(dirPath, string(filepath.Separator))

	// Look for threedddconsecutive components that could be year/month/day
	if len(dirComponents) >= 3 {
		start := len(dirComponents) - 3
		year, yearErr := strconv.Atoi(dirComponents[start])
		month, monthErr := strconv.Atoi(dirComponents[start+1])
		day, dayErr := strconv.Atoi(dirComponents[start+2])

		if yearErr == nil && monthErr == nil && dayErr == nil {
			if valid, err := validateDate(year, month, day, path); valid {
				return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local), nil
			} else {
				log.Printf("Date validation failed in directory path: %v", err)
			}
		}
	}
	return time.Time{}, errors.New("no valid date format found in filename or directory path")
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

// handleImport manages the photo import process from a SQLite database
// to a destination directory, using an MMH3 hash cache file for efficiency.
func handleImport(dbPath string, destDir string) {
	log.Printf("Importing using db<%s> to destDir<%s>\n", dbPath, destDir)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Failed to open database '%s': %v", dbPath, err)
	}
	defer db.Close() // Ensure database connection is closed when the function exits

	// Create the destination directory if it doesn't exist.
	if err := os.MkdirAll(destDir, 0755); err != nil {
		log.Fatalf("Failed to create destination directory [%s]: %v", destDir, err)
	}

	// Initialize theCacheManager for the MMH3 hash cache file.
	hashCacheFilePath := filepath.Join(destDir, "mmh3_hash_cache.txt")
	cacheManager, err := NewCacheManager(hashCacheFilePath, batchSize)
	if err != nil {
		log.Fatalf("Failed to initialize hash cache: %v", err)
	}
	// Ensure the CacheManager is properly closed on function exit.
	defer func() {
		if closeErr := cacheManager.Close(); closeErr != nil {
			log.Printf("Error closing hash cache manager: %v", closeErr)
		}
	}()

	// Channel to send import tasks to worker goroutines.
	tasks := make(chan importTask, copyWorkers*2)
	var wg sync.WaitGroup // WaitGroup to wait for all workers to finish.

	log.Printf("Starting %d worker goroutines for importing files...", copyWorkers)
	for i := 0; i < copyWorkers; i++ {
		wg.Add(1) // Increment WaitGroup counter for each worker
		go func(workerID int) {
			defer wg.Done() // Decrement WaitGroup counter when worker exits
			for task := range tasks {

				// Re-fetch create_time from DB if not already present in task.
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

				// Construct the target subdirectory based on create_time (Year/Month/Day).
				year := createTime.Format("2006")
				month := createTime.Format("01")
				day := createTime.Format("02")
				targetSubDir := filepath.Join(destDir, year, month, day)

				// Create the target subdirectory if it doesn't exist.
				if err := os.MkdirAll(targetSubDir, 0755); err != nil {
					log.Printf("Failed to create target subdirectory [%s] for [%s]: %v", targetSubDir, task.SourcePath, err)
					continue
				}

				originalFileName := task.FileName
				currentTargetFilePath := filepath.Join(targetSubDir, originalFileName)

				// **Optimization 1: Check if source file's hash already exists in cache.**
				// If the source file has an MMH3 hash (from the DB) and that hash is found
				// in our in-memory cache, it means a file with identical content might
				// already exist in the destination.
				if task.MMH3Hash != "" {
					if cachedPath, ok := cacheManager.cache.Load(task.MMH3Hash); ok {
						if cachedPathStr, isString := cachedPath.(string); isString {
							// Double-check if the cached target file path actually exists on disk.
							// This handles cases where the cache might be stale (e.g., file deleted manually).
							if _, statErr := os.Stat(cachedPathStr); !os.IsNotExist(statErr) {
								log.Printf("Source file hash [%s] already exists in cache and target file [%s] exists on disk. Skipping copy for source [%s].", task.MMH3Hash, cachedPathStr, task.SourcePath)
								// Add to cache again (no actual write unless batch is full) to ensure it's counted for `processedCount`.
								cacheManager.AddEntry(task.MMH3Hash, cachedPathStr)
								continue // Skip to the next task
							} else {
								// Cache entry exists but the file itself is missing. Remove from cache (best-effort cleanup).
								cacheManager.cache.Delete(task.MMH3Hash)
								log.Printf("Cached file [%s] with hash [%s] not found on disk. Removing from cache and proceeding with import.", cachedPathStr, task.MMH3Hash)
							}
						}
					}
				}

				finalTargetFilePath := currentTargetFilePath // Initial assumption for the target path
				fileNameToUse := originalFileName
				// Calculate the hash of the newly copied file if it wasn't already known (i.e., from task.MMH3Hash).
				copiedFileHash := task.MMH3Hash

				// **Handle existing files at the target path.**
				if _, statErr := os.Stat(finalTargetFilePath); !os.IsNotExist(statErr) {
					// File already exists at the intended target path. We need to compare hashes.
					var sourceHashForComparison string
					var errSourceHash error

					if task.MMH3Hash != "" {
						sourceHashForComparison = task.MMH3Hash // Use pre-computed hash from DB
					} else {
						// Source hash not in DB, calculate it on-the-fly for comparison.
						log.Printf("Source file [%s] hash not in DB. Calculating now for comparison with existing target [%s].", task.SourcePath, finalTargetFilePath)
						sourceHashForComparison, errSourceHash = calculateHash(task.SourcePath)
						if errSourceHash != nil {
							log.Printf("Failed to calculate hash for source file [%s] during import comparison: %v. Will proceed with rename.", task.SourcePath, errSourceHash)
							// If we can't get the source hash, we can't compare content, so proceed with renaming.
						}
					}

					if errSourceHash == nil { // Proceed only if we have a valid source hash
						var existingTargetFileHash string
						// Calculate the hash of the existing target file.
						var errTargetHash error
						existingTargetFileHash, errTargetHash = calculateHash(finalTargetFilePath)
						if errTargetHash != nil {
							log.Printf("Failed to calculate hash for existing target file [%s]: %v. Will proceed with rename.", finalTargetFilePath, errTargetHash)
						} else {
							// Add the newly calculated target hash to the cache for future lookups.
							cacheManager.AddEntry(existingTargetFileHash, finalTargetFilePath)
						}

						// If hashes match, the existing file is identical; skip copying.
						if existingTargetFileHash != "" && sourceHashForComparison == existingTargetFileHash {
							log.Printf("Target file [%s] exists and content matches source [%s] (hash: %s). Skipping copy.", finalTargetFilePath, task.SourcePath, sourceHashForComparison)
							cacheManager.AddEntry(task.MMH3Hash, finalTargetFilePath) // Mark as processed for this run
							continue                                                  // Skip to the next task in the channel
						}
						log.Printf("Target file [%s] exists but content differs from source [%s]. Proceeding with rename to a unique name.", finalTargetFilePath, task.SourcePath)
					}

					// If we reach here, it means the file exists and either:
					// 1. Hashes differ.
					// 2. We couldn't calculate one of the hashes for comparison.
					// In either case, we proceed with the renaming strategy to find a unique target name.
					counter := 1
					for {
						ext := filepath.Ext(originalFileName)
						nameWithoutExt := strings.TrimSuffix(originalFileName, ext)
						fileNameToUse = fmt.Sprintf("%s-%d%s", nameWithoutExt, counter, ext)
						finalTargetFilePath = filepath.Join(targetSubDir, fileNameToUse)

						if _, err := os.Stat(finalTargetFilePath); os.IsNotExist(err) {
							break // Found a unique name that doesn't exist
						}
						counter++
						if counter > 1000 { // Safety break to prevent infinite loops for extremely common names
							log.Printf("Could not find a unique filename for [%s] in [%s] after 1000 attempts. Skipping this file.", originalFileName, targetSubDir)
							goto nextTask // Use goto to jump to the label below and process the next task
						}
					}
				}

				// Perform the file copy.
				if err := copyFile(task.SourcePath, finalTargetFilePath); err != nil {
					log.Printf("Failed to copy file [%s] to [%s]: %v", task.SourcePath, finalTargetFilePath, err)
					continue
				}

				if copiedFileHash == "" {
					var errHash error
					copiedFileHash, errHash = calculateHash(finalTargetFilePath)
					if errHash != nil {
						log.Printf("Failed to calculate hash for newly copied file [%s]: %v. Cache will not include its hash.", finalTargetFilePath, errHash)
					}
				}

				// If a valid hash is available for the copied file, add it to the cache.
				if copiedFileHash != "" {
					cacheManager.AddEntry(copiedFileHash, finalTargetFilePath)
				}

				log.Printf("Successfully imported: [%s] -> [%s]", task.SourcePath, finalTargetFilePath)
			nextTask: // Label for the goto statement, allows skipping to the next task in the loop.
			}
		}(i)
	}

	// Query the database for files to import.
	// This query selects:
	// 1. Files with group_id = 0 (likely unique files).
	// 2. For files with group_id != 0, it selects the one with the minimum source_path
	//    among those having the same group_id and a non-empty mmh3_hash.
	// This approach ensures that only one representative from each group (duplicates) is considered.
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
    `)
	if err != nil {
		log.Fatalf("Failed to query photos for import: %v", err)
	}
	defer rows.Close() // Close the database rows when done

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
		// Send the import task to the channel.
		tasks <- importTask{
			SourcePath: sourcePath,
			FileName:   filepath.Base(sourcePath),
			MMH3Hash:   mmh3Hash, // Pass the MMH3 hash directly from the database
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Error during rows iteration for import: %v", err)
	}
	log.Printf("Found %d files to potentially import. Processing...", filesToImportCount)

	close(tasks) // Close the tasks channel to signal workers that no more tasks will be sent.
	wg.Wait()    // Wait for all worker goroutines to finish their tasks.

	log.Println("Import command finished.")
}

// hashResult holds the outcome of a hash calculation for a single file.
type hashResult struct {
	path string
	hash string
	err  error
}

// updateHashes calculates and updates hashes for files concurrently.
// It uses a worker pool pattern to parallelize the hash calculation, which is
// often the most time-consuming part of the operation.
func updateHashes(db *sql.DB) error {
	// 1. Query for all file paths that need updating.
	// This part remains sequential as it's fetching the initial dataset.
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

	// 2. Collect all paths into memory first.
	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			log.Printf("Failed to scan path for hash update: %v", err)
			continue
		}
		paths = append(paths, path)
	}
	rows.Close() // It's important to close rows before checking rows.Err().
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error during path collection: %w", err)
	}

	if len(paths) == 0 {
		log.Println("No files found needing a hash update.")
		return nil
	}

	log.Printf("Found %d files to process for hash calculation...", len(paths))

	// 3. Set up the worker pool.
	// We'll use a number of workers equal to the number of available CPU cores.
	// This is a good default for CPU-bound tasks and is also effective for I/O-bound tasks.
	jobs := make(chan string, len(paths))
	results := make(chan hashResult, len(paths))

	// 4. Start the worker goroutines.
	// These workers will receive file paths from the 'jobs' channel,
	// calculate the hash, and send the result to the 'results' channel.
	var workerWg sync.WaitGroup
	for w := 1; w <= hashWorkers; w++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for path := range jobs {
				hash, err := calculateHash(path)
				results <- hashResult{path: path, hash: hash, err: err}
			}
		}()
	}

	// 5. Start a separate goroutine to handle database updates.
	// This prevents the update logic from blocking the main loop. It collects
	// results from the 'results' channel and executes the database updates.
	var updaterWg sync.WaitGroup
	updaterWg.Add(1)
	go func() {
		defer updaterWg.Done()
		updatedCount := 0
		// We know exactly how many results to expect, so we loop that many times.
		for i := 0; i < len(paths); i++ {
			result := <-results

			if result.err != nil {
				log.Printf("Hash calculation failed for [%s]: %v. Skipping update.", result.path, result.err)
				continue
			}

			if _, errDB := db.Exec(`UPDATE photos SET mmh3_hash = ? WHERE source_path = ?`, result.hash, result.path); errDB != nil {
				log.Printf("Failed to update hash for [%s]: %v", result.path, errDB)
				continue
			}

			updatedCount++
			if updatedCount%batchSize == 0 {
				log.Printf("Updated hashes for %d files...", updatedCount)
			}
		}
		log.Printf("Finished updating hashes for %d files.", updatedCount)
	}()

	// 6. Send all jobs (file paths) to the workers.
	for _, path := range paths {
		jobs <- path
	}
	close(jobs) // Close the 'jobs' channel to signal workers that no more jobs are coming.

	// 7. Wait for all work to be done.
	// First, wait for all worker goroutines to finish their hash calculations.
	workerWg.Wait()

	// All hashes are now calculated and have been sent to the 'results' channel.
	// We can now wait for the updater goroutine to finish writing all results to the database.
	updaterWg.Wait()

	log.Println("All hash update operations completed successfully.")
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
	log.Println("Starting group ID assignment...")

	// 使用单个 SQL 语句完成所有更新
	// 1. 首先将空哈希值的记录设置为 group_id = 0
	// 2. 然后使用窗口函数为每个不同的哈希值分配连续的 group_id
	updateSQL := `
	WITH numbered_hashes AS (
		SELECT DISTINCT mmh3_hash,
			ROW_NUMBER() OVER (ORDER BY mmh3_hash) as new_group_id
		FROM photos
		WHERE mmh3_hash != '' AND mmh3_hash IS NOT NULL
	)
	UPDATE photos SET group_id = CASE
		WHEN mmh3_hash = '' OR mmh3_hash IS NULL THEN 0
		ELSE (SELECT new_group_id FROM numbered_hashes WHERE numbered_hashes.mmh3_hash = photos.mmh3_hash)
	END`

	result, err := db.Exec(updateSQL)
	if err != nil {
		return fmt.Errorf("failed to update group IDs: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Printf("Warning: couldn't get number of affected rows: %v", err)
	} else {
		log.Printf("Updated group IDs for %d photos", rowsAffected)
	}

	// 获取分配的组数量
	var groupCount int
	err = db.QueryRow("SELECT COUNT(DISTINCT group_id) - 1 FROM photos").Scan(&groupCount) // -1 to exclude group 0
	if err != nil {
		log.Printf("Warning: couldn't get group count: %v", err)
	} else {
		log.Printf("Created %d distinct groups (excluding group 0 for empty hashes)", groupCount)
	}

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
