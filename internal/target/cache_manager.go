package target

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/linuxhenhao/photo_organize/internal/dedupe"
	"github.com/linuxhenhao/photo_organize/internal/hasher"
	_ "modernc.org/sqlite"
)

// CacheInfo tracks the completion state of a cached file entry
type CacheInfo struct {
	MMH3 string
	// DHash is stored in SQLite column file_cache.phash for historical reasons.
	DHash   string
	Size    int64
	HasMeta bool
}

// cacheEntry represents a database update task for the CacheManager background worker.
type cacheEntry struct {
	path        string
	mmh3        string
	phash       string // String representation for the DB
	size        int64
	metadata    string
	thumbJson   string
	thumbPath   string
	thumbMeta   string
	promoteFrom string
	isAppend    bool
	isDelete    bool
	isPromote   bool
}

// CacheManager handles the MMH3 Hash and PHash cache operations,
// managing the BK-Tree for visual search and SQLite for persistence.
// It tracks relationships between thumbnails and original master files.
type CacheManager struct {
	db        *sql.DB
	mutex     sync.Mutex // Protects consistent access to in-memory state
	mmh3Cache sync.Map   // In-memory cache: mmh3_hash -> targetFilePath
	paths     sync.Map   // In-memory cache: targetFilePath -> CacheInfo
	DHashTree *hasher.BKTree
	batchSize int

	entries  chan cacheEntry
	workerWg sync.WaitGroup
}

// NewCacheManager initializes and returns a new CacheManager instance.
func NewCacheManager(destDir string, batchSize int) (*CacheManager, error) {
	cm := &CacheManager{
		mmh3Cache: sync.Map{},
		paths:     sync.Map{},
		DHashTree: hasher.NewBKTree(),
		batchSize: batchSize,
		entries:   make(chan cacheEntry, 1000),
	}

	cacheDBPath := filepath.Join(destDir, "cache.db")
	db, err := sql.Open("sqlite", cacheDBPath+"?_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("failed to open cache db: %w", err)
	}
	cm.db = db

	// Optimize sqlite
	db.Exec(`PRAGMA synchronous = OFF`)
	db.Exec(`PRAGMA journal_mode = WAL`)

	// Schema management
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS file_cache (
			target_path TEXT PRIMARY KEY,
			mmh3_hash TEXT,
			phash TEXT,
			size INTEGER,
			metadata TEXT DEFAULT '{}',
			thumbnails TEXT DEFAULT '[]'
		);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache table: %w", err)
	}

	// Migrations for existing databases
	_, _ = db.Exec(`ALTER TABLE file_cache ADD COLUMN metadata TEXT DEFAULT '{}'`)
	_, _ = db.Exec(`ALTER TABLE file_cache ADD COLUMN thumbnails TEXT DEFAULT '[]'`)
	_, _ = db.Exec(`UPDATE file_cache SET thumbnails = '[]' WHERE thumbnails = '' OR thumbnails IS NULL`)
	// Attempt to drop master_path if it existed (SQLite ALTER TABLE DROP COLUMN is supported in 3.35.0+)
	_, _ = db.Exec(`ALTER TABLE file_cache DROP COLUMN master_path`)

	// 1. Check for old format migration
	oldTxtPath := filepath.Join(destDir, "mmh3_hash_cache.txt")
	if stat, err := os.Stat(oldTxtPath); err == nil && !stat.IsDir() {
		log.Printf("Found old txt cache '%s', migrating to SQLite...", oldTxtPath)
		cm.migrateOldCache(oldTxtPath)
	}

	// 2. Load existing DB entries
	rows, err := db.Query("SELECT target_path, mmh3_hash, phash, size, metadata FROM file_cache")
	if err == nil {
		defer rows.Close()
		var loaded int
		for rows.Next() {
			var path, mmh3, dhashStr, metadataStr string
			var size int64
			if err := rows.Scan(&path, &mmh3, &dhashStr, &size, &metadataStr); err == nil {
				// Initialize memory representation
				cm.paths.Store(path, CacheInfo{
					MMH3:    mmh3,
					DHash:   dhashStr,
					Size:    size,
					HasMeta: metadataStr != "" && metadataStr != "{}",
				})
				if mmh3 != "" {
					cm.mmh3Cache.Store(mmh3, path)
				}
				if dhashStr != "" {
					if hashVal, parseErr := hasher.StringToDHash(dhashStr); parseErr == nil {
						cm.DHashTree.Add(hashVal, path, size)
					}
				}
				loaded++
			}
		}
		log.Printf("Loaded %d entries from SQLite cache.", loaded)
	}

	// Start the background worker for DB IO
	cm.workerWg.Add(1)
	go cm.runWorker()

	return cm, nil
}

func (cm *CacheManager) setEntryLocked(path string, mmh3 string, dhash uint64, hasDHash bool, size int64, hasMeta bool) string {
	dhashStr := ""
	if hasDHash {
		dhashStr = hasher.DHashToString(dhash)
	}

	if prevVal, ok := cm.paths.Load(path); ok {
		prev := prevVal.(CacheInfo)
		if prev.MMH3 != "" && prev.MMH3 != mmh3 {
			if mappedPath, ok := cm.mmh3Cache.Load(prev.MMH3); ok && mappedPath.(string) == path {
				cm.mmh3Cache.Delete(prev.MMH3)
			}
		}
	}

	cm.paths.Store(path, CacheInfo{
		MMH3:    mmh3,
		DHash:   dhashStr,
		Size:    size,
		HasMeta: hasMeta,
	})
	if mmh3 != "" {
		cm.mmh3Cache.Store(mmh3, path)
	}
	if hasDHash {
		cm.DHashTree.Add(dhash, path, size)
	}

	return dhashStr
}

func (cm *CacheManager) removeEntryLocked(path string) {
	val, ok := cm.paths.Load(path)
	if !ok {
		return
	}

	info := val.(CacheInfo)
	cm.paths.Delete(path)
	if info.MMH3 != "" {
		if mappedPath, ok := cm.mmh3Cache.Load(info.MMH3); ok && mappedPath.(string) == path {
			cm.mmh3Cache.Delete(info.MMH3)
		}
	}
}

func (cm *CacheManager) filterLiveMatchesLocked(matches []hasher.MatchResult) []hasher.MatchResult {
	liveMatches := make([]hasher.MatchResult, 0, len(matches))
	for _, match := range matches {
		val, ok := cm.paths.Load(match.Path)
		if !ok {
			continue
		}

		info := val.(CacheInfo)
		if info.DHash == "" || info.DHash != hasher.DHashToString(match.Hash) {
			continue
		}

		liveMatches = append(liveMatches, match)
	}

	return liveMatches
}

func rankMatches(matches []hasher.MatchResult) {
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Distance != matches[j].Distance {
			return matches[i].Distance < matches[j].Distance
		}
		if matches[i].Size != matches[j].Size {
			return matches[i].Size > matches[j].Size
		}
		return matches[i].Path < matches[j].Path
	})
}

func (cm *CacheManager) runWorker() {
	defer cm.workerWg.Done()

	// Helper to start/commit transactions on the DB directly for simplicity in this worker
	// since we want to handle errors and retries if needed.

	executeBatch := func(batch []cacheEntry) {
		tx, err := cm.db.Begin()
		if err != nil {
			log.Printf("CacheManager worker failed to begin transaction: %v", err)
			return
		}
		defer tx.Rollback()

		for _, entry := range batch {
			var err error
			if entry.isDelete {
				_, err = tx.Exec(`DELETE FROM file_cache WHERE target_path = ?`, entry.path)
			} else if entry.isAppend {
				_, err = tx.Exec(`UPDATE file_cache SET thumbnails = json_insert(CASE WHEN thumbnails = '' OR thumbnails IS NULL THEN '[]' ELSE thumbnails END, '$[#]', json(?)) WHERE target_path = ?`,
					entry.thumbJson, entry.path)
			} else if entry.isPromote {
				oldThumbs := "[]"
				if entry.promoteFrom != "" {
					scanErr := tx.QueryRow(`SELECT thumbnails FROM file_cache WHERE target_path = ?`, entry.promoteFrom).Scan(&oldThumbs)
					if scanErr != nil && scanErr != sql.ErrNoRows {
						err = scanErr
					}
				}
				newThumbs := "[]"
				if err == nil {
					scanErr := tx.QueryRow(`SELECT thumbnails FROM file_cache WHERE target_path = ?`, entry.path).Scan(&newThumbs)
					if scanErr != nil && scanErr != sql.ErrNoRows {
						err = scanErr
					}
				}
				if err == nil {
					merged := mergeThumbnailEntries(
						parseThumbnailEntries(newThumbs),
						parseThumbnailEntries(oldThumbs),
					)
					if entry.thumbPath != "" {
						merged = mergeThumbnailEntries(merged, []thumbnailEntry{makeThumbnailEntry(entry.thumbPath, "", entry.thumbMeta)})
					}
					err = setThumbnails(tx, entry.path, marshalThumbnailEntries(merged))
				}
				if err == nil && entry.promoteFrom != "" {
					err = deleteCacheRow(tx, entry.promoteFrom)
				}
			} else {
				_, err = tx.Exec(`
					INSERT INTO file_cache (target_path, mmh3_hash, phash, size, metadata)
					VALUES (?, ?, ?, ?, ?)
					ON CONFLICT(target_path) DO UPDATE SET
						mmh3_hash = excluded.mmh3_hash,
						phash = excluded.phash,
						size = excluded.size,
						metadata = excluded.metadata
				`,
					entry.path, entry.mmh3, entry.phash, entry.size, entry.metadata)
			}

			if err != nil {
				log.Printf("CacheManager worker DB error for path %s: %v", entry.path, err)
			}
		}

		if err := tx.Commit(); err != nil {
			log.Printf("CacheManager worker commit error: %v", err)
		}
	}

	var batch []cacheEntry
	for entry := range cm.entries {
		batch = append(batch, entry)
		if len(batch) >= cm.batchSize {
			executeBatch(batch)
			batch = nil
		}
	}

	if len(batch) > 0 {
		executeBatch(batch)
	}
}

func (cm *CacheManager) IsCached(targetPath string) bool {
	_, ok := cm.paths.Load(targetPath)
	return ok
}

// GetCachedInfo returns the cached information for a given path.
func (cm *CacheManager) GetCachedInfo(targetPath string) (CacheInfo, bool) {
	val, ok := cm.paths.Load(targetPath)
	if ok {
		return val.(CacheInfo), true
	}
	return CacheInfo{}, false
}

func (cm *CacheManager) FindExactMatch(mmh3 string) (string, bool) {
	if mmh3 == "" {
		return "", false
	}

	val, ok := cm.mmh3Cache.Load(mmh3)
	if ok {
		path := val.(string)
		infoVal, exists := cm.paths.Load(path)
		if !exists {
			cm.mmh3Cache.Delete(mmh3)
			return "", false
		}
		if infoVal.(CacheInfo).MMH3 != mmh3 {
			cm.mmh3Cache.Delete(mmh3)
			return "", false
		}
		return path, true
	}
	return "", false
}

// SearchDHash performs a search on the BKTree using first-stage dHash values.
func (cm *CacheManager) SearchDHash(dhash uint64, maxDistance int) []hasher.MatchResult {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	matches := cm.filterLiveMatchesLocked(cm.DHashTree.Search(dhash, maxDistance))
	rankMatches(matches)
	return matches
}

// SearchPHash is a compatibility alias for SearchDHash.
// Deprecated: this project uses dHash for first-stage lookup.
func (cm *CacheManager) SearchPHash(phash uint64, maxDistance int) []hasher.MatchResult {
	return cm.SearchDHash(phash, maxDistance)
}

// CheckAndAddPerceptualMatch atomically searches for a match and adds the entry if no match is found.
// Returns the match if found, or nil if the entry was added as unique.
func (cm *CacheManager) CheckAndAddPerceptualMatch(phash uint64, path string, size int64, mmh3 string) *hasher.MatchResult {
	return cm.CheckAndAddPerceptualMatchWithPresence(phash, phash != 0, path, size, mmh3)
}

// CheckAndAddPerceptualMatchWithPresence behaves like CheckAndAddPerceptualMatch but allows zero-valued hashes.
func (cm *CacheManager) CheckAndAddPerceptualMatchWithPresence(phash uint64, hasPHash bool, path string, size int64, mmh3 string) *hasher.MatchResult {
	cm.mutex.Lock()
	matches := cm.filterLiveMatchesLocked(cm.DHashTree.Search(phash, dedupe.CandidateSearchDistance))
	rankMatches(matches)
	if len(matches) > 0 {
		match := matches[0]
		cm.mutex.Unlock()
		return &match
	}

	// No match, add to memory state immediately
	phashStr := cm.setEntryLocked(path, mmh3, phash, hasPHash, size, false)
	cm.mutex.Unlock()

	// Queue for DB outside lock
	cm.entries <- cacheEntry{
		path:     path,
		mmh3:     mmh3,
		phash:    phashStr,
		size:     size,
		metadata: "{}",
	}

	return nil
}

// AddEntry adds a new entry to the in-memory state and triggers an asynchronous DB write.
func (cm *CacheManager) AddEntry(path string, mmh3 string, phash uint64, size int64, metadata string) {
	cm.AddEntryWithPresence(path, mmh3, phash, phash != 0, size, metadata)
}

// AddEntryWithPresence behaves like AddEntry but allows zero-valued hashes.
func (cm *CacheManager) AddEntryWithPresence(path string, mmh3 string, phash uint64, hasPHash bool, size int64, metadata string) {
	cm.mutex.Lock()
	phashStr := cm.setEntryLocked(path, mmh3, phash, hasPHash, size, metadata != "" && metadata != "{}")
	cm.mutex.Unlock()

	// Trigger asynchronous DB update outside lock
	cm.entries <- cacheEntry{
		path:     path,
		mmh3:     mmh3,
		phash:    phashStr,
		size:     size,
		metadata: metadata,
	}
}

// SetEntryMemory updates the in-memory cache without scheduling a database write.
func (cm *CacheManager) SetEntryMemory(path string, mmh3 string, phash uint64, size int64, metadata string) {
	cm.SetEntryMemoryWithPresence(path, mmh3, phash, phash != 0, size, metadata)
}

// SetEntryMemoryWithPresence behaves like SetEntryMemory but allows zero-valued hashes.
func (cm *CacheManager) SetEntryMemoryWithPresence(path string, mmh3 string, phash uint64, hasPHash bool, size int64, metadata string) {
	cm.mutex.Lock()
	cm.setEntryLocked(path, mmh3, phash, hasPHash, size, metadata != "" && metadata != "{}")
	cm.mutex.Unlock()
}

// AppendThumbnailToMaster sends an append instruction to the background DB worker.
func (cm *CacheManager) AppendThumbnailToMaster(masterPath, thumbPath, thumbMeta string) {
	thumbObjStr := fmt.Sprintf(`{"path":%q,"metadata":%s}`, thumbPath, thumbMeta)
	cm.entries <- cacheEntry{
		path:      masterPath,
		thumbJson: thumbObjStr,
		isAppend:  true,
	}
}

// DeleteEntry clears an entry from the memory state and triggers an asynchronous DB delete.
func (cm *CacheManager) DeleteEntry(path string) {
	cm.mutex.Lock()
	cm.removeEntryLocked(path)
	cm.mutex.Unlock()

	cm.entries <- cacheEntry{
		path:     path,
		isDelete: true,
	}
}

// PromoteMaster replaces oldMasterPath with newMasterPath while preserving the old master's
// existing thumbnail list and appending the moved old master as a thumbnail of the new master.
func (cm *CacheManager) PromoteMaster(newMasterPath, oldMasterPath, oldMasterThumbPath, oldMasterThumbMeta string) {
	cm.mutex.Lock()
	cm.removeEntryLocked(oldMasterPath)
	cm.mutex.Unlock()

	cm.entries <- cacheEntry{
		path:        newMasterPath,
		thumbPath:   oldMasterThumbPath,
		thumbMeta:   oldMasterThumbMeta,
		promoteFrom: oldMasterPath,
		isPromote:   true,
	}
}

// DeleteEntryMemory clears an entry from the in-memory cache without scheduling a database write.
func (cm *CacheManager) DeleteEntryMemory(path string) {
	cm.mutex.Lock()
	cm.removeEntryLocked(path)
	cm.mutex.Unlock()
}

func (cm *CacheManager) Close() error {
	close(cm.entries)
	cm.workerWg.Wait()
	return cm.db.Close()
}

func (cm *CacheManager) migrateOldCache(txtPath string) {
	file, err := os.Open(txtPath)
	if err != nil {
		return
	}
	defer file.Close()

	tx, err := cm.db.Begin()
	if err != nil {
		return
	}

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ",", 2)
		if len(parts) == 2 {
			hash := parts[0]
			path := parts[1]
			stat, err := os.Stat(path)
			var size int64
			if err == nil {
				size = stat.Size()
			}
			_, err = tx.Exec(`INSERT OR REPLACE INTO file_cache (target_path, mmh3_hash, phash, size, metadata) VALUES (?, ?, '', ?, '{}')`, path, hash, size)
			if err == nil {
				count++
			}
		}
	}
	tx.Commit()
	log.Printf("Migrated %d entries from %s. Deleting old txt cache...", count, txtPath)
	os.Remove(txtPath)
}
