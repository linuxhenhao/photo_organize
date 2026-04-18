package target

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/linuxhenhao/photo_organize/internal/dedupe"
	"github.com/linuxhenhao/photo_organize/internal/fsutil"
	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
)

const initCacheWorkers = 10

// InitCacheOptions controls how initcache reconciles the target directory.
type InitCacheOptions struct {
	MoveDuplicates bool
}

type fileCacheRow struct {
	MMH3       string
	PHash      string
	Size       int64
	Metadata   string
	Thumbnails string
}

type targetFile struct {
	Path     string
	MMH3     string
	PHash    uint64
	PHashStr string
	HasPHash bool
	Size     int64
	Metadata string
}

type thumbnailEntry struct {
	Path     string          `json:"path"`
	Metadata json.RawMessage `json:"metadata"`
}

type confirmedDuplicateMatch struct {
	Match hasher.MatchResult
}

type preparedTargetFile struct {
	Path string
	Row  fileCacheRow
	File targetFile
	Skip bool
	Err  error
}

// InitTargetDirCache defaults to read-only cache refresh.
func InitTargetDirCache(targetDir string, cm *CacheManager) {
	InitTargetDirCacheWithContext(context.Background(), targetDir, cm, InitCacheOptions{})
}

// InitTargetDirCacheWithOptions refreshes cache state and optionally moves duplicate files.
func InitTargetDirCacheWithOptions(targetDir string, cm *CacheManager, options InitCacheOptions) {
	InitTargetDirCacheWithContext(context.Background(), targetDir, cm, options)
}

// InitTargetDirCacheWithContext refreshes cache state and optionally moves duplicate files.
func InitTargetDirCacheWithContext(ctx context.Context, targetDir string, cm *CacheManager, options InitCacheOptions) {
	mode := "read-only"
	if options.MoveDuplicates {
		mode = "move-duplicates"
	}
	log.Printf("Initializing cache for target directory: %s (%s mode)", targetDir, mode)

	paths, thumbnailPaths, err := collectTargetPaths(ctx, targetDir)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Printf("Error walking target directory %s: %v", targetDir, err)
		return
	}

	rows, err := loadFileCacheRows(ctx, cm.db)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Printf("Failed to load existing cache rows: %v", err)
		return
	}

	if options.MoveDuplicates {
		if err := initTargetDirCacheMove(ctx, targetDir, paths, thumbnailPaths, rows, cm); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("Failed duplicate-moving cache initialization: %v", err)
		}
		return
	}

	if err := initTargetDirCacheReadOnly(ctx, paths, thumbnailPaths, rows, cm); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("Failed read-only cache initialization: %v", err)
	}
}

func collectTargetPaths(ctx context.Context, targetDir string) ([]string, []string, error) {
	thumbnailRoot := filepath.Join(targetDir, "thumbnails")
	paths := make([]string, 0)
	thumbnailPaths := make([]string, 0)

	err := filepath.Walk(targetDir, func(path string, info os.FileInfo, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			return nil
		}
		if info.IsDir() {
			if filepath.Clean(path) == filepath.Clean(thumbnailRoot) {
				return filepath.SkipDir
			}
			return nil
		}
		baseName := filepath.Base(path)
		if strings.HasPrefix(baseName, "cache.db") || baseName == "mmh3_hash_cache.txt" {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	if stat, err := os.Stat(thumbnailRoot); err == nil && stat.IsDir() {
		err = filepath.Walk(thumbnailRoot, func(path string, info os.FileInfo, err error) error {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if err != nil {
				return nil
			}
			if info.IsDir() {
				return nil
			}
			thumbnailPaths = append(thumbnailPaths, path)
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	}

	sort.Strings(paths)
	sort.Strings(thumbnailPaths)
	return paths, thumbnailPaths, nil
}

func loadFileCacheRows(ctx context.Context, db *sql.DB) (map[string]fileCacheRow, error) {
	rows, err := db.QueryContext(ctx, `SELECT target_path, mmh3_hash, phash, size, metadata, thumbnails FROM file_cache`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]fileCacheRow)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var path, mmh3, phash, metadataStr, thumbnails string
		var size int64
		if err := rows.Scan(&path, &mmh3, &phash, &size, &metadataStr, &thumbnails); err != nil {
			return nil, err
		}
		result[path] = fileCacheRow{
			MMH3:       mmh3,
			PHash:      phash,
			Size:       size,
			Metadata:   metadataStr,
			Thumbnails: thumbnails,
		}
	}

	return result, rows.Err()
}

func shouldRefreshPath(path string, row fileCacheRow, exists bool, stat os.FileInfo) bool {
	if !exists {
		return true
	}
	if row.Size != stat.Size() {
		return true
	}
	if row.MMH3 == "" {
		return true
	}
	if row.Metadata == "" || row.Metadata == "{}" {
		return true
	}
	if row.PHash != "" {
		return false
	}
	return hasher.CanVisualHash(path, "")
}

func buildTargetFile(path string, stat os.FileInfo, row fileCacheRow, exists bool) (targetFile, error) {
	sizeChanged := exists && row.Size != stat.Size()
	if sizeChanged {
		row = fileCacheRow{
			Thumbnails: row.Thumbnails,
		}
	}

	file := targetFile{
		Path: path,
		Size: stat.Size(),
	}

	if row.MMH3 != "" {
		file.MMH3 = row.MMH3
	} else {
		hash, err := hasher.CalculateHash(path)
		if err != nil {
			return targetFile{}, fmt.Errorf("failed to calculate hash for %s: %w", path, err)
		}
		file.MMH3 = hash
	}

	if row.Metadata != "" && row.Metadata != "{}" {
		file.Metadata = row.Metadata
	} else {
		file.Metadata = metadata.ExtractImageMetaJson(path)
	}

	if row.PHash != "" {
		hashVal, err := hasher.StringToPHash(row.PHash)
		if err == nil {
			file.PHash = hashVal
			file.PHashStr = row.PHash
			file.HasPHash = true
		}
	}

	if !file.HasPHash && hasher.CanVisualHash(path, "") {
		hashVal, err := hasher.CalculatePHash(path)
		if err == nil {
			file.PHash = hashVal
			file.PHashStr = hasher.PHashToString(hashVal)
			file.HasPHash = true
		}
	}

	return file, nil
}

func prepareTargetFiles(ctx context.Context, paths []string, rows map[string]fileCacheRow, refreshOnly bool) []preparedTargetFile {
	results := make([]preparedTargetFile, len(paths))
	if len(paths) == 0 {
		return results
	}

	jobs := make(chan int)
	workerCount := initCacheWorkers
	if workerCount > len(paths) {
		workerCount = len(paths)
	}

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				var idx int
				var ok bool
				select {
				case <-ctx.Done():
					return
				case idx, ok = <-jobs:
					if !ok {
						return
					}
					if ctx.Err() != nil {
						return
					}
				}

				path := paths[idx]
				prepared := preparedTargetFile{Path: path}

				stat, err := os.Stat(path)
				if err != nil {
					prepared.Err = err
					results[idx] = prepared
					continue
				}

				row, exists := rows[path]
				prepared.Row = row
				if refreshOnly && !shouldRefreshPath(path, row, exists, stat) {
					prepared.Skip = true
					results[idx] = prepared
					continue
				}

				file, err := buildTargetFile(path, stat, row, exists)
				if err != nil {
					prepared.Err = err
					results[idx] = prepared
					continue
				}

				prepared.File = file
				results[idx] = prepared
			}
		}()
	}

	for idx := range paths {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return results
		case jobs <- idx:
		}
	}
	close(jobs)
	wg.Wait()
	return results
}

func initTargetDirCacheReadOnly(ctx context.Context, paths []string, thumbnailPaths []string, rows map[string]fileCacheRow, cm *CacheManager) error {
	var refreshed int
	prepared := prepareTargetFiles(ctx, paths, rows, true)
	for _, entry := range prepared {
		if err := stopInitCacheAtSafePoint(ctx, "before refreshing the next cache entry"); err != nil {
			return err
		}
		if entry.Skip {
			continue
		}
		if entry.Err != nil {
			log.Printf("Failed to refresh cache entry for %s: %v", entry.Path, entry.Err)
			continue
		}

		file := entry.File
		if err := upsertMasterPreservingThumbnails(cm.db, file); err != nil {
			log.Printf("Failed to upsert cache entry for %s: %v", file.Path, err)
			continue
		}
		cm.SetEntryMemoryWithPresence(file.Path, file.MMH3, file.PHash, file.HasPHash, file.Size, file.Metadata)
		rows[file.Path] = fileCacheRow{
			MMH3:       file.MMH3,
			PHash:      file.PHashStr,
			Size:       file.Size,
			Metadata:   file.Metadata,
			Thumbnails: entry.Row.Thumbnails,
		}
		refreshed++
	}

	if err := stopInitCacheAtSafePoint(ctx, "before rebuilding thumbnail links"); err != nil {
		return err
	}
	if err := rebuildThumbnailLinks(ctx, thumbnailPaths, rows, cm); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		log.Printf("Failed to rebuild thumbnail links: %v", err)
	}

	log.Printf("Finished read-only cache initialization. %d entries refreshed.", refreshed)
	return nil
}

func initTargetDirCacheMove(ctx context.Context, targetDir string, paths []string, thumbnailPaths []string, rows map[string]fileCacheRow, cm *CacheManager) error {
	var processed int
	prepared := prepareTargetFiles(ctx, paths, rows, false)
	for _, entry := range prepared {
		if err := stopInitCacheAtSafePoint(ctx, "before processing the next duplicate candidate"); err != nil {
			return err
		}
		if entry.Err != nil {
			log.Printf("Failed to build cache entry for %s: %v", entry.Path, entry.Err)
			continue
		}

		file := entry.File
		if _, err := os.Stat(file.Path); err != nil {
			continue
		}

		var match *confirmedDuplicateMatch
		if file.HasPHash {
			var err error
			match, err = findConfirmedDuplicateMatch(ctx, file, rows, cm)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				log.Printf("Failed to confirm duplicates for %s: %v", file.Path, err)
				continue
			}
		}

		if match == nil {
			if err := upsertMasterPreservingThumbnails(cm.db, file); err != nil {
				log.Printf("Failed to upsert master entry for %s: %v", file.Path, err)
				continue
			}
			rows[file.Path] = fileCacheRow{
				MMH3:       file.MMH3,
				PHash:      file.PHashStr,
				Size:       file.Size,
				Metadata:   file.Metadata,
				Thumbnails: entry.Row.Thumbnails,
			}
			cm.SetEntryMemoryWithPresence(file.Path, file.MMH3, file.PHash, file.HasPHash, file.Size, file.Metadata)
			processed++
			continue
		}

		if err := stopInitCacheAtSafePoint(ctx, fmt.Sprintf("before moving derived variant %s into thumbnails", file.Path)); err != nil {
			return err
		}
		if err := demoteCurrentFile(targetDir, file, match.Match, rows, cm); err != nil {
			log.Printf("Failed to move derived variant %s to thumbnails: %v", file.Path, err)
			continue
		}
		processed++
	}

	if err := stopInitCacheAtSafePoint(ctx, "before rebuilding thumbnail links"); err != nil {
		return err
	}
	if err := rebuildThumbnailLinks(ctx, thumbnailPaths, rows, cm); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		log.Printf("Failed to rebuild existing thumbnail links: %v", err)
	}

	log.Printf("Finished duplicate-moving cache initialization. %d files processed.", processed)
	return nil
}

func findConfirmedDuplicateMatch(ctx context.Context, file targetFile, rows map[string]fileCacheRow, cm *CacheManager) (*confirmedDuplicateMatch, error) {
	matches := cm.SearchPHash(file.PHash, dedupe.CandidateSearchDistance)
	var best *confirmedDuplicateMatch
	var bestMeta metadata.MediaMeta
	ambiguous := false
	for _, candidate := range matches {
		if err := stopInitCacheAtSafePoint(ctx, fmt.Sprintf("before confirming duplicate candidates for %s", file.Path)); err != nil {
			return nil, err
		}
		if candidate.Path == file.Path {
			continue
		}

		if _, err := os.Stat(candidate.Path); err != nil {
			log.Printf("Removing stale cache entry for missing path %s during initcache", candidate.Path)
			if err := deleteCacheRow(cm.db, candidate.Path); err != nil {
				log.Printf("Failed to delete stale row for %s: %v", candidate.Path, err)
				continue
			}
			cm.DeleteEntryMemory(candidate.Path)
			delete(rows, candidate.Path)
			continue
		}

		row := rows[candidate.Path]
		decision, err := dedupe.ClassifyDerivative(file.Path, file.Metadata, file.Size, candidate.Path, row.Metadata, candidate.Size)
		if err != nil {
			log.Printf("Failed to confirm visual duplicate %s against %s: %v", file.Path, candidate.Path, err)
			continue
		}
		if decision.Confirmed {
			candidateMeta := metadata.ParseMediaMetaJSON(row.Metadata)
			if best == nil {
				best = &confirmedDuplicateMatch{Match: candidate}
				bestMeta = candidateMeta
				ambiguous = false
				continue
			}

			cmp := dedupe.CompareMasterPreference(candidate.Path, candidateMeta, candidate.Size, best.Match.Path, bestMeta, best.Match.Size)
			if cmp > 0 || (cmp == 0 && candidate.Distance < best.Match.Distance) {
				best = &confirmedDuplicateMatch{Match: candidate}
				bestMeta = candidateMeta
				ambiguous = false
				continue
			}
			if cmp == 0 && candidate.Distance == best.Match.Distance {
				ambiguous = true
			}
		}
	}
	if ambiguous {
		return nil, nil
	}
	return best, nil
}

func buildThumbnailPath(baseDir, filePath string) (string, error) {
	rel, err := filepath.Rel(baseDir, filePath)
	if err != nil {
		return "", err
	}

	targetDir := filepath.Join(baseDir, "thumbnails", filepath.Dir(rel))
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return "", err
	}

	baseName := filepath.Base(rel)
	ext := filepath.Ext(baseName)
	nameWithoutExt := strings.TrimSuffix(baseName, ext)
	candidate := filepath.Join(targetDir, baseName)
	if _, err := os.Stat(candidate); os.IsNotExist(err) {
		return candidate, nil
	}

	for suffix := 1; ; suffix++ {
		candidate = filepath.Join(targetDir, fmt.Sprintf("%s-%d%s", nameWithoutExt, suffix, ext))
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate, nil
		}
	}
}

func moveFileToThumbnails(baseDir, filePath string) (string, error) {
	thumbTarget, err := buildThumbnailPath(baseDir, filePath)
	if err != nil {
		return "", err
	}
	if err := os.Rename(filePath, thumbTarget); err != nil {
		return "", err
	}
	return thumbTarget, nil
}

func parseThumbnailEntries(raw string) []thumbnailEntry {
	if raw == "" || raw == "[]" {
		return nil
	}

	var entries []thumbnailEntry
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil
	}
	return entries
}

func marshalThumbnailEntries(entries []thumbnailEntry) string {
	if len(entries) == 0 {
		return "[]"
	}

	data, err := json.Marshal(entries)
	if err != nil {
		return "[]"
	}
	return string(data)
}

func mergeThumbnailEntries(groups ...[]thumbnailEntry) []thumbnailEntry {
	seen := make(map[string]bool)
	merged := make([]thumbnailEntry, 0)
	for _, group := range groups {
		for _, entry := range group {
			if entry.Path == "" || seen[entry.Path] {
				continue
			}
			seen[entry.Path] = true
			merged = append(merged, entry)
		}
	}
	return merged
}

func makeThumbnailEntry(path string, metadataJSON string) thumbnailEntry {
	if metadataJSON == "" {
		metadataJSON = "{}"
	}
	return thumbnailEntry{
		Path:     path,
		Metadata: json.RawMessage(metadataJSON),
	}
}

func deleteCacheRow(exec interface {
	Exec(query string, args ...any) (sql.Result, error)
}, path string) error {
	_, err := exec.Exec(`DELETE FROM file_cache WHERE target_path = ?`, path)
	return err
}

func upsertMasterPreservingThumbnails(exec interface {
	Exec(query string, args ...any) (sql.Result, error)
}, file targetFile) error {
	_, err := exec.Exec(`
		INSERT INTO file_cache (target_path, mmh3_hash, phash, size, metadata)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(target_path) DO UPDATE SET
			mmh3_hash = excluded.mmh3_hash,
			phash = excluded.phash,
			size = excluded.size,
			metadata = excluded.metadata
	`, file.Path, file.MMH3, file.PHashStr, file.Size, file.Metadata)
	return err
}

func replaceMasterWithThumbnails(tx *sql.Tx, file targetFile, thumbnails string) error {
	_, err := tx.Exec(`
		INSERT INTO file_cache (target_path, mmh3_hash, phash, size, metadata, thumbnails)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(target_path) DO UPDATE SET
			mmh3_hash = excluded.mmh3_hash,
			phash = excluded.phash,
			size = excluded.size,
			metadata = excluded.metadata,
			thumbnails = excluded.thumbnails
	`, file.Path, file.MMH3, file.PHashStr, file.Size, file.Metadata, thumbnails)
	return err
}

func setThumbnails(tx *sql.Tx, masterPath string, thumbnails string) error {
	result, err := tx.Exec(`UPDATE file_cache SET thumbnails = ? WHERE target_path = ?`, thumbnails, masterPath)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err == nil && rowsAffected == 0 {
		return fmt.Errorf("master row %s not found", masterPath)
	}
	return nil
}

func rollbackRename(currentPath, originalPath string) {
	if err := os.Rename(currentPath, originalPath); err != nil {
		log.Printf("Failed to roll back rename %s -> %s: %v", currentPath, originalPath, err)
	}
}

func rebuildThumbnailLinks(ctx context.Context, thumbnailPaths []string, rows map[string]fileCacheRow, cm *CacheManager) error {
	if len(thumbnailPaths) == 0 {
		return nil
	}

	aggregated := make(map[string][]thumbnailEntry)
	for masterPath, row := range rows {
		if err := stopInitCacheAtSafePoint(ctx, "while gathering existing thumbnail links"); err != nil {
			return err
		}
		if row.Thumbnails != "" && row.Thumbnails != "[]" {
			aggregated[masterPath] = append(aggregated[masterPath], parseThumbnailEntries(row.Thumbnails)...)
		}
	}

	preparedThumbs := prepareTargetFiles(ctx, thumbnailPaths, nil, false)
	for _, entry := range preparedThumbs {
		if err := stopInitCacheAtSafePoint(ctx, "before matching the next thumbnail"); err != nil {
			return err
		}
		if entry.Err != nil || !entry.File.HasPHash {
			continue
		}

		file := entry.File
		match, err := findConfirmedDuplicateMatch(ctx, file, rows, cm)
		if err != nil {
			return err
		}
		if match == nil {
			continue
		}

		thumbMeta := metadata.ExtractImageMetaJson(entry.Path)
		aggregated[match.Match.Path] = mergeThumbnailEntries(
			aggregated[match.Match.Path],
			[]thumbnailEntry{makeThumbnailEntry(entry.Path, thumbMeta)},
		)
	}

	if err := stopInitCacheAtSafePoint(ctx, "before persisting thumbnail links"); err != nil {
		return err
	}
	tx, err := cm.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for masterPath, entries := range aggregated {
		thumbJSON := marshalThumbnailEntries(entries)
		if err := setThumbnails(tx, masterPath, thumbJSON); err != nil {
			return err
		}
		row := rows[masterPath]
		row.Thumbnails = thumbJSON
		rows[masterPath] = row
	}

	return tx.Commit()
}

func stopInitCacheAtSafePoint(ctx context.Context, stage string) error {
	if err := ctx.Err(); err != nil {
		log.Printf("Stopping initcache at safe point: %s", stage)
		return err
	}
	return nil
}

func demoteCurrentFile(targetDir string, file targetFile, match hasher.MatchResult, rows map[string]fileCacheRow, cm *CacheManager) error {
	thumbPath, err := moveFileToThumbnails(targetDir, file.Path)
	if err != nil {
		return err
	}

	thumbMeta := metadata.ExtractImageMetaJson(thumbPath)
	mergedThumbs := mergeThumbnailEntries(
		parseThumbnailEntries(rows[match.Path].Thumbnails),
		parseThumbnailEntries(rows[file.Path].Thumbnails),
		[]thumbnailEntry{makeThumbnailEntry(thumbPath, thumbMeta)},
	)
	thumbJSON := marshalThumbnailEntries(mergedThumbs)

	tx, err := cm.db.Begin()
	if err != nil {
		rollbackRename(thumbPath, file.Path)
		return err
	}
	defer tx.Rollback()

	if err := deleteCacheRow(tx, file.Path); err != nil {
		rollbackRename(thumbPath, file.Path)
		return err
	}
	if err := setThumbnails(tx, match.Path, thumbJSON); err != nil {
		rollbackRename(thumbPath, file.Path)
		return err
	}
	if err := tx.Commit(); err != nil {
		rollbackRename(thumbPath, file.Path)
		return err
	}

	cm.DeleteEntryMemory(file.Path)
	delete(rows, file.Path)
	if err := fsutil.RemoveEmptyParentDirs(filepath.Dir(file.Path), targetDir); err != nil {
		log.Printf("Failed to remove empty directory for %s: %v", file.Path, err)
	}
	matchRow := rows[match.Path]
	matchRow.Thumbnails = thumbJSON
	rows[match.Path] = matchRow
	return nil
}
