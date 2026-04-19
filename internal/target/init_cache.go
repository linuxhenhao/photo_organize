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
	"github.com/linuxhenhao/photo_organize/internal/precompute"
)

const initCacheWorkers = 10

// InitCacheOptions controls how initcache reconciles the target directory.
type InitCacheOptions struct {
	MoveDuplicates bool
	SkipRebuild    bool
}

type fileCacheRow struct {
	MMH3       string
	DHash      string
	Size       int64
	Metadata   string
	Thumbnails string
}

type targetFile struct {
	Path     string
	MMH3     string
	DHash    uint64
	DHashStr string
	HasDHash bool
	Size     int64
	Metadata string
}

type thumbnailEntry struct {
	Path string `json:"path"`
	MMH3 string `json:"mmh3_hash,omitempty"`
	// DHash is the canonical field written by modern code.
	DHash string `json:"dhash,omitempty"`
	// PHash is a legacy field name that historically held dHash values.
	PHash    string          `json:"phash,omitempty"`
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
		if err := initTargetDirCacheMove(ctx, targetDir, paths, thumbnailPaths, rows, cm, options.SkipRebuild); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("Failed duplicate-moving cache initialization: %v", err)
		}
		return
	}

	if err := initTargetDirCacheReadOnly(ctx, targetDir, paths, thumbnailPaths, rows, cm, options.SkipRebuild); err != nil && !errors.Is(err, context.Canceled) {
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
		var path, mmh3, dhashStr, metadataStr, thumbnails string
		var size int64
		if err := rows.Scan(&path, &mmh3, &dhashStr, &size, &metadataStr, &thumbnails); err != nil {
			return nil, err
		}
		result[path] = fileCacheRow{
			MMH3:       mmh3,
			DHash:      dhashStr,
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
	if row.DHash != "" {
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

	if row.DHash != "" {
		hashVal, err := hasher.StringToDHash(row.DHash)
		if err == nil {
			file.DHash = hashVal
			file.DHashStr = row.DHash
			file.HasDHash = true
		}
	}

	if !file.HasDHash && hasher.CanVisualHash(path, "") {
		hashVal, err := hasher.CalculateDHash(path)
		if err == nil {
			file.DHash = hashVal
			file.DHashStr = hasher.DHashToString(hashVal)
			file.HasDHash = true
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

func initTargetDirCacheReadOnly(ctx context.Context, targetDir string, paths []string, thumbnailPaths []string, rows map[string]fileCacheRow, cm *CacheManager, skipRebuild bool) error {
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
		cm.SetEntryMemoryWithPresence(file.Path, file.MMH3, file.DHash, file.HasDHash, file.Size, file.Metadata)
		rows[file.Path] = fileCacheRow{
			MMH3:       file.MMH3,
			DHash:      file.DHashStr,
			Size:       file.Size,
			Metadata:   file.Metadata,
			Thumbnails: entry.Row.Thumbnails,
		}
		refreshed++
	}

	if err := stopInitCacheAtSafePoint(ctx, "before backfilling thumbnail mmh3 hashes"); err != nil {
		return err
	}
	if err := backfillThumbnailEntryHashes(ctx, targetDir, rows, cm.db); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		log.Printf("Failed to backfill thumbnail mmh3 hashes: %v", err)
	}

	if err := stopInitCacheAtSafePoint(ctx, "before rebuilding thumbnail links"); err != nil {
		return err
	}
	if skipRebuild {
		log.Printf("Skipping rebuildThumbnailLinks during initcache.")
		log.Printf("Finished read-only cache initialization. %d entries refreshed.", refreshed)
		return nil
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

func backfillThumbnailEntryHashes(ctx context.Context, targetDir string, rows map[string]fileCacheRow, db *sql.DB) error {
	type thumbnailRef struct {
		masterPath string
		index      int
	}

	pathRefs := make(map[string][]thumbnailRef)
	for masterPath, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries := parseThumbnailEntries(row.Thumbnails)
		for idx, entry := range entries {
			entryDHash := entry.DHash
			if entryDHash == "" {
				entryDHash = entry.PHash
			}
			if entry.Path == "" || (entry.MMH3 != "" && entryDHash != "") {
				continue
			}
			pathRefs[entry.Path] = append(pathRefs[entry.Path], thumbnailRef{masterPath: masterPath, index: idx})
		}
	}

	if len(pathRefs) == 0 {
		return nil
	}

	type hashResult struct {
		path  string
		mmh3  string
		phash string
		err   error
	}

	paths := make([]string, 0, len(pathRefs))
	for path := range pathRefs {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	jobs := make(chan string, len(paths))
	results := make(chan hashResult, len(paths))
	workerCount := initCacheWorkers
	if workerCount > len(paths) {
		workerCount = len(paths)
	}

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				resolved := resolveStoredPath(targetDir, path)
				mmh3, mmh3Err := hasher.CalculateHash(resolved)
				if mmh3Err != nil {
					results <- hashResult{path: path, err: mmh3Err}
					continue
				}

				phash := ""
				if hasher.CanVisualHash(resolved, "") {
					if dhash, dhashErr := hasher.CalculateDHash(resolved); dhashErr == nil {
						phash = hasher.DHashToString(dhash)
					}
				}
				results <- hashResult{path: path, mmh3: mmh3, phash: phash, err: nil}
			}
		}()
	}

	for _, path := range paths {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			close(results)
			return ctx.Err()
		case jobs <- path:
		}
	}
	close(jobs)
	go func() {
		wg.Wait()
		close(results)
	}()

	updatedMasters := make(map[string][]thumbnailEntry)
	backfilled := 0
	for result := range results {
		if err := ctx.Err(); err != nil {
			return err
		}
		if result.err != nil {
			log.Printf("Failed to backfill thumbnail mmh3 for %s: %v", result.path, result.err)
			continue
		}

		for _, ref := range pathRefs[result.path] {
			entries, ok := updatedMasters[ref.masterPath]
			if !ok {
				entries = parseThumbnailEntries(rows[ref.masterPath].Thumbnails)
			}
			if ref.index >= len(entries) {
				updatedMasters[ref.masterPath] = entries
				continue
			}
			changed := false
			if entries[ref.index].MMH3 == "" && result.mmh3 != "" {
				entries[ref.index].MMH3 = result.mmh3
				changed = true
			}
			entryDHash := entries[ref.index].DHash
			if entryDHash == "" {
				entryDHash = entries[ref.index].PHash
			}
			if entryDHash == "" && result.phash != "" {
				entries[ref.index].DHash = result.phash
				entries[ref.index].PHash = ""
				changed = true
			}
			updatedMasters[ref.masterPath] = entries
			if changed {
				backfilled++
			}
		}
	}

	if len(updatedMasters) == 0 {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	masterPaths := make([]string, 0, len(updatedMasters))
	for masterPath := range updatedMasters {
		masterPaths = append(masterPaths, masterPath)
	}
	sort.Strings(masterPaths)

	for _, masterPath := range masterPaths {
		thumbJSON := marshalThumbnailEntries(updatedMasters[masterPath])
		if err := setThumbnails(tx, masterPath, thumbJSON); err != nil {
			return err
		}
		row := rows[masterPath]
		row.Thumbnails = thumbJSON
		rows[masterPath] = row
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Backfilled identifiers for %d thumbnail entries in read-only initcache.", backfilled)
	return nil
}

func initTargetDirCacheMove(ctx context.Context, targetDir string, paths []string, thumbnailPaths []string, rows map[string]fileCacheRow, cm *CacheManager, skipRebuild bool) error {
	var processed int
	featureResolver, err := precompute.NewResolver(ctx, cm.db)
	if err != nil {
		return err
	}
	defer featureResolver.Close()

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
		if file.HasDHash {
			match, err = findConfirmedDuplicateMatch(ctx, file, rows, cm, featureResolver)
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
				DHash:      file.DHashStr,
				Size:       file.Size,
				Metadata:   file.Metadata,
				Thumbnails: entry.Row.Thumbnails,
			}
			cm.SetEntryMemoryWithPresence(file.Path, file.MMH3, file.DHash, file.HasDHash, file.Size, file.Metadata)
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
	if skipRebuild {
		log.Printf("Skipping rebuildThumbnailLinks during initcache.")
		log.Printf("Finished duplicate-moving cache initialization. %d files processed.", processed)
		return nil
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

func findConfirmedDuplicateMatch(ctx context.Context, file targetFile, rows map[string]fileCacheRow, cm *CacheManager, featureResolver *precompute.Resolver) (*confirmedDuplicateMatch, error) {
	matches := cm.SearchDHash(file.DHash, dedupe.CandidateSearchDistance)
	var best *confirmedDuplicateMatch
	var bestMeta metadata.MediaMeta
	ambiguous := false
	childFeatures := resolveDedupeFeatures(ctx, featureResolver, file.MMH3, file.DHash, file.HasDHash, file.Path)
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
		parentDHash := uint64(0)
		parentHasDHash := false
		if row.DHash != "" {
			if parsed, parseErr := hasher.StringToDHash(row.DHash); parseErr == nil {
				parentDHash = parsed
				parentHasDHash = true
			}
		}
		parentFeatures := resolveDedupeFeatures(ctx, featureResolver, row.MMH3, parentDHash, parentHasDHash, candidate.Path)

		decision, err := dedupe.ClassifyDerivativeWithResolvedFeatures(
			file.Path, file.Metadata, file.Size, childFeatures,
			candidate.Path, row.Metadata, candidate.Size, parentFeatures,
		)
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
	indexByPath := make(map[string]int)
	merged := make([]thumbnailEntry, 0)
	for _, group := range groups {
		for _, entry := range group {
			if entry.Path == "" {
				continue
			}
			if idx, ok := indexByPath[entry.Path]; ok {
				if merged[idx].MMH3 == "" && entry.MMH3 != "" {
					merged[idx].MMH3 = entry.MMH3
				}
				mergedDHash := merged[idx].DHash
				if mergedDHash == "" {
					mergedDHash = merged[idx].PHash
				}
				entryDHash := entry.DHash
				if entryDHash == "" {
					entryDHash = entry.PHash
				}
				if mergedDHash == "" && entryDHash != "" {
					merged[idx].DHash = entryDHash
					merged[idx].PHash = ""
				}
				if len(merged[idx].Metadata) == 0 || string(merged[idx].Metadata) == "{}" {
					if len(entry.Metadata) > 0 && string(entry.Metadata) != "{}" {
						merged[idx].Metadata = entry.Metadata
					}
				}
				continue
			}
			indexByPath[entry.Path] = len(merged)
			merged = append(merged, entry)
		}
	}
	return merged
}

func makeThumbnailEntry(path string, parts ...string) thumbnailEntry {
	mmh3 := ""
	phash := ""
	metadataJSON := ""
	switch len(parts) {
	case 0:
	case 1:
		metadataJSON = parts[0]
	case 2:
		mmh3 = parts[0]
		metadataJSON = parts[1]
	default:
		mmh3 = parts[0]
		phash = parts[1]
		metadataJSON = parts[2]
	}
	if metadataJSON == "" {
		metadataJSON = "{}"
	}
	return thumbnailEntry{
		Path:     path,
		MMH3:     mmh3,
		DHash:    phash,
		PHash:    "",
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
	`, file.Path, file.MMH3, file.DHashStr, file.Size, file.Metadata)
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
	`, file.Path, file.MMH3, file.DHashStr, file.Size, file.Metadata, thumbnails)
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

	featureResolver, err := precompute.NewResolver(ctx, cm.db)
	if err != nil {
		return err
	}
	defer featureResolver.Close()

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
		if entry.Err != nil || !entry.File.HasDHash {
			continue
		}

		file := entry.File
		match, err := findConfirmedDuplicateMatch(ctx, file, rows, cm, featureResolver)
		if err != nil {
			return err
		}
		if match == nil {
			continue
		}

		thumbMeta := metadata.ExtractImageMetaJson(entry.Path)
		aggregated[match.Match.Path] = mergeThumbnailEntries(
			aggregated[match.Match.Path],
			[]thumbnailEntry{makeThumbnailEntry(entry.Path, entry.File.MMH3, entry.File.DHashStr, thumbMeta)},
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
		[]thumbnailEntry{makeThumbnailEntry(thumbPath, file.MMH3, file.DHashStr, thumbMeta)},
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
