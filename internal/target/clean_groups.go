package target

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/linuxhenhao/photo_organize/internal/dedupe"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
)

// CleanGroupsOptions controls how thumbnail-group cleanup is executed.
type CleanGroupsOptions struct {
	Apply bool
}

// CleanGroupsReport summarizes the cleanup result.
type CleanGroupsReport struct {
	GroupsScanned      int
	GroupsChanged      int
	ThumbnailsScanned  int
	ThumbnailsRemoved  int
	ThumbnailsRehomed  int
	StandaloneCreated  int
	MissingRemoved     int
	StandaloneDeleted  int
	SkippedGroups      int
	ValidationFailures int
}

// CleanThumbnailGroupsWithContext revalidates thumbnail links in cache.db and
// repairs stale relationships without rescanning the whole repository.
func CleanThumbnailGroupsWithContext(ctx context.Context, targetDir string, cm *CacheManager, options CleanGroupsOptions) (CleanGroupsReport, error) {
	rows, err := loadFileCacheRows(ctx, cm.db)
	if err != nil {
		return CleanGroupsReport{}, err
	}

	masters := make([]string, 0)
	for path, row := range rows {
		if row.Thumbnails != "" && row.Thumbnails != "[]" {
			masters = append(masters, path)
		}
	}
	sort.Strings(masters)

	report := CleanGroupsReport{}
	changedMasters := make(map[string]bool)
	deletedStandalone := make(map[string]bool)
	createdStandalone := make(map[string]bool)
	prepared := make(map[string]targetFile)

	for _, masterPath := range masters {
		if err := ctx.Err(); err != nil {
			return report, err
		}

		row, ok := rows[masterPath]
		if !ok {
			continue
		}
		report.GroupsScanned++

		masterFile, err := loadStoredTargetFile(targetDir, masterPath, row, prepared)
		if err != nil {
			report.SkippedGroups++
			log.Printf("cleangroups: skipping master %s: %v", masterPath, err)
			continue
		}

		originalEntries := parseThumbnailEntries(row.Thumbnails)
		keptEntries := make([]thumbnailEntry, 0, len(originalEntries))
		groupChanged := false

		for _, entry := range originalEntries {
			if err := ctx.Err(); err != nil {
				return report, err
			}

			report.ThumbnailsScanned++
			entryAbsPath := resolveStoredPath(targetDir, entry.Path)
			entryStat, statErr := os.Stat(entryAbsPath)
			if statErr != nil {
				groupChanged = true
				report.ThumbnailsRemoved++
				report.MissingRemoved++
				log.Printf("cleangroups: removing missing thumbnail %s from %s", entry.Path, masterPath)
				continue
			}

			decision, err := dedupe.ClassifyDerivative(
				entryAbsPath,
				string(entry.Metadata),
				entryStat.Size(),
				masterFile.Path,
				masterFile.Metadata,
				masterFile.Size,
			)
			if err != nil {
				report.ValidationFailures++
				keptEntries = append(keptEntries, entry)
				log.Printf("cleangroups: keeping %s under %s because validation failed: %v", entry.Path, masterPath, err)
				continue
			}
			if decision.Confirmed {
				keptEntries = append(keptEntries, entry)
				continue
			}

			groupChanged = true
			report.ThumbnailsRemoved++

			entryFile, err := loadStoredTargetFile(targetDir, entry.Path, fileCacheRow{}, prepared)
			if err != nil {
				report.ValidationFailures++
				log.Printf("cleangroups: dropping %s from %s but failed to prepare rehome target: %v", entry.Path, masterPath, err)
				continue
			}

			targetMasterPath, err := findCleanupRehomeTarget(ctx, targetDir, cm, rows, prepared, entryFile, masterPath)
			if err != nil {
				return report, err
			}
			if targetMasterPath != "" {
				targetRow := rows[targetMasterPath]
				targetRow.Thumbnails = marshalThumbnailEntries(mergeThumbnailEntries(
					parseThumbnailEntries(targetRow.Thumbnails),
					[]thumbnailEntry{makeThumbnailEntry(entry.Path, entryFile.Metadata)},
				))
				rows[targetMasterPath] = targetRow
				changedMasters[targetMasterPath] = true
				report.ThumbnailsRehomed++

				if _, exists := rows[entry.Path]; exists {
					delete(rows, entry.Path)
					delete(prepared, entry.Path)
					cm.DeleteEntryMemory(entry.Path)
					deletedStandalone[entry.Path] = true
					report.StandaloneDeleted++
				}

				log.Printf("cleangroups: rehomed %s from %s to %s", entry.Path, masterPath, targetMasterPath)
				continue
			}

			if _, exists := rows[entry.Path]; !exists {
				rows[entry.Path] = fileCacheRow{
					MMH3:       entryFile.MMH3,
					PHash:      entryFile.PHashStr,
					Size:       entryFile.Size,
					Metadata:   entryFile.Metadata,
					Thumbnails: "[]",
				}
				prepared[entry.Path] = entryFile
				createdStandalone[entry.Path] = true
				cm.SetEntryMemoryWithPresence(entry.Path, entryFile.MMH3, entryFile.PHash, entryFile.HasPHash, entryFile.Size, entryFile.Metadata)
				report.StandaloneCreated++
				log.Printf("cleangroups: restored %s as standalone master", entry.Path)
			}
		}

		if groupChanged {
			row.Thumbnails = marshalThumbnailEntries(keptEntries)
			rows[masterPath] = row
			changedMasters[masterPath] = true
			report.GroupsChanged++
		}
	}

	if !options.Apply {
		log.Printf(
			"cleangroups dry-run: groups_scanned=%d groups_changed=%d thumbnails_scanned=%d removed=%d rehomed=%d standalone_created=%d missing_removed=%d standalone_deleted=%d validation_failures=%d skipped_groups=%d",
			report.GroupsScanned,
			report.GroupsChanged,
			report.ThumbnailsScanned,
			report.ThumbnailsRemoved,
			report.ThumbnailsRehomed,
			report.StandaloneCreated,
			report.MissingRemoved,
			report.StandaloneDeleted,
			report.ValidationFailures,
			report.SkippedGroups,
		)
		return report, nil
	}

	tx, err := cm.db.BeginTx(ctx, nil)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()

	deletePaths := make([]string, 0, len(deletedStandalone))
	for path := range deletedStandalone {
		deletePaths = append(deletePaths, path)
	}
	sort.Strings(deletePaths)
	for _, path := range deletePaths {
		if err := deleteCacheRow(tx, path); err != nil {
			return report, fmt.Errorf("delete stale standalone %s: %w", path, err)
		}
	}

	changedMasterPaths := make([]string, 0, len(changedMasters))
	for masterPath := range changedMasters {
		changedMasterPaths = append(changedMasterPaths, masterPath)
	}
	sort.Strings(changedMasterPaths)
	for _, masterPath := range changedMasterPaths {
		row, ok := rows[masterPath]
		if !ok {
			continue
		}
		if err := setThumbnails(tx, masterPath, row.Thumbnails); err != nil {
			return report, fmt.Errorf("update thumbnails for %s: %w", masterPath, err)
		}
	}

	newStandalonePaths := make([]string, 0, len(createdStandalone))
	for path := range createdStandalone {
		if deletedStandalone[path] {
			continue
		}
		newStandalonePaths = append(newStandalonePaths, path)
	}
	sort.Strings(newStandalonePaths)
	for _, path := range newStandalonePaths {
		row := rows[path]
		file := prepared[path]
		if err := replaceMasterWithThumbnails(tx, file, row.Thumbnails); err != nil {
			return report, fmt.Errorf("restore standalone %s: %w", path, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return report, err
	}

	log.Printf(
		"cleangroups apply: groups_scanned=%d groups_changed=%d thumbnails_scanned=%d removed=%d rehomed=%d standalone_created=%d missing_removed=%d standalone_deleted=%d validation_failures=%d skipped_groups=%d",
		report.GroupsScanned,
		report.GroupsChanged,
		report.ThumbnailsScanned,
		report.ThumbnailsRemoved,
		report.ThumbnailsRehomed,
		report.StandaloneCreated,
		report.MissingRemoved,
		report.StandaloneDeleted,
		report.ValidationFailures,
		report.SkippedGroups,
	)

	return report, nil
}

func findCleanupRehomeTarget(ctx context.Context, targetDir string, cm *CacheManager, rows map[string]fileCacheRow, prepared map[string]targetFile, candidate targetFile, excludedMaster string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	if exactPath, ok := cm.FindExactMatch(candidate.MMH3); ok && exactPath != "" && exactPath != excludedMaster && exactPath != candidate.Path {
		if _, exists := rows[exactPath]; exists && dedupe.CanAutoGroupUnderParent(candidate.Path, exactPath) {
			return exactPath, nil
		}
	}

	if !candidate.HasPHash {
		return "", nil
	}

	bestPath := ""
	bestMeta := metadata.MediaMeta{}
	bestDistance := 0
	ambiguous := false
	for _, match := range cm.SearchPHash(candidate.PHash, dedupe.CandidateSearchDistance) {
		if match.Path == excludedMaster || match.Path == candidate.Path {
			continue
		}

		row, ok := rows[match.Path]
		if !ok {
			continue
		}

		existingFile, err := loadStoredTargetFile(targetDir, match.Path, row, prepared)
		if err != nil {
			log.Printf("cleangroups: skipping candidate master %s during rehome: %v", match.Path, err)
			continue
		}

		decision, err := dedupe.ClassifyDerivative(
			candidate.Path,
			candidate.Metadata,
			candidate.Size,
			existingFile.Path,
			existingFile.Metadata,
			existingFile.Size,
		)
		if err != nil {
			log.Printf("cleangroups: rehome validation failed for %s -> %s: %v", candidate.Path, match.Path, err)
			continue
		}
		if decision.Confirmed {
			matchMeta := metadata.ParseMediaMetaJSON(existingFile.Metadata)
			if bestPath == "" {
				bestPath = match.Path
				bestMeta = matchMeta
				bestDistance = match.Distance
				ambiguous = false
				continue
			}

			cmp := dedupe.CompareMasterPreference(match.Path, matchMeta, match.Size, bestPath, bestMeta, rows[bestPath].Size)
			if cmp > 0 || (cmp == 0 && match.Distance < bestDistance) {
				bestPath = match.Path
				bestMeta = matchMeta
				bestDistance = match.Distance
				ambiguous = false
				continue
			}
			if cmp == 0 && match.Distance == bestDistance {
				ambiguous = true
			}
		}
	}

	if ambiguous {
		return "", nil
	}
	return bestPath, nil
}

func loadStoredTargetFile(targetDir, storedPath string, row fileCacheRow, prepared map[string]targetFile) (targetFile, error) {
	if file, ok := prepared[storedPath]; ok {
		return file, nil
	}

	resolved := resolveStoredPath(targetDir, storedPath)
	stat, err := os.Stat(resolved)
	if err != nil {
		return targetFile{}, err
	}

	file, err := buildTargetFile(resolved, stat, row, row.MMH3 != "" || row.PHash != "" || row.Metadata != "" || row.Size != 0)
	if err != nil {
		return targetFile{}, err
	}
	prepared[storedPath] = file
	return file, nil
}

func resolveStoredPath(targetDir, storedPath string) string {
	if storedPath == "" {
		return ""
	}
	if filepath.IsAbs(storedPath) {
		return filepath.Clean(storedPath)
	}

	cleanStored := filepath.Clean(storedPath)
	parentDir := filepath.Dir(targetDir)
	candidates := make([]string, 0, 3)
	if filepath.Base(targetDir) == strings.Split(cleanStored, string(filepath.Separator))[0] {
		candidates = append(candidates, filepath.Join(parentDir, cleanStored))
	}
	candidates = append(candidates, filepath.Join(targetDir, cleanStored))
	candidates = append(candidates, filepath.Join(parentDir, cleanStored))

	seen := make(map[string]bool)
	for _, candidate := range candidates {
		candidate = filepath.Clean(candidate)
		if seen[candidate] {
			continue
		}
		seen[candidate] = true
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}

	if filepath.Base(targetDir) == strings.Split(cleanStored, string(filepath.Separator))[0] {
		return filepath.Clean(filepath.Join(parentDir, cleanStored))
	}
	return filepath.Clean(filepath.Join(targetDir, cleanStored))
}
