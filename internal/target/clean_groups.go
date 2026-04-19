package target

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/linuxhenhao/photo_organize/internal/dedupe"
	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/linuxhenhao/photo_organize/internal/precompute"
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

type cleanupRehomeResult struct {
	TargetPath string
	Reason     string
}

// CleanThumbnailGroupsWithContext revalidates thumbnail links in cache.db and
// repairs stale relationships without rescanning the whole repository.
func CleanThumbnailGroupsWithContext(ctx context.Context, targetDir string, cm *CacheManager, options CleanGroupsOptions) (CleanGroupsReport, error) {
	rows, err := loadFileCacheRows(ctx, cm.db)
	if err != nil {
		return CleanGroupsReport{}, err
	}

	featureResolver, err := precompute.NewResolver(ctx, cm.db)
	if err != nil {
		return CleanGroupsReport{}, err
	}
	defer featureResolver.Close()

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
			logCleanGroups("skip_master",
				"mode", cleanGroupsMode(options.Apply),
				"master_path", masterPath,
				"error", err,
			)
			continue
		}

		masterResolved := resolveDedupeFeatures(ctx, featureResolver, masterFile.MMH3, masterFile.DHash, masterFile.HasDHash, masterFile.Path)

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
				logCleanGroups("missing_thumbnail",
					"mode", cleanGroupsMode(options.Apply),
					"action", "remove_missing_thumbnail",
					"master_path", masterPath,
					"thumbnail_path", entry.Path,
					"error", statErr,
				)
				continue
			}

			entryMMH3 := entry.MMH3
			if entryMMH3 == "" {
				if computed, hashErr := hasher.CalculateHash(entryAbsPath); hashErr == nil {
					entryMMH3 = computed
				}
			}
			entryDHash := uint64(0)
			entryHasDHash := false
			entryDHashStr := entry.DHash
			if entryDHashStr == "" {
				entryDHashStr = entry.PHash
			}
			if entryDHashStr != "" {
				if parsed, parseErr := hasher.StringToDHash(entryDHashStr); parseErr == nil {
					entryDHash = parsed
					entryHasDHash = true
				}
			}
			entryResolved := resolveDedupeFeatures(ctx, featureResolver, entryMMH3, entryDHash, entryHasDHash, entryAbsPath)

			decision, err := dedupe.RevalidateDerivativeWithResolvedFeatures(
				entryAbsPath,
				string(entry.Metadata),
				entryStat.Size(),
				entryResolved,
				masterFile.Path,
				masterFile.Metadata,
				masterFile.Size,
				masterResolved,
			)
			if err != nil {
				report.ValidationFailures++
				keptEntries = append(keptEntries, entry)
				logCleanGroups("validation_failed",
					"mode", cleanGroupsMode(options.Apply),
					"action", "keep_thumbnail",
					"master_path", masterPath,
					"thumbnail_path", entry.Path,
					"error", err,
				)
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
				logCleanGroups("prepare_rehome_failed",
					"mode", cleanGroupsMode(options.Apply),
					"action", "drop_thumbnail",
					"master_path", masterPath,
					"thumbnail_path", entry.Path,
					"error", err,
				)
				continue
			}

			rehomeResult, err := findCleanupRehomeTarget(ctx, targetDir, cm, rows, prepared, featureResolver, entryFile, masterPath)
			if err != nil {
				return report, err
			}
			targetMasterPath := rehomeResult.TargetPath
			if targetMasterPath != "" {
				targetRow := rows[targetMasterPath]
				targetRow.Thumbnails = marshalThumbnailEntries(mergeThumbnailEntries(
					parseThumbnailEntries(targetRow.Thumbnails),
					[]thumbnailEntry{makeThumbnailEntry(entry.Path, entryFile.MMH3, entryFile.DHashStr, entryFile.Metadata)},
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

				logCleanGroups("rehome",
					"mode", cleanGroupsMode(options.Apply),
					"thumbnail_path", entry.Path,
					"source_master", masterPath,
					"target_master", targetMasterPath,
					"rehome_reason", rehomeResult.Reason,
					"standalone_deleted", deletedStandalone[entry.Path],
				)
				continue
			}

			standaloneAction := "would_keep_existing_standalone"
			if _, exists := rows[entry.Path]; !exists {
				rows[entry.Path] = fileCacheRow{
					MMH3:       entryFile.MMH3,
					DHash:      entryFile.DHashStr,
					Size:       entryFile.Size,
					Metadata:   entryFile.Metadata,
					Thumbnails: "[]",
				}
				prepared[entry.Path] = entryFile
				createdStandalone[entry.Path] = true
				cm.SetEntryMemoryWithPresence(entry.Path, entryFile.MMH3, entryFile.DHash, entryFile.HasDHash, entryFile.Size, entryFile.Metadata)
				report.StandaloneCreated++
				standaloneAction = "restore_standalone"
			}
			logStandaloneDecision(entry.Path, masterPath, standaloneAction, rehomeResult.Reason, entryFile, options.Apply)
		}

		if groupChanged {
			row.Thumbnails = marshalThumbnailEntries(keptEntries)
			rows[masterPath] = row
			changedMasters[masterPath] = true
			report.GroupsChanged++
		}
	}

	hits, misses, invalid := featureResolver.Snapshot()
	logCleanGroups("feature_cache",
		"mode", cleanGroupsMode(options.Apply),
		"hits", hits,
		"misses", misses,
		"invalid", invalid,
	)

	if !options.Apply {
		logCleanGroupsSummary(report, options.Apply)
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

	logCleanGroupsSummary(report, options.Apply)

	return report, nil
}

func findCleanupRehomeTarget(ctx context.Context, targetDir string, cm *CacheManager, rows map[string]fileCacheRow, prepared map[string]targetFile, featureResolver *precompute.Resolver, candidate targetFile, excludedMaster string) (cleanupRehomeResult, error) {
	if err := ctx.Err(); err != nil {
		return cleanupRehomeResult{}, err
	}

	if exactPath, ok := cm.FindExactMatch(candidate.MMH3); ok && exactPath != "" && exactPath != excludedMaster && exactPath != candidate.Path {
		if _, exists := rows[exactPath]; exists {
			return cleanupRehomeResult{TargetPath: exactPath, Reason: "exact_hash_match"}, nil
		}
	}

	if !candidate.HasDHash {
		return cleanupRehomeResult{Reason: "no_phash"}, nil
	}

	bestPath := ""
	bestMeta := metadata.MediaMeta{}
	bestDistance := 0
	ambiguous := false
	validatedCandidate := false
	candidateResolved := resolveDedupeFeatures(ctx, featureResolver, candidate.MMH3, candidate.DHash, candidate.HasDHash, candidate.Path)
	for _, match := range cm.SearchDHash(candidate.DHash, dedupe.CandidateSearchDistance) {
		if match.Path == excludedMaster || match.Path == candidate.Path {
			continue
		}

		row, ok := rows[match.Path]
		if !ok {
			continue
		}

		existingFile, err := loadStoredTargetFile(targetDir, match.Path, row, prepared)
		if err != nil {
			logCleanGroups("skip_rehome_candidate",
				"candidate_path", candidate.Path,
				"match_path", match.Path,
				"error", err,
			)
			continue
		}

		existingResolved := resolveDedupeFeatures(ctx, featureResolver, existingFile.MMH3, existingFile.DHash, existingFile.HasDHash, existingFile.Path)
		decision, err := dedupe.RevalidateDerivativeWithResolvedFeatures(
			candidate.Path,
			candidate.Metadata,
			candidate.Size,
			candidateResolved,
			existingFile.Path,
			existingFile.Metadata,
			existingFile.Size,
			existingResolved,
		)
		if err != nil {
			logCleanGroups("rehome_validation_failed",
				"candidate_path", candidate.Path,
				"match_path", match.Path,
				"error", err,
			)
			continue
		}
		validatedCandidate = true
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
		return cleanupRehomeResult{Reason: "ambiguous_phash_match"}, nil
	}
	if bestPath != "" {
		return cleanupRehomeResult{TargetPath: bestPath, Reason: "validated_phash_match"}, nil
	}
	if validatedCandidate {
		return cleanupRehomeResult{Reason: "validated_candidates_rejected"}, nil
	}
	return cleanupRehomeResult{Reason: "no_match_found"}, nil
}

func logStandaloneDecision(path, sourceMaster, action, rehomeReason string, file targetFile, apply bool) {
	meta := metadata.ParseMediaMetaJSON(file.Metadata)
	logCleanGroups("standalone",
		"mode", cleanGroupsMode(apply),
		"action", action,
		"path", path,
		"source_master", sourceMaster,
		"rehome_reason", rehomeReason,
		"size", file.Size,
		"dimensions", formatStandaloneDimensions(meta),
		"create_time", standaloneLogValue(meta.CreateTime),
		"has_dhash", file.HasDHash,
		"has_phash", file.HasDHash,
	)
}

func logCleanGroupsSummary(report CleanGroupsReport, apply bool) {
	logCleanGroups("summary",
		"mode", cleanGroupsMode(apply),
		"groups_scanned", report.GroupsScanned,
		"groups_changed", report.GroupsChanged,
		"thumbnails_scanned", report.ThumbnailsScanned,
		"removed", report.ThumbnailsRemoved,
		"rehomed", report.ThumbnailsRehomed,
		"standalone_created", report.StandaloneCreated,
		"missing_removed", report.MissingRemoved,
		"standalone_deleted", report.StandaloneDeleted,
		"validation_failures", report.ValidationFailures,
		"skipped_groups", report.SkippedGroups,
	)
}

func cleanGroupsMode(apply bool) string {
	if apply {
		return "apply"
	}
	return "dry-run"
}

func logCleanGroups(event string, fields ...any) {
	var b strings.Builder
	b.WriteString("cleangroups:")
	b.WriteString(" event=")
	b.WriteString(strconv.Quote(event))
	for i := 0; i+1 < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok || key == "" {
			continue
		}
		b.WriteByte(' ')
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(formatCleanGroupsLogValue(fields[i+1]))
	}
	_ = log.Output(2, b.String())
}

func formatCleanGroupsLogValue(value any) string {
	switch v := value.(type) {
	case nil:
		return strconv.Quote("unknown")
	case string:
		return strconv.Quote(standaloneLogValue(v))
	case error:
		if v == nil {
			return strconv.Quote("unknown")
		}
		return strconv.Quote(v.Error())
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case time.Time:
		if v.IsZero() {
			return strconv.Quote("unknown")
		}
		return strconv.Quote(v.Format(time.RFC3339))
	default:
		return strconv.Quote(fmt.Sprint(v))
	}
}

func formatStandaloneDimensions(meta metadata.MediaMeta) string {
	if meta.Width <= 0 && meta.Height <= 0 {
		return "unknown"
	}
	return strconv.Itoa(meta.Width) + "x" + strconv.Itoa(meta.Height)
}

func standaloneLogValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return "unknown"
	}
	return value
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

	file, err := buildTargetFile(resolved, stat, row, row.MMH3 != "" || row.DHash != "" || row.Metadata != "" || row.Size != 0)
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
