package precompute

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Options struct {
	Workers int
	Force   bool
}

type thumbnailEntry struct {
	Path string `json:"path"`
	MMH3 string `json:"mmh3_hash,omitempty"`
}

type workItem struct {
	MMH3           string
	CandidatePaths []string
}

type workResult struct {
	MMH3        string
	Path        string
	Features    VisualFeatures
	ComputeErr  error
	UpsertErr   error
	WasComputed bool
}

func Run(ctx context.Context, targetDir string, db *sql.DB, options Options) error {
	if options.Workers <= 0 {
		options.Workers = runtime.NumCPU()
	}

	if err := EnsureVisualFeatureCacheTable(ctx, db); err != nil {
		return err
	}

	items, err := discoverWork(ctx, db)
	if err != nil {
		return err
	}

	logPrecompute("precompute_start",
		"workers", options.Workers,
		"force", options.Force,
		"discovered", len(items),
		"feature_version", visualFeatureVersion,
	)

	var queued int64
	var skipped int64
	var processed int64
	var failed int64
	var lastPath atomic.Value

	taskCh := make(chan workItem, 256)
	resultCh := make(chan workResult, 256)

	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for result := range resultCh {
			if result.ComputeErr != nil {
				atomic.AddInt64(&failed, 1)
				logPrecompute("precompute_failed",
					"mmh3_hash", result.MMH3,
					"path", result.Path,
					"stage", "compute",
					"error", result.ComputeErr,
				)
				continue
			}
			if result.UpsertErr != nil {
				atomic.AddInt64(&failed, 1)
				logPrecompute("precompute_failed",
					"mmh3_hash", result.MMH3,
					"path", result.Path,
					"stage", "upsert",
					"error", result.UpsertErr,
				)
				continue
			}

			atomic.AddInt64(&processed, 1)
			lastPath.Store(result.Path)
			count := atomic.LoadInt64(&processed)
			if count%50 == 0 {
				logPrecomputeProgress(count, atomic.LoadInt64(&queued), atomic.LoadInt64(&skipped), atomic.LoadInt64(&failed), lastPath.Load())
			}
		}
	}()

	stopProgress := make(chan struct{})
	doneProgress := make(chan struct{})
	go func() {
		defer close(doneProgress)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopProgress:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				logPrecomputeProgress(
					atomic.LoadInt64(&processed),
					atomic.LoadInt64(&queued),
					atomic.LoadInt64(&skipped),
					atomic.LoadInt64(&failed),
					lastPath.Load(),
				)
			}
		}
	}()

	var workerWg sync.WaitGroup
	workerWg.Add(options.Workers)
	for i := 0; i < options.Workers; i++ {
		go func() {
			defer workerWg.Done()
			for item := range taskCh {
				if err := ctx.Err(); err != nil {
					return
				}
				selectedPath, absPath, statErr := selectExistingPath(targetDir, item.CandidatePaths)
				if statErr != nil {
					resultCh <- workResult{
						MMH3:       item.MMH3,
						Path:       selectedPath,
						ComputeErr: statErr,
					}
					continue
				}

				features, computeErr := computeVisualFeatures(absPath)
				if computeErr != nil {
					resultCh <- workResult{
						MMH3:       item.MMH3,
						Path:       selectedPath,
						ComputeErr: computeErr,
					}
					continue
				}

				features.MMH3 = item.MMH3
				features.FeatureVersion = visualFeatureVersion

				upsertErr := upsertVisualFeatures(ctx, db, features)
				resultCh <- workResult{
					MMH3:        item.MMH3,
					Path:        selectedPath,
					Features:    features,
					UpsertErr:   upsertErr,
					WasComputed: true,
				}
			}
		}()
	}

	for _, item := range items {
		if err := ctx.Err(); err != nil {
			break
		}

		if !options.Force {
			exists, lookupErr := hasCachedVisualFeatures(ctx, db, item.MMH3, visualFeatureVersion)
			if lookupErr != nil {
				return lookupErr
			}
			if exists {
				atomic.AddInt64(&skipped, 1)
				continue
			}
		}

		atomic.AddInt64(&queued, 1)
		taskCh <- item
	}
	close(taskCh)

	workerWg.Wait()
	close(resultCh)
	writerWg.Wait()

	close(stopProgress)
	<-doneProgress

	logPrecompute("precompute_done",
		"workers", options.Workers,
		"feature_version", visualFeatureVersion,
		"queued", atomic.LoadInt64(&queued),
		"processed", atomic.LoadInt64(&processed),
		"skipped", atomic.LoadInt64(&skipped),
		"failed", atomic.LoadInt64(&failed),
	)

	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func discoverWork(ctx context.Context, db *sql.DB) ([]workItem, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT target_path, mmh3_hash, thumbnails
		FROM file_cache
		WHERE thumbnails IS NOT NULL AND thumbnails != '' AND thumbnails != '[]'
	`)
	if err != nil {
		return nil, fmt.Errorf("query cache rows: %w", err)
	}
	defer rows.Close()

	pathsByMMH3 := make(map[string][]string)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		var path string
		var mmh3 string
		var thumbsRaw string
		if err := rows.Scan(&path, &mmh3, &thumbsRaw); err != nil {
			return nil, fmt.Errorf("scan cache row: %w", err)
		}

		if mmh3 != "" {
			pathsByMMH3[mmh3] = append(pathsByMMH3[mmh3], path)
		}

		var thumbs []thumbnailEntry
		if err := json.Unmarshal([]byte(thumbsRaw), &thumbs); err != nil {
			return nil, fmt.Errorf("parse thumbnails for %s: %w", path, err)
		}
		for _, entry := range thumbs {
			if entry.Path == "" || entry.MMH3 == "" {
				continue
			}
			pathsByMMH3[entry.MMH3] = append(pathsByMMH3[entry.MMH3], entry.Path)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(pathsByMMH3))
	for mmh3 := range pathsByMMH3 {
		keys = append(keys, mmh3)
	}
	sort.Strings(keys)

	items := make([]workItem, 0, len(keys))
	for _, mmh3 := range keys {
		candidates := uniqueStringsPreserveOrder(pathsByMMH3[mmh3])
		items = append(items, workItem{MMH3: mmh3, CandidatePaths: candidates})
	}
	return items, nil
}

func uniqueStringsPreserveOrder(values []string) []string {
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func resolveStoredPath(targetDir, storedPath string) string {
	if storedPath == "" {
		return ""
	}
	if filepath.IsAbs(storedPath) {
		return filepath.Clean(storedPath)
	}
	return filepath.Clean(filepath.Join(targetDir, storedPath))
}

func selectExistingPath(targetDir string, candidates []string) (string, string, error) {
	var last string
	for _, path := range candidates {
		last = path
		abs := resolveStoredPath(targetDir, path)
		if abs == "" {
			continue
		}
		if _, err := os.Stat(abs); err == nil {
			return path, abs, nil
		}
	}
	if last == "" && len(candidates) > 0 {
		last = candidates[0]
	}
	return last, "", fmt.Errorf("no existing path among %d candidates", len(candidates))
}

func computeVisualFeatures(absPath string) (VisualFeatures, error) {
	return computeSecondStageFeatures(absPath)
}

func logPrecomputeProgress(processed int64, queued int64, skipped int64, failed int64, last any) {
	lastStr := ""
	if s, ok := last.(string); ok {
		lastStr = s
	}
	logPrecompute("precompute_progress",
		"processed", processed,
		"queued", queued,
		"skipped", skipped,
		"failed", failed,
		"last_path", lastStr,
	)
}

func logPrecompute(event string, fields ...any) {
	var b strings.Builder
	b.WriteString("precompute: ")
	b.WriteString(`event="`)
	b.WriteString(event)
	b.WriteString(`"`)
	for i := 0; i+1 < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok || key == "" {
			continue
		}
		b.WriteByte(' ')
		b.WriteString(key)
		b.WriteByte('=')
		value := fields[i+1]
		switch v := value.(type) {
		case string:
			b.WriteString(strconvQuote(v))
		case error:
			b.WriteString(strconvQuote(v.Error()))
		default:
			b.WriteString(fmt.Sprintf("%v", v))
		}
	}
	log.Print(b.String())
}

func strconvQuote(value string) string {
	// Keep log format stable without importing strconv everywhere.
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return `"` + value + `"`
}
