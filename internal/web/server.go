package web

import (
	"crypto/md5"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	projectexiftool "github.com/linuxhenhao/photo_organize/internal/exiftool"
	"github.com/linuxhenhao/photo_organize/internal/fsutil"
	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/linuxhenhao/photo_organize/internal/target"
	"golang.org/x/sys/unix"
)

//go:embed static/*
var staticFS embed.FS

// WebServer handles the Web UI for duplicate resolution.
type WebServer struct {
	cm                  *target.CacheManager
	db                  *sql.DB
	destDir             string
	previewForPath      func(string) ([]byte, string, error)
	thumbnailPathFor    func(string) string
	thumbnailForPath    func(string) string
	thumbnailCandidates []string
	xattrForPath        func(string, string) (string, error)
	ugosThumbnailMode   bool
	prewarmWorkers      int
	thumbnailCache      sync.Map
}

// ImageInfo holds metadata for the frontend
type ImageInfo struct {
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	Width      int    `json:"width"`
	Height     int    `json:"height"`
	CreateTime string `json:"createTime"`
	IsMaster   bool   `json:"isMaster"`
}

// DuplicateGroup represents a group of visually identical photos
type DuplicateGroup struct {
	Master     ImageInfo   `json:"master"`
	Duplicates []ImageInfo `json:"duplicates"`
}

// NewWebServer initializes the web server backend
func NewWebServer(cm *target.CacheManager, destDir string) *WebServer {
	ws := &WebServer{
		cm:               cm,
		destDir:          destDir,
		previewForPath:   extractPreviewForBrowser,
		thumbnailForPath: synologyThumbnailBasePathFor,
		thumbnailCandidates: []string{
			"_640_40.webp",
			"_640_40.jpg",
			"_320_40.webp",
			"_320_40.jpg",
			".webp",
			".jpg",
		},
		xattrForPath:      readXattrString,
		ugosThumbnailMode: detectUGOSThumbnailSystem(),
		prewarmWorkers:    defaultThumbnailPrewarmWorkers(),
	}
	ws.thumbnailPathFor = ws.cachedThumbnailPathFor
	return ws
}

func listenAddr(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func validateListenHost(host string) error {
	trimmed := strings.TrimSpace(host)
	if trimmed == "" {
		return fmt.Errorf("listen host is required")
	}

	unwrapped := strings.TrimPrefix(strings.TrimSuffix(trimmed, "]"), "[")
	if ip := net.ParseIP(unwrapped); ip != nil && ip.IsUnspecified() {
		return fmt.Errorf("refusing to listen on unrestricted host %q; bind to a specific LAN address instead", host)
	}

	return nil
}

func pathWithinRoot(root string, candidate string) bool {
	rel, err := filepath.Rel(root, candidate)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}

func (ws *WebServer) resolveWithinDest(requestPath string) (string, error) {
	if requestPath == "" {
		return "", fmt.Errorf("path is required")
	}

	absDest, err := filepath.Abs(ws.destDir)
	if err != nil {
		return "", fmt.Errorf("failed to resolve destination: %w", err)
	}

	candidates := make([]string, 0, 3)
	if filepath.IsAbs(requestPath) {
		candidates = append(candidates, requestPath)
	} else {
		cleanPath := filepath.Clean(requestPath)
		destBase := filepath.Base(filepath.Clean(ws.destDir))
		trimmedPath := cleanPath
		if cleanPath == destBase {
			trimmedPath = "."
		} else if strings.HasPrefix(cleanPath, destBase+string(os.PathSeparator)) {
			trimmedPath = strings.TrimPrefix(cleanPath, destBase+string(os.PathSeparator))
		}

		candidates = append(candidates,
			cleanPath,
			filepath.Join(ws.destDir, cleanPath),
			filepath.Join(ws.destDir, trimmedPath),
		)
	}

	for _, candidate := range candidates {
		absPath, err := filepath.Abs(candidate)
		if err != nil {
			continue
		}
		if pathWithinRoot(absDest, absPath) {
			return absPath, nil
		}
	}

	return "", fmt.Errorf("path escapes destination")
}

func hashFileForCache(path string) (string, uint64, bool, error) {
	mmh3, err := hasher.CalculateHash(path)
	if err != nil {
		return "", 0, false, err
	}

	phash, err := hasher.CalculatePHash(path)
	if err != nil {
		return mmh3, 0, false, nil
	}

	return mmh3, phash, true, nil
}

type groupMember struct {
	raw      string
	abs      string
	isMaster bool
}

type renameRecord struct {
	currentPath  string
	originalPath string
}

type resolvedStandaloneEntry struct {
	storedPath string
	hash       string
	phash      uint64
	hasPHash   bool
	size       int64
	metadata   string
}

func dedupePaths(paths []string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0, len(paths))
	for _, path := range paths {
		trimmed := strings.TrimSpace(path)
		if trimmed == "" || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		result = append(result, trimmed)
	}
	return result
}

func (ws *WebServer) storagePathForResolved(absPath string) (string, error) {
	if filepath.IsAbs(ws.destDir) {
		return absPath, nil
	}

	absDest, err := filepath.Abs(ws.destDir)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(absDest, absPath)
	if err != nil {
		return "", err
	}
	return filepath.Clean(filepath.Join(ws.destDir, rel)), nil
}

func (ws *WebServer) loadGroupMembers(masterRaw string, masterAbs string) ([]groupMember, error) {
	var thumbnailsRaw string
	if err := ws.db.QueryRow(`SELECT thumbnails FROM file_cache WHERE target_path = ?`, masterRaw).Scan(&thumbnailsRaw); err != nil {
		return nil, err
	}

	members := []groupMember{{raw: masterRaw, abs: masterAbs, isMaster: true}}
	if thumbnailsRaw == "" || thumbnailsRaw == "[]" {
		return members, nil
	}

	var thumbs []struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal([]byte(thumbnailsRaw), &thumbs); err != nil {
		return nil, err
	}

	for _, thumb := range thumbs {
		if thumb.Path == "" {
			continue
		}
		thumbAbs, err := ws.resolveWithinDest(thumb.Path)
		if err != nil {
			return nil, err
		}
		members = append(members, groupMember{raw: thumb.Path, abs: thumbAbs})
	}

	return members, nil
}

func fileNameWithSuffix(name string, suffix int, ext string) string {
	return fmt.Sprintf("%s-%d%s", name, suffix, ext)
}

func (ws *WebServer) isThumbnailAbs(absPath string) bool {
	absDest, err := filepath.Abs(ws.destDir)
	if err != nil {
		return false
	}
	thumbRoot := filepath.Join(absDest, "thumbnails")
	return pathWithinRoot(thumbRoot, absPath)
}

func (ws *WebServer) restoreThumbnailDestination(absThumbPath string) (string, error) {
	absDest, err := filepath.Abs(ws.destDir)
	if err != nil {
		return "", err
	}
	thumbRoot := filepath.Join(absDest, "thumbnails")

	rel, err := filepath.Rel(thumbRoot, absThumbPath)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("thumbnail path escapes thumbnails root")
	}

	targetDir := filepath.Join(absDest, filepath.Dir(rel))
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return "", err
	}

	baseName := filepath.Base(rel)
	ext := filepath.Ext(baseName)
	nameWithoutExt := strings.TrimSuffix(baseName, ext)
	candidate := filepath.Join(targetDir, baseName)
	if _, err := os.Stat(candidate); os.IsNotExist(err) {
		return candidate, nil
	} else if err != nil {
		return "", err
	}

	for suffix := 1; ; suffix++ {
		candidate = filepath.Join(targetDir, fileNameWithSuffix(nameWithoutExt, suffix, ext))
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate, nil
		} else if err != nil {
			return "", err
		}
	}
}

func rollbackRenames(renames []renameRecord) {
	for i := len(renames) - 1; i >= 0; i-- {
		record := renames[i]
		if err := os.Rename(record.currentPath, record.originalPath); err != nil {
			log.Printf("Failed to roll back rename %s -> %s: %v", record.currentPath, record.originalPath, err)
		}
	}
}

func browserRenderableContentType(path string) (string, bool) {
	file, err := os.Open(path)
	if err != nil {
		return "", false
	}
	defer file.Close()

	header := make([]byte, 512)
	n, err := io.ReadFull(file, header)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", false
	}
	contentType := http.DetectContentType(header[:n])
	switch contentType {
	case "image/jpeg", "image/png", "image/gif", "image/webp", "image/bmp", "image/avif":
		return contentType, true
	default:
		return "", false
	}
}

func extractPreviewForBrowser(path string) ([]byte, string, error) {
	pool, err := projectexiftool.SharedPool()
	if err != nil {
		return nil, "", err
	}

	results, err := pool.Extract([]string{path}, []string{
		"PreviewImage",
		"JpgFromRaw",
		"ThumbnailImage",
	}, projectexiftool.QueryOptions{
		Binary:            true,
		IgnoreMinorErrors: true,
	})
	if err != nil {
		return nil, "", err
	}
	if len(results) != 1 {
		return nil, "", fmt.Errorf("unexpected exiftool result count for %s: %d", path, len(results))
	}

	for _, key := range []string{"PreviewImage", "JpgFromRaw", "ThumbnailImage"} {
		data, ok, err := results[0].GetBytes(key)
		if err != nil {
			return nil, "", err
		}
		if ok && len(data) > 0 {
			contentType := http.DetectContentType(data)
			if contentType == "application/octet-stream" {
				contentType = "image/jpeg"
			}
			return data, contentType, nil
		}
	}

	return nil, "", fmt.Errorf("no embedded preview found for %s", path)
}

var ugosThumbnailSentinelPaths = []string{
	"/usr/ugreen",
	"/ugreen",
	"/etc/sysconfig/thumb_core.sh",
}

var ugosThumbnailCandidates = []string{
	"_640_40.webp",
	"_640_40.jpg",
	"_320_40.webp",
	"_320_40.jpg",
	"_mini.webp",
	"_mini.jpg",
	"_1600_40.webp",
	"_1600_40.jpg",
}

func detectUGOSThumbnailSystem() bool {
	for _, sentinel := range ugosThumbnailSentinelPaths {
		if _, err := os.Stat(sentinel); err == nil {
			return true
		}
	}
	return false
}

func defaultThumbnailPrewarmWorkers() int {
	workers := runtime.NumCPU()
	switch {
	case workers < 4:
		return 4
	case workers > 16:
		return 16
	default:
		return workers
	}
}

func readXattrString(path string, name string) (string, error) {
	size, err := unix.Getxattr(path, name, nil)
	if err != nil {
		return "", err
	}
	if size == 0 {
		return "", nil
	}

	buf := make([]byte, size)
	n, err := unix.Getxattr(path, name, buf)
	if err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

func ugosThumbnailStem(thumbID string) string {
	stem, _, _ := strings.Cut(strings.TrimSpace(thumbID), "-")
	return stem
}

func browserRenderableFile(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	_, ok := browserRenderableContentType(path)
	return ok
}

func (ws *WebServer) cachedThumbnailPathFor(path string) string {
	cleanPath := filepath.Clean(path)
	if cached, ok := ws.thumbnailCache.Load(cleanPath); ok {
		return cached.(string)
	}

	resolved := ws.resolveThumbnailPath(cleanPath)
	ws.thumbnailCache.Store(cleanPath, resolved)
	return resolved
}

func (ws *WebServer) resolveThumbnailPath(path string) string {
	if ws.ugosThumbnailMode {
		return ws.resolveUGOSThumbnailPath(path)
	}
	return ws.resolveLegacyThumbnailPath(path)
}

func (ws *WebServer) resolveUGOSThumbnailPath(path string) string {
	if ws.xattrForPath == nil {
		return ""
	}

	thumbnailDir, err := ws.xattrForPath(path, "user.thumb.dir")
	if err != nil || strings.TrimSpace(thumbnailDir) == "" {
		return ""
	}

	thumbnailID, err := ws.xattrForPath(path, "user.thumb.id")
	if err != nil {
		return ""
	}
	stem := ugosThumbnailStem(thumbnailID)
	if stem == "" {
		return ""
	}

	for _, suffix := range ugosThumbnailCandidates {
		candidatePath := filepath.Join(thumbnailDir, stem+suffix)
		if browserRenderableFile(candidatePath) {
			return candidatePath
		}
	}

	return ""
}

func (ws *WebServer) resolveLegacyThumbnailPath(path string) string {
	if ws.thumbnailForPath == nil || len(ws.thumbnailCandidates) == 0 {
		return ""
	}

	thumbnailBasePath := ws.thumbnailForPath(path)
	if thumbnailBasePath == "" {
		return ""
	}

	for _, suffix := range ws.thumbnailCandidates {
		candidatePath := thumbnailBasePath + suffix
		if browserRenderableFile(candidatePath) {
			return candidatePath
		}
	}

	return ""
}

func (ws *WebServer) prewarmThumbnailPaths(paths []string) {
	if ws.thumbnailPathFor == nil {
		return
	}

	uniquePaths := dedupePaths(paths)
	if len(uniquePaths) == 0 {
		return
	}

	workerCount := ws.prewarmWorkers
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(uniquePaths) {
		workerCount = len(uniquePaths)
	}

	jobs := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				ws.thumbnailPathFor(path)
			}
		}()
	}

	for _, path := range uniquePaths {
		jobs <- path
	}
	close(jobs)
	wg.Wait()
}

func synologyVolumeRoot(path string) (string, bool) {
	cleanPath := filepath.Clean(path)
	if !filepath.IsAbs(cleanPath) {
		return "", false
	}

	trimmed := strings.TrimPrefix(cleanPath, string(os.PathSeparator))
	parts := strings.Split(trimmed, string(os.PathSeparator))
	if len(parts) == 0 {
		return "", false
	}

	volumeName := parts[0]
	if !strings.HasPrefix(volumeName, "volume") {
		return "", false
	}
	if _, err := strconv.Atoi(strings.TrimPrefix(volumeName, "volume")); err != nil {
		return "", false
	}

	return filepath.Join(string(os.PathSeparator), volumeName), true
}

func synologyThumbnailBasePathFor(path string) string {
	volumeRoot, ok := synologyVolumeRoot(path)
	if !ok {
		return ""
	}

	digest := md5.Sum([]byte(filepath.Clean(path)))
	hashHex := hex.EncodeToString(digest[:])
	if len(hashHex) < 4 {
		return ""
	}

	return filepath.Join(volumeRoot, "@thumbnail", hashHex[:2], hashHex[2:4], hashHex)
}

// Start API server on the given host and port.
func (ws *WebServer) Start(host string, port int, db *sql.DB) error {
	if err := validateListenHost(host); err != nil {
		return err
	}

	ws.db = db
	mux := http.NewServeMux()

	// Static files
	mux.Handle("/", http.FileServer(http.FS(staticFS)))

	// API Endpoints
	mux.HandleFunc("/api/duplicates", ws.handleGetDuplicates)
	mux.HandleFunc("/api/resolve", ws.handleResolveGroup)
	mux.HandleFunc("/image", ws.handleImageServe)

	addr := listenAddr(host, port)
	log.Printf("Web UI for Deduplication is running at: http://%s/static/", addr)
	return http.ListenAndServe(addr, mux)
}

func (ws *WebServer) handleGetDuplicates(w http.ResponseWriter, r *http.Request) {
	pageStr := r.URL.Query().Get("page")
	limitStr := r.URL.Query().Get("limit")

	page := 1
	if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
		page = p
	}
	limit := 50
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 200 {
		limit = l
	}

	var total int
	err := ws.db.QueryRow(`
		SELECT COUNT(*)
		FROM file_cache
		WHERE thumbnails IS NOT NULL AND thumbnails != '' AND thumbnails != '[]'
	`).Scan(&total)
	if err != nil {
		http.Error(w, "Failed to query database", http.StatusInternalServerError)
		return
	}

	totalPages := 0
	if total > 0 {
		totalPages = (total + limit - 1) / limit
		if page > totalPages {
			page = totalPages
		}
	} else {
		page = 1
	}
	offset := (page - 1) * limit

	rows, err := ws.db.Query(`
		SELECT target_path, size, metadata, thumbnails
		FROM file_cache
		WHERE thumbnails IS NOT NULL AND thumbnails != '' AND thumbnails != '[]'
		ORDER BY target_path
		LIMIT ? OFFSET ?
	`, limit, offset)
	if err != nil {
		http.Error(w, "Failed to query database", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var groups []DuplicateGroup
	prewarmPaths := make([]string, 0, limit*2)

	type jsonMeta struct {
		Width      int    `json:"width"`
		Height     int    `json:"height"`
		Size       int64  `json:"size"`
		CreateTime string `json:"create_time"`
	}

	for rows.Next() {
		var masterPath, metadataStr, thumbnailsStr string
		var masterSize int64
		if err := rows.Scan(&masterPath, &masterSize, &metadataStr, &thumbnailsStr); err != nil {
			log.Printf("Failed to scan group: %v", err)
			continue
		}

		var masterMeta jsonMeta
		if metadataStr != "" && metadataStr != "{}" {
			json.Unmarshal([]byte(metadataStr), &masterMeta)
		}

		finalMasterSize := masterSize
		if masterMeta.Size > 0 {
			finalMasterSize = masterMeta.Size
		}

		group := DuplicateGroup{
			Master: ImageInfo{
				Path:       masterPath,
				Size:       finalMasterSize,
				Width:      masterMeta.Width,
				Height:     masterMeta.Height,
				CreateTime: masterMeta.CreateTime,
				IsMaster:   true,
			},
			Duplicates: []ImageInfo{},
		}
		if absPath, err := ws.resolveWithinDest(masterPath); err == nil {
			prewarmPaths = append(prewarmPaths, absPath)
		}

		type thumbObj struct {
			Path     string   `json:"path"`
			Metadata jsonMeta `json:"metadata"`
		}
		var thumbs []thumbObj
		if err := json.Unmarshal([]byte(thumbnailsStr), &thumbs); err == nil {
			for _, thumb := range thumbs {
				if thumb.Path == "" {
					continue
				}

				group.Duplicates = append(group.Duplicates, ImageInfo{
					Path:       thumb.Path,
					Size:       thumb.Metadata.Size,
					Width:      thumb.Metadata.Width,
					Height:     thumb.Metadata.Height,
					CreateTime: thumb.Metadata.CreateTime,
					IsMaster:   false,
				})
				if absPath, err := ws.resolveWithinDest(thumb.Path); err == nil {
					prewarmPaths = append(prewarmPaths, absPath)
				}
			}
		}

		groups = append(groups, group)
	}

	ws.prewarmThumbnailPaths(prewarmPaths)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"groups":     groups,
		"page":       page,
		"limit":      limit,
		"total":      total,
		"totalPages": totalPages,
	})
}

func (ws *WebServer) handleResolveGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		KeepPath    string   `json:"keepPath"`
		KeepPaths   []string `json:"keepPaths"`
		DeletePaths []string `json:"deletePaths"`
		MasterPath  string   `json:"masterPath"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	masterAbs, err := ws.resolveWithinDest(req.MasterPath)
	if err != nil {
		http.Error(w, "Invalid masterPath", http.StatusBadRequest)
		return
	}

	keepRawPaths := dedupePaths(req.KeepPaths)
	if len(keepRawPaths) == 0 && strings.TrimSpace(req.KeepPath) != "" {
		keepRawPaths = []string{strings.TrimSpace(req.KeepPath)}
	}
	if len(keepRawPaths) == 0 {
		http.Error(w, "At least one keep path is required", http.StatusBadRequest)
		return
	}

	members, err := ws.loadGroupMembers(req.MasterPath, masterAbs)
	if err == sql.ErrNoRows {
		http.Error(w, "Master group not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "Failed to load group", http.StatusInternalServerError)
		return
	}

	memberByAbs := make(map[string]groupMember, len(members))
	for _, member := range members {
		memberByAbs[filepath.Clean(member.abs)] = member
	}

	keepSet := make(map[string]bool, len(keepRawPaths))
	keepMembers := make([]groupMember, 0, len(keepRawPaths))
	keepMaster := false
	for _, keepRaw := range keepRawPaths {
		keepAbs, err := ws.resolveWithinDest(keepRaw)
		if err != nil {
			http.Error(w, "Invalid keepPaths entry", http.StatusBadRequest)
			return
		}

		key := filepath.Clean(keepAbs)
		member, ok := memberByAbs[key]
		if !ok {
			http.Error(w, "keepPaths must belong to the selected group", http.StatusBadRequest)
			return
		}
		if keepSet[key] {
			continue
		}
		keepSet[key] = true
		if member.isMaster {
			keepMaster = true
		}
		keepMembers = append(keepMembers, member)
	}

	deleteMembers := make([]groupMember, 0, len(members))
	for _, member := range members {
		if keepSet[filepath.Clean(member.abs)] {
			continue
		}
		deleteMembers = append(deleteMembers, member)
	}

	promoteSingle := !keepMaster && len(keepMembers) == 1

	tx, err := ws.db.Begin()
	if err != nil {
		http.Error(w, "Failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	for _, member := range deleteMembers {
		if err := os.Remove(member.abs); err != nil && !os.IsNotExist(err) {
			log.Printf("Warning: failed to delete file %s: %v", member.abs, err)
		}
	}

	switch {
	case keepMaster:
		_, err = tx.Exec(`UPDATE file_cache SET thumbnails = '[]' WHERE target_path = ?`, req.MasterPath)
		if err != nil {
			http.Error(w, "Failed to clear thumbnails", http.StatusInternalServerError)
			return
		}
	case !promoteSingle:
		_, err = tx.Exec(`DELETE FROM file_cache WHERE target_path = ?`, req.MasterPath)
		if err != nil {
			http.Error(w, "Failed to delete master row", http.StatusInternalServerError)
			return
		}
	}

	renames := make([]renameRecord, 0, len(keepMembers))
	standaloneEntries := make([]resolvedStandaloneEntry, 0, len(keepMembers))
	var promotedEntry *resolvedStandaloneEntry

	for _, member := range keepMembers {
		if member.isMaster {
			continue
		}

		finalAbs := member.abs
		storedPath := member.raw
		if promoteSingle {
			if filepath.Clean(member.abs) != filepath.Clean(masterAbs) {
				if err := os.Rename(member.abs, masterAbs); err != nil {
					rollbackRenames(renames)
					http.Error(w, "Failed to promote kept file", http.StatusInternalServerError)
					return
				}
				renames = append(renames, renameRecord{currentPath: masterAbs, originalPath: member.abs})
			}
			finalAbs = masterAbs
			storedPath = req.MasterPath
		} else if ws.isThumbnailAbs(member.abs) {
			finalAbs, err = ws.restoreThumbnailDestination(member.abs)
			if err != nil {
				rollbackRenames(renames)
				http.Error(w, "Failed to restore kept thumbnail", http.StatusInternalServerError)
				return
			}
			if err := os.Rename(member.abs, finalAbs); err != nil {
				rollbackRenames(renames)
				http.Error(w, "Failed to restore kept thumbnail", http.StatusInternalServerError)
				return
			}
			renames = append(renames, renameRecord{currentPath: finalAbs, originalPath: member.abs})
			storedPath, err = ws.storagePathForResolved(finalAbs)
			if err != nil {
				rollbackRenames(renames)
				http.Error(w, "Failed to resolve kept file path", http.StatusInternalServerError)
				return
			}
		} else {
			storedPath, err = ws.storagePathForResolved(finalAbs)
			if err != nil {
				rollbackRenames(renames)
				http.Error(w, "Failed to resolve kept file path", http.StatusInternalServerError)
				return
			}
		}

		stat, err := os.Stat(finalAbs)
		if err != nil {
			rollbackRenames(renames)
			http.Error(w, "Failed to stat kept file", http.StatusInternalServerError)
			return
		}

		meta := metadata.ExtractImageMetaJson(finalAbs)
		hash, phash, hasPHash, err := hashFileForCache(finalAbs)
		if err != nil {
			rollbackRenames(renames)
			http.Error(w, "Failed to hash kept file", http.StatusInternalServerError)
			return
		}

		entry := resolvedStandaloneEntry{
			storedPath: storedPath,
			hash:       hash,
			phash:      phash,
			hasPHash:   hasPHash,
			size:       stat.Size(),
			metadata:   meta,
		}

		phashStr := ""
		if hasPHash {
			phashStr = hasher.PHashToString(phash)
		}
		_, err = tx.Exec(`INSERT OR REPLACE INTO file_cache (target_path, mmh3_hash, phash, size, metadata, thumbnails) VALUES (?, ?, ?, ?, ?, '[]')`,
			entry.storedPath, entry.hash, phashStr, entry.size, entry.metadata)
		if err != nil {
			rollbackRenames(renames)
			http.Error(w, "Failed to keep selected file", http.StatusInternalServerError)
			return
		}

		if promoteSingle {
			copyEntry := entry
			promotedEntry = &copyEntry
		} else {
			standaloneEntries = append(standaloneEntries, entry)
		}
	}

	if err := tx.Commit(); err != nil {
		rollbackRenames(renames)
		http.Error(w, "Failed to commit resolution", http.StatusInternalServerError)
		return
	}

	if ws.cm != nil {
		if promoteSingle {
			ws.cm.DeleteEntryMemory(req.MasterPath)
			if promotedEntry != nil {
				ws.cm.SetEntryMemoryWithPresence(promotedEntry.storedPath, promotedEntry.hash, promotedEntry.phash, promotedEntry.hasPHash, promotedEntry.size, promotedEntry.metadata)
			}
		} else {
			if !keepMaster {
				ws.cm.DeleteEntryMemory(req.MasterPath)
			}
			for _, entry := range standaloneEntries {
				ws.cm.SetEntryMemoryWithPresence(entry.storedPath, entry.hash, entry.phash, entry.hasPHash, entry.size, entry.metadata)
			}
		}
	}

	dirsToClean := make(map[string]struct{})
	for _, member := range deleteMembers {
		dirsToClean[filepath.Dir(member.abs)] = struct{}{}
	}
	for _, rename := range renames {
		dirsToClean[filepath.Dir(rename.originalPath)] = struct{}{}
	}
	for dir := range dirsToClean {
		if err := fsutil.RemoveEmptyParentDirs(dir, ws.destDir); err != nil {
			log.Printf("Failed to remove empty directory for %s: %v", dir, err)
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (ws *WebServer) handleImageServe(w http.ResponseWriter, r *http.Request) {
	fullPath, err := ws.resolveWithinDest(r.URL.Query().Get("path"))
	if err != nil {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	w.Header().Set("Cache-Control", "public, max-age=31536000")
	if _, err := os.Stat(fullPath); err != nil {
		http.NotFound(w, r)
		return
	}

	if ws.thumbnailPathFor != nil {
		if thumbnailPath := ws.thumbnailPathFor(fullPath); thumbnailPath != "" {
			http.ServeFile(w, r, thumbnailPath)
			return
		}
	}

	if _, ok := browserRenderableContentType(fullPath); ok {
		http.ServeFile(w, r, fullPath)
		return
	}

	if ws.previewForPath != nil {
		previewBytes, contentType, err := ws.previewForPath(fullPath)
		if err == nil && len(previewBytes) > 0 {
			if contentType == "" {
				contentType = http.DetectContentType(previewBytes)
			}
			if contentType == "application/octet-stream" {
				contentType = "image/jpeg"
			}
			w.Header().Set("Content-Type", contentType)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(previewBytes)
			return
		}
	}

	http.NotFound(w, r)
}
