package web

import (
	"archive/tar"
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
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linuxhenhao/photo_organize/internal/dedupe"
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
	cleanGroupsLogPath  string
	previewForPath      func(string) ([]byte, string, error)
	thumbnailPathFor    func(string) string
	thumbnailForPath    func(string) string
	thumbnailCandidates []string
	xattrForPath        func(string, string) (string, error)
	ugosThumbnailMode   bool
	prewarmWorkers      int
	thumbnailCache      sync.Map
	resolveDBWriteMu    sync.Mutex
	resolveDBWriteHook  func()
	resolveRequestSeq   atomic.Uint64
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
	Master            ImageInfo   `json:"master"`
	Duplicates        []ImageInfo `json:"duplicates"`
	PreferredKeepPath string      `json:"preferredKeepPath,omitempty"`
}

const maxDuplicatesPageSize = 2000

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

func (ws *WebServer) SetCleanGroupsLogPath(path string) {
	ws.cleanGroupsLogPath = strings.TrimSpace(path)
}

func listenAddr(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
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

	dhash, err := hasher.CalculateDHash(path)
	if err != nil {
		return mmh3, 0, false, nil
	}

	return mmh3, dhash, true, nil
}

type groupMember struct {
	raw      string
	abs      string
	isMaster bool
}

type groupArchiveMember struct {
	Path        string `json:"path"`
	ArchivePath string `json:"archivePath"`
	IsMaster    bool   `json:"isMaster"`
	Size        int64  `json:"size"`
}

type groupArchiveManifest struct {
	MasterPath  string               `json:"masterPath"`
	GeneratedAt string               `json:"generatedAt"`
	Members     []groupArchiveMember `json:"members"`
}

type renameRecord struct {
	currentPath  string
	originalPath string
}

type resolvedStandaloneEntry struct {
	storedPath string
	hash       string
	dhash      uint64
	hasDHash   bool
	size       int64
	metadata   string
}

func imageInfoMeta(info ImageInfo) metadata.MediaMeta {
	return metadata.MediaMeta{
		Width:      info.Width,
		Height:     info.Height,
		Size:       info.Size,
		CreateTime: info.CreateTime,
	}
}

func preferredKeepPathForGroup(group DuplicateGroup) string {
	best := group.Master
	bestMeta := imageInfoMeta(best)

	for _, candidate := range group.Duplicates {
		if dedupe.CompareMasterPreference(
			candidate.Path,
			imageInfoMeta(candidate),
			candidate.Size,
			best.Path,
			bestMeta,
			best.Size,
		) > 0 {
			best = candidate
			bestMeta = imageInfoMeta(candidate)
		}
	}

	return best.Path
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

func summarizePathsForLog(paths []string) string {
	if len(paths) == 0 {
		return "[]"
	}

	const maxPaths = 6
	if len(paths) <= maxPaths {
		return fmt.Sprintf("%q", paths)
	}

	return fmt.Sprintf("%q ... (%d total)", paths[:maxPaths], len(paths))
}

func rawPathsForMembers(members []groupMember) []string {
	paths := make([]string, 0, len(members))
	for _, member := range members {
		paths = append(paths, member.raw)
	}
	return paths
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

func safeArchiveBaseName(path string) string {
	base := strings.TrimSpace(strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)))
	if base == "" {
		base = "group"
	}
	replacer := strings.NewReplacer(
		" ", "-",
		"/", "-",
		"\\", "-",
		":", "-",
		";", "-",
		",", "-",
	)
	base = replacer.Replace(base)
	base = strings.Trim(base, "-.")
	if base == "" {
		base = "group"
	}
	return base
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

func (ws *WebServer) archivePathFor(absPath string) (string, error) {
	absDest, err := filepath.Abs(ws.destDir)
	if err != nil {
		return "", err
	}

	rel, err := filepath.Rel(absDest, absPath)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("path escapes destination")
	}

	return filepath.ToSlash(filepath.Clean(rel)), nil
}

type archiveFileEntry struct {
	sourcePath  string
	archivePath string
	stat        os.FileInfo
}

func (ws *WebServer) prepareGroupArchive(masterRaw string, members []groupMember, prefix string) (groupArchiveManifest, []archiveFileEntry, error) {
	manifest := groupArchiveManifest{
		MasterPath:  masterRaw,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Members:     make([]groupArchiveMember, 0, len(members)),
	}

	entries := make([]archiveFileEntry, 0, len(members))
	for _, member := range members {
		stat, err := os.Stat(member.abs)
		if err != nil {
			return groupArchiveManifest{}, nil, err
		}
		if stat.IsDir() {
			continue
		}

		archivePath, err := ws.archivePathFor(member.abs)
		if err != nil {
			return groupArchiveManifest{}, nil, err
		}
		if prefix != "" {
			archivePath = path.Join(prefix, archivePath)
		}

		entries = append(entries, archiveFileEntry{
			sourcePath:  member.abs,
			archivePath: archivePath,
			stat:        stat,
		})
		manifest.Members = append(manifest.Members, groupArchiveMember{
			Path:        member.raw,
			ArchivePath: archivePath,
			IsMaster:    member.isMaster,
			Size:        stat.Size(),
		})
	}

	return manifest, entries, nil
}

func writeTarBytes(tw *tar.Writer, archivePath string, data []byte) error {
	header := &tar.Header{
		Name:    archivePath,
		Mode:    0644,
		Size:    int64(len(data)),
		ModTime: time.Now(),
	}
	if err := tw.WriteHeader(header); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

func writeArchiveFiles(tw *tar.Writer, entries []archiveFileEntry) error {
	for _, entry := range entries {
		file, err := os.Open(entry.sourcePath)
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(entry.stat, "")
		if err != nil {
			file.Close()
			return err
		}
		header.Name = entry.archivePath
		if err := tw.WriteHeader(header); err != nil {
			file.Close()
			return err
		}
		if _, err := io.Copy(tw, file); err != nil {
			file.Close()
			return err
		}
		file.Close()
	}

	return nil
}

func (ws *WebServer) writeGroupArchiveToTar(tw *tar.Writer, masterRaw string, members []groupMember, prefix string) (groupArchiveManifest, error) {
	manifest, entries, err := ws.prepareGroupArchive(masterRaw, members, prefix)
	if err != nil {
		return groupArchiveManifest{}, err
	}

	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return groupArchiveManifest{}, err
	}

	manifestPath := "manifest.json"
	if prefix != "" {
		manifestPath = path.Join(prefix, "manifest.json")
	}
	if err := writeTarBytes(tw, manifestPath, manifestBytes); err != nil {
		return groupArchiveManifest{}, err
	}
	if err := writeArchiveFiles(tw, entries); err != nil {
		return groupArchiveManifest{}, err
	}

	return manifest, nil
}

func (ws *WebServer) streamGroupArchive(w io.Writer, masterRaw string, members []groupMember) error {
	tw := tar.NewWriter(w)
	defer tw.Close()

	_, err := ws.writeGroupArchiveToTar(tw, masterRaw, members, "")
	return err
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
	ws.db = db
	mux := http.NewServeMux()

	// Static files
	mux.Handle("/", http.FileServer(http.FS(staticFS)))

	// API Endpoints
	mux.HandleFunc("/api/duplicates", ws.handleGetDuplicates)
	mux.HandleFunc("/api/group-archive", ws.handleGroupArchiveDownload)
	mux.HandleFunc("/api/cleangroups-log", ws.handleGetCleanGroupsLog)
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
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= maxDuplicatesPageSize {
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

		group.PreferredKeepPath = preferredKeepPathForGroup(group)

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

	resolveID := ws.resolveRequestSeq.Add(1)
	w.Header().Set("X-Resolve-Request-Id", strconv.FormatUint(resolveID, 10))

	var req struct {
		KeepPath    string   `json:"keepPath"`
		KeepPaths   []string `json:"keepPaths"`
		DeletePaths []string `json:"deletePaths"`
		MasterPath  string   `json:"masterPath"`
	}

	keepRawPaths := []string(nil)
	deleteRawPaths := []string(nil)

	failResolve := func(status int, publicMessage string, stage string, detail string, err error) {
		msg := fmt.Sprintf("[resolve %d] %s failed: remote=%q master=%q keep=%s delete=%s",
			resolveID,
			stage,
			r.RemoteAddr,
			req.MasterPath,
			summarizePathsForLog(keepRawPaths),
			summarizePathsForLog(deleteRawPaths),
		)
		if detail != "" {
			msg += " " + detail
		}
		if err != nil {
			msg += fmt.Sprintf(" err=%v", err)
		}
		log.Print(msg)
		http.Error(w, fmt.Sprintf("%s [resolve_id=%d]", publicMessage, resolveID), status)
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		failResolve(http.StatusBadRequest, "Invalid request body", "decode request body", "", err)
		return
	}

	deleteRawPaths = dedupePaths(req.DeletePaths)

	masterAbs, err := ws.resolveWithinDest(req.MasterPath)
	if err != nil {
		failResolve(http.StatusBadRequest, "Invalid masterPath", "resolve master path", "", err)
		return
	}

	keepRawPaths = dedupePaths(req.KeepPaths)
	if len(keepRawPaths) == 0 && strings.TrimSpace(req.KeepPath) != "" {
		keepRawPaths = []string{strings.TrimSpace(req.KeepPath)}
	}
	log.Printf("[resolve %d] start: remote=%q master=%q requested_keep=%s requested_delete=%s",
		resolveID,
		r.RemoteAddr,
		req.MasterPath,
		summarizePathsForLog(keepRawPaths),
		summarizePathsForLog(deleteRawPaths),
	)
	if len(keepRawPaths) == 0 {
		failResolve(http.StatusBadRequest, "At least one keep path is required", "validate keep paths", "", nil)
		return
	}

	members, err := ws.loadGroupMembers(req.MasterPath, masterAbs)
	if err == sql.ErrNoRows {
		failResolve(http.StatusNotFound, "Master group not found", "load group members", "", err)
		return
	}
	if err != nil {
		failResolve(http.StatusInternalServerError, "Failed to load group", "load group members", "", err)
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
			failResolve(http.StatusBadRequest, "Invalid keepPaths entry", "resolve keep path", fmt.Sprintf("keep=%q", keepRaw), err)
			return
		}

		key := filepath.Clean(keepAbs)
		member, ok := memberByAbs[key]
		if !ok {
			failResolve(http.StatusBadRequest, "keepPaths must belong to the selected group", "validate keep path membership", fmt.Sprintf("keep=%q", keepRaw), nil)
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
	log.Printf("[resolve %d] validated: members=%d keep=%d delete=%d keep_master=%t promote_single=%t validated_keep=%s computed_delete=%s",
		resolveID,
		len(members),
		len(keepMembers),
		len(deleteMembers),
		keepMaster,
		promoteSingle,
		summarizePathsForLog(rawPathsForMembers(keepMembers)),
		summarizePathsForLog(rawPathsForMembers(deleteMembers)),
	)
	if len(deleteRawPaths) > 0 && len(deleteRawPaths) != len(deleteMembers) {
		log.Printf("[resolve %d] delete count mismatch: requested=%d computed=%d requested_delete=%s computed_delete=%s",
			resolveID,
			len(deleteRawPaths),
			len(deleteMembers),
			summarizePathsForLog(deleteRawPaths),
			summarizePathsForLog(rawPathsForMembers(deleteMembers)),
		)
	}

	for _, member := range deleteMembers {
		if err := os.Remove(member.abs); err != nil && !os.IsNotExist(err) {
			log.Printf("[resolve %d] warning: failed to delete file %s: %v", resolveID, member.abs, err)
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
					failResolve(http.StatusInternalServerError, "Failed to promote kept file", "rename kept file to master", fmt.Sprintf("from=%q to=%q", member.raw, req.MasterPath), err)
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
				failResolve(http.StatusInternalServerError, "Failed to restore kept thumbnail", "resolve kept thumbnail destination", fmt.Sprintf("thumb=%q", member.raw), err)
				return
			}
			if err := os.Rename(member.abs, finalAbs); err != nil {
				rollbackRenames(renames)
				failResolve(http.StatusInternalServerError, "Failed to restore kept thumbnail", "rename kept thumbnail", fmt.Sprintf("from=%q to=%q", member.raw, finalAbs), err)
				return
			}
			renames = append(renames, renameRecord{currentPath: finalAbs, originalPath: member.abs})
			storedPath, err = ws.storagePathForResolved(finalAbs)
			if err != nil {
				rollbackRenames(renames)
				failResolve(http.StatusInternalServerError, "Failed to resolve kept file path", "resolve storage path for restored thumbnail", fmt.Sprintf("path=%q", finalAbs), err)
				return
			}
		} else {
			storedPath, err = ws.storagePathForResolved(finalAbs)
			if err != nil {
				rollbackRenames(renames)
				failResolve(http.StatusInternalServerError, "Failed to resolve kept file path", "resolve storage path", fmt.Sprintf("path=%q", finalAbs), err)
				return
			}
		}

		stat, err := os.Stat(finalAbs)
		if err != nil {
			rollbackRenames(renames)
			failResolve(http.StatusInternalServerError, "Failed to stat kept file", "stat kept file", fmt.Sprintf("path=%q", finalAbs), err)
			return
		}

		meta := metadata.ExtractImageMetaJson(finalAbs)
		hash, dhash, hasDHash, err := hashFileForCache(finalAbs)
		if err != nil {
			rollbackRenames(renames)
			failResolve(http.StatusInternalServerError, "Failed to hash kept file", "hash kept file", fmt.Sprintf("path=%q", finalAbs), err)
			return
		}

		entry := resolvedStandaloneEntry{
			storedPath: storedPath,
			hash:       hash,
			dhash:      dhash,
			hasDHash:   hasDHash,
			size:       stat.Size(),
			metadata:   meta,
		}

		if promoteSingle {
			copyEntry := entry
			promotedEntry = &copyEntry
		} else {
			standaloneEntries = append(standaloneEntries, entry)
		}
	}

	invokeResolveDBWriteHook := func() {
		if ws.resolveDBWriteHook != nil {
			ws.resolveDBWriteHook()
		}
	}

	ws.resolveDBWriteMu.Lock()
	defer ws.resolveDBWriteMu.Unlock()

	tx, err := ws.db.Begin()
	if err != nil {
		rollbackRenames(renames)
		failResolve(http.StatusInternalServerError, "Failed to begin transaction", "begin transaction", "", err)
		return
	}
	defer tx.Rollback()

	switch {
	case keepMaster:
		_, err = tx.Exec(`UPDATE file_cache SET thumbnails = '[]' WHERE target_path = ?`, req.MasterPath)
		if err != nil {
			rollbackRenames(renames)
			failResolve(http.StatusInternalServerError, "Failed to clear thumbnails", "update master thumbnails", "", err)
			return
		}
		invokeResolveDBWriteHook()
	case !promoteSingle:
		_, err = tx.Exec(`DELETE FROM file_cache WHERE target_path = ?`, req.MasterPath)
		if err != nil {
			rollbackRenames(renames)
			failResolve(http.StatusInternalServerError, "Failed to delete master row", "delete master row", "", err)
			return
		}
		invokeResolveDBWriteHook()
	}

	for _, entry := range standaloneEntries {
		phashStr := ""
		if entry.hasDHash {
			phashStr = hasher.DHashToString(entry.dhash)
		}
		_, err = tx.Exec(`INSERT OR REPLACE INTO file_cache (target_path, mmh3_hash, phash, size, metadata, thumbnails) VALUES (?, ?, ?, ?, ?, '[]')`,
			entry.storedPath, entry.hash, phashStr, entry.size, entry.metadata)
		if err != nil {
			rollbackRenames(renames)
			failResolve(http.StatusInternalServerError, "Failed to keep selected file", "upsert kept standalone file", fmt.Sprintf("path=%q", entry.storedPath), err)
			return
		}
		invokeResolveDBWriteHook()
	}

	if promoteSingle && promotedEntry != nil {
		phashStr := ""
		if promotedEntry.hasDHash {
			phashStr = hasher.DHashToString(promotedEntry.dhash)
		}
		_, err = tx.Exec(`INSERT OR REPLACE INTO file_cache (target_path, mmh3_hash, phash, size, metadata, thumbnails) VALUES (?, ?, ?, ?, ?, '[]')`,
			promotedEntry.storedPath, promotedEntry.hash, phashStr, promotedEntry.size, promotedEntry.metadata)
		if err != nil {
			rollbackRenames(renames)
			failResolve(http.StatusInternalServerError, "Failed to keep selected file", "upsert promoted file", fmt.Sprintf("path=%q", promotedEntry.storedPath), err)
			return
		}
		invokeResolveDBWriteHook()
	}

	if err := tx.Commit(); err != nil {
		rollbackRenames(renames)
		failResolve(http.StatusInternalServerError, "Failed to commit resolution", "commit transaction", "", err)
		return
	}

	if ws.cm != nil {
		if promoteSingle {
			ws.cm.DeleteEntryMemory(req.MasterPath)
			if promotedEntry != nil {
				ws.cm.SetEntryMemoryWithPresence(promotedEntry.storedPath, promotedEntry.hash, promotedEntry.dhash, promotedEntry.hasDHash, promotedEntry.size, promotedEntry.metadata)
			}
		} else {
			if !keepMaster {
				ws.cm.DeleteEntryMemory(req.MasterPath)
			}
			for _, entry := range standaloneEntries {
				ws.cm.SetEntryMemoryWithPresence(entry.storedPath, entry.hash, entry.dhash, entry.hasDHash, entry.size, entry.metadata)
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
			log.Printf("[resolve %d] failed to remove empty directory for %s: %v", resolveID, dir, err)
		}
	}

	log.Printf("[resolve %d] success: master=%q kept=%d deleted=%d renames=%d keep_master=%t promote_single=%t",
		resolveID,
		req.MasterPath,
		len(keepMembers),
		len(deleteMembers),
		len(renames),
		keepMaster,
		promoteSingle,
	)
	w.WriteHeader(http.StatusOK)
}

func (ws *WebServer) handleGroupArchiveDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	masterRaw := strings.TrimSpace(r.URL.Query().Get("masterPath"))
	if masterRaw == "" {
		http.Error(w, "masterPath is required", http.StatusBadRequest)
		return
	}

	masterAbs, err := ws.resolveWithinDest(masterRaw)
	if err != nil {
		http.Error(w, "Invalid masterPath", http.StatusBadRequest)
		return
	}

	members, err := ws.loadGroupMembers(masterRaw, masterAbs)
	if err == sql.ErrNoRows {
		http.Error(w, "Master group not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "Failed to load group", http.StatusInternalServerError)
		return
	}

	archiveDigest := md5.Sum([]byte(masterRaw))
	archiveName := fmt.Sprintf(
		"%s-%s.tar",
		safeArchiveBaseName(masterRaw),
		hex.EncodeToString(archiveDigest[:4]),
	)
	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", archiveName))

	if err := ws.streamGroupArchive(w, masterRaw, members); err != nil {
		log.Printf("Failed to stream group archive for %s: %v", masterRaw, err)
	}
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
