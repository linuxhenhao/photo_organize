package web

import (
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/linuxhenhao/photo_organize/internal/hasher"
	"github.com/linuxhenhao/photo_organize/internal/metadata"
	"github.com/linuxhenhao/photo_organize/internal/target"
)

//go:embed static/*
var staticFS embed.FS

// WebServer handles the Web UI for duplicate resolution.
type WebServer struct {
	cm      *target.CacheManager
	db      *sql.DB
	destDir string
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
	return &WebServer{
		cm:      cm,
		destDir: destDir,
	}
}

func listenAddr(port int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
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

	candidate := requestPath
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(ws.destDir, requestPath)
	}

	absPath, err := filepath.Abs(candidate)
	if err != nil {
		return "", fmt.Errorf("failed to resolve path: %w", err)
	}

	absDest, err := filepath.Abs(ws.destDir)
	if err != nil {
		return "", fmt.Errorf("failed to resolve destination: %w", err)
	}
	if !pathWithinRoot(absDest, absPath) {
		return "", fmt.Errorf("path escapes destination")
	}

	return absPath, nil
}

func hashFileForCache(path string) (string, uint64, error) {
	mmh3, err := hasher.CalculateHash(path)
	if err != nil {
		return "", 0, err
	}

	phash, err := hasher.CalculatePHash(path)
	if err != nil {
		return mmh3, 0, nil
	}

	return mmh3, phash, nil
}

// Start API server on the given port
func (ws *WebServer) Start(port int, db *sql.DB) error {
	ws.db = db
	mux := http.NewServeMux()

	// Static files
	mux.Handle("/", http.FileServer(http.FS(staticFS)))

	// API Endpoints
	mux.HandleFunc("/api/duplicates", ws.handleGetDuplicates)
	mux.HandleFunc("/api/resolve", ws.handleResolveGroup)
	mux.HandleFunc("/image", ws.handleImageServe)

	addr := listenAddr(port)
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
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 100 {
		limit = l
	}

	offset := (page - 1) * limit

	// Query DB for groups. We ONLY query the database and deserialize JSON; no disk reads!
	rows, err := ws.db.Query(`
		SELECT target_path, size, metadata, thumbnails 
		FROM file_cache 
		WHERE thumbnails IS NOT NULL AND thumbnails != '' AND thumbnails != '[]'
		LIMIT ? OFFSET ?
	`, limit, offset)

	if err != nil {
		http.Error(w, "Failed to query database", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var groups []DuplicateGroup

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

		// 1. Build Master Info
		var masterMeta jsonMeta
		if metadataStr != "" && metadataStr != "{}" {
			json.Unmarshal([]byte(metadataStr), &masterMeta)
		}

		// Use cached size if available, fallback to DB column
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

		// 2. Build Thumbnails Info
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
			}
		}

		groups = append(groups, group)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"groups": groups,
		"page":   page,
		"limit":  limit,
	})
}

func (ws *WebServer) handleResolveGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		KeepPath    string   `json:"keepPath"`
		DeletePaths []string `json:"deletePaths"`
		MasterPath  string   `json:"masterPath"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	keepAbs, err := ws.resolveWithinDest(req.KeepPath)
	if err != nil {
		http.Error(w, "Invalid keepPath", http.StatusBadRequest)
		return
	}
	masterAbs, err := ws.resolveWithinDest(req.MasterPath)
	if err != nil {
		http.Error(w, "Invalid masterPath", http.StatusBadRequest)
		return
	}

	type deleteTarget struct {
		raw string
		abs string
	}
	deleteTargets := make([]deleteTarget, 0, len(req.DeletePaths))
	for _, delPath := range req.DeletePaths {
		delAbs, err := ws.resolveWithinDest(delPath)
		if err != nil {
			http.Error(w, "Invalid deletePaths entry", http.StatusBadRequest)
			return
		}
		if filepath.Clean(delAbs) == filepath.Clean(keepAbs) {
			http.Error(w, "keepPath cannot be deleted", http.StatusBadRequest)
			return
		}
		deleteTargets = append(deleteTargets, deleteTarget{raw: delPath, abs: delAbs})
	}

	keepMaster := filepath.Clean(keepAbs) == filepath.Clean(masterAbs)

	tx, err := ws.db.Begin()
	if err != nil {
		http.Error(w, "Failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	// 1. Delete physical files and DB cache references
	for _, delTarget := range deleteTargets {
		// Try to delete physical file
		if err := os.Remove(delTarget.abs); err != nil && !os.IsNotExist(err) {
			log.Printf("Warning: failed to delete file %s: %v", delTarget.abs, err)
		}

		// If the file was a master, delete its row from DB
		if filepath.Clean(delTarget.abs) == filepath.Clean(masterAbs) {
			_, err = tx.Exec(`DELETE FROM file_cache WHERE target_path = ?`, delTarget.raw)
			if err != nil {
				http.Error(w, "Failed to delete master row", http.StatusInternalServerError)
				return
			}
		}
	}

	// 2. Promote kept thumbnail to master if needed
	var updatedMasterMeta string
	var updatedMasterSize int64
	var updatedMasterHash string
	var updatedMasterPHash uint64
	if !keepMaster {
		if err := os.Rename(keepAbs, masterAbs); err != nil {
			http.Error(w, "Failed to promote kept file", http.StatusInternalServerError)
			return
		}

		stat, err := os.Stat(masterAbs)
		if err != nil {
			http.Error(w, "Failed to stat promoted file", http.StatusInternalServerError)
			return
		}

		updatedMasterSize = stat.Size()
		updatedMasterMeta = metadata.ExtractImageMetaJson(masterAbs)
		updatedMasterHash, updatedMasterPHash, err = hashFileForCache(masterAbs)
		if err != nil {
			http.Error(w, "Failed to hash promoted file", http.StatusInternalServerError)
			return
		}

		_, err = tx.Exec(`INSERT OR REPLACE INTO file_cache (target_path, mmh3_hash, phash, size, metadata, thumbnails) VALUES (?, ?, ?, ?, ?, '[]')`,
			req.MasterPath, updatedMasterHash, hasher.PHashToString(updatedMasterPHash), updatedMasterSize, updatedMasterMeta)
		if err != nil {
			http.Error(w, "Failed to promote thumbnail to master", http.StatusInternalServerError)
			return
		}
	} else {
		// If kept path is the original master, we just clear its thumbnails column
		_, err = tx.Exec(`UPDATE file_cache SET thumbnails = '[]' WHERE target_path = ?`, req.KeepPath)
		if err != nil {
			http.Error(w, "Failed to clear thumbnails", http.StatusInternalServerError)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "Failed to commit resolution", http.StatusInternalServerError)
		return
	}

	if !keepMaster {
		ws.cm.SetEntryMemory(req.MasterPath, updatedMasterHash, updatedMasterPHash, updatedMasterSize, updatedMasterMeta)
	}

	w.WriteHeader(http.StatusOK)
}

func (ws *WebServer) handleImageServe(w http.ResponseWriter, r *http.Request) {
	fullPath, err := ws.resolveWithinDest(r.URL.Query().Get("path"))
	if err != nil {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	w.Header().Set("Cache-Control", "public, max-age=31536000") // Cache heavily
	http.ServeFile(w, r, fullPath)
}
