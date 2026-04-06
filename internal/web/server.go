package web

import (
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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

	addr := fmt.Sprintf(":%d", port)
	log.Printf("🚀 Web UI for Deduplication is running at: http://localhost%s/static/", addr)
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

	tx, err := ws.db.Begin()
	if err != nil {
		http.Error(w, "Failed to begin transaction", http.StatusInternalServerError)
		return
	}

	// 1. Delete physical files and DB cache references
	for _, delPath := range req.DeletePaths {
		fullDelPath := filepath.Join(ws.destDir, delPath)
		// Try to delete physical file
		if err := os.Remove(fullDelPath); err != nil && !os.IsNotExist(err) {
			log.Printf("Warning: failed to delete file %s: %v", fullDelPath, err)
		}
		
		// If the file was a master, delete its row from DB
		if delPath == req.MasterPath {
			_, err = tx.Exec(`DELETE FROM file_cache WHERE target_path = ?`, delPath)
			if err != nil {
				log.Printf("Warning: failed to delete master row %s: %v", delPath, err)
			}
			// In-memory cleanup
			ws.cm.DeleteEntry(delPath, "")
		}
	}

	// 2. Promote kept thumbnail to master if needed
	if req.KeepPath != req.MasterPath {
		// KeepPath is a thumbnail being promoted
		// Get old thumbnail info if possible, or just insert it as a new master
		stat, err := os.Stat(filepath.Join(ws.destDir, req.KeepPath))
		var size int64 = 0
		if err == nil {
			size = stat.Size()
		}
		
		masterMeta := metadata.ExtractImageMetaJson(filepath.Join(ws.destDir, req.KeepPath))

		// Insert the new master into the root level without any thumbnails
		_, err = tx.Exec(`INSERT OR REPLACE INTO file_cache (target_path, mmh3_hash, phash, size, metadata, thumbnails) VALUES (?, '', '', ?, ?, '[]')`, req.KeepPath, size, masterMeta)
		if err != nil {
			log.Printf("Failed to promote thumbnail to master: %v", err)
		}
		ws.cm.AddEntry(req.KeepPath, "", 0, size, masterMeta)
	} else {
		// If kept path is the original master, we just clear its thumbnails column
		_, err = tx.Exec(`UPDATE file_cache SET thumbnails = '[]' WHERE target_path = ?`, req.KeepPath)
		if err != nil {
			log.Printf("Failed to clear thumbnails for master %s: %v", req.KeepPath, err)
		}
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "Failed to commit resolution", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (ws *WebServer) handleImageServe(w http.ResponseWriter, r *http.Request) {
	relPath := r.URL.Query().Get("path")
	if relPath == "" || strings.Contains(relPath, "..") {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	fullPath := filepath.Join(ws.destDir, relPath)
	
	// Ensure the file stays inside destDir boundaries for security
	absPath, err := filepath.Abs(fullPath)
	if err != nil {
		http.Error(w, "Resolved path error", http.StatusBadRequest)
		return
	}
	absDest, _ := filepath.Abs(ws.destDir)
	if !strings.HasPrefix(absPath, absDest) {
		http.Error(w, "Access Denied", http.StatusForbidden)
		return
	}

	w.Header().Set("Cache-Control", "public, max-age=31536000") // Cache heavily
	http.ServeFile(w, r, absPath)
}
