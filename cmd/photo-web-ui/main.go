package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"path/filepath"

	_ "modernc.org/sqlite" // Ensure sqlite driver is loaded

	"github.com/linuxhenhao/photo_organize/internal/target"
	"github.com/linuxhenhao/photo_organize/internal/web"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var destDir string
	var serveHost string
	var servePort int

	flag.StringVar(&destDir, "dest", "", "Target directory containing deduplicated items and cache.db (Required)")
	flag.StringVar(&serveHost, "host", "127.0.0.1", "IP address to bind the web server to")
	flag.IntVar(&servePort, "port", 8080, "Port for the web server")

	flag.Parse()

	if destDir == "" {
		fmt.Println("Usage: photo-web-ui -dest <target_dir> [-host <host>] [-port <port>]")
		flag.PrintDefaults()
		log.Fatal("Target directory is required.")
	}

	dbPath := filepath.Join(destDir, "cache.db")
	sqliteDB, err := sql.Open("sqlite", dbPath+"?_busy_timeout=5000")
	if err != nil {
		log.Fatalf("Failed to open cache.db: %v", err)
	}
	defer sqliteDB.Close()

	cm, err := target.NewCacheManager(destDir, 10)
	if err != nil {
		log.Fatalf("Failed to initialize CacheManager for web server: %v", err)
	}
	defer cm.Close()

	server := web.NewWebServer(cm, destDir)
	fmt.Printf("Starting web server on %s:%d for destination %s\n", serveHost, servePort, destDir)
	if err := server.Start(serveHost, servePort, sqliteDB); err != nil {
		log.Fatalf("Web Server failed: %v", err)
	}
}
