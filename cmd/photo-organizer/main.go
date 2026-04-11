package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite" // Ensure sqlite driver is loaded

	"github.com/linuxhenhao/photo_organize/internal/importer"
	"github.com/linuxhenhao/photo_organize/internal/scanner"
	"github.com/linuxhenhao/photo_organize/internal/target"
	"github.com/linuxhenhao/photo_organize/internal/web"
)

// stringArrayFlag implements flag.Value for parsing comma-separated strings
type stringArrayFlag []string

func (s *stringArrayFlag) String() string {
	return strings.Join(*s, ",")
}

func (s *stringArrayFlag) Set(value string) error {
	if value == "" {
		return errors.New("source directories cannot be empty")
	}
	*s = strings.Split(value, ",")
	return nil
}

func main() {
	var dbPath string  // SQLite database path
	var destDir string // Target directory for import

	scanCmd := flag.NewFlagSet("scan", flag.ExitOnError)
	scanCmd.StringVar(&dbPath, "db", "photos.db", "SQLite database path")
	var parsedSourceDirs stringArrayFlag
	scanCmd.Var(&parsedSourceDirs, "src", "Source directories, comma-separated (e.g., /path/to/photos1,/path/to/photos2)")

	importCmd := flag.NewFlagSet("import", flag.ExitOnError)
	importCmd.StringVar(&dbPath, "db", "photos.db", "SQLite database path")
	importCmd.StringVar(&destDir, "dest", "", "Target directory for import (e.g., /path/to/organized_photos)")

	initCacheCmd := flag.NewFlagSet("initcache", flag.ExitOnError)
	initCacheCmd.StringVar(&destDir, "dest", "", "Target directory for import (e.g., /path/to/organized_photos)")
	var moveDuplicates bool
	initCacheCmd.BoolVar(&moveDuplicates, "move-duplicates", false, "Move perceptual duplicates into thumbnails; default is read-only cache refresh")

	serveCmd := flag.NewFlagSet("serve", flag.ExitOnError)
	serveCmd.StringVar(&destDir, "dest", "", "Target directory containing deduplicated items and cache.db")
	var servePort int
	serveCmd.IntVar(&servePort, "port", 8080, "Port for the web server")

	if len(os.Args) < 2 {
		fmt.Println("Usage: photo-organizer <command> [options]")
		fmt.Println("Commands: scan, import, initcache, serve")
		fmt.Println("\nScan command options:")
		scanCmd.PrintDefaults()
		fmt.Println("\nImport command options:")
		importCmd.PrintDefaults()
		fmt.Println("\nInitCache command options:")
		initCacheCmd.PrintDefaults()
		fmt.Println("\nServe command options:")
		serveCmd.PrintDefaults()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "scan":
		scanCmd.Parse(os.Args[2:])
		if len(parsedSourceDirs) == 0 {
			log.Fatal("Scan command requires at least one source directory specified with -src.")
		}
		scanner.HandleScan(dbPath, parsedSourceDirs)
	case "import":
		importCmd.Parse(os.Args[2:])
		if destDir == "" {
			log.Fatal("Import command requires a target directory specified with -dest.")
		}
		importer.HandleImport(dbPath, destDir)
	case "initcache":
		initCacheCmd.Parse(os.Args[2:])
		if destDir == "" {
			log.Fatal("InitCache command requires a target directory specified with -dest.")
		}

		cacheManager, err := target.NewCacheManager(destDir, 100)
		if err != nil {
			log.Fatalf("Failed to initialize target directory cache: %v", err)
		}
		defer cacheManager.Close()
		target.InitTargetDirCacheWithOptions(destDir, cacheManager, target.InitCacheOptions{
			MoveDuplicates: moveDuplicates,
		})
	case "serve":
		serveCmd.Parse(os.Args[2:])
		if destDir == "" {
			log.Fatal("Serve command requires a target directory specified with -dest.")
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
		if err := server.Start(servePort, sqliteDB); err != nil {
			log.Fatalf("Web Server failed: %v", err)
		}
	default:
		log.Fatalf("Invalid command: %s\nUsage: photo-organizer <command> [options]", os.Args[1])
	}
}
