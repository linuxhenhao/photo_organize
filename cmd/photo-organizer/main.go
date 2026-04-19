package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	_ "modernc.org/sqlite" // Ensure sqlite driver is loaded

	"github.com/linuxhenhao/photo_organize/internal/importer"
	"github.com/linuxhenhao/photo_organize/internal/precompute"
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
	log.SetFlags(log.LstdFlags | log.Lshortfile)

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
	var skipRebuild bool
	initCacheCmd.BoolVar(&moveDuplicates, "move-duplicates", false, "Move perceptual duplicates into thumbnails; default is read-only cache refresh")
	initCacheCmd.BoolVar(&skipRebuild, "skip-rebuild", false, "Skip rebuilding thumbnail links from thumbnails/ (can be very slow)")

	cleanGroupsCmd := flag.NewFlagSet("cleangroups", flag.ExitOnError)
	cleanGroupsCmd.StringVar(&destDir, "dest", "", "Target directory containing deduplicated items and cache.db")
	var applyCleanGroups bool
	cleanGroupsCmd.BoolVar(&applyCleanGroups, "apply", false, "Persist cleanup changes; default is dry-run")

	precomputeCmd := flag.NewFlagSet("precompute", flag.ExitOnError)
	precomputeCmd.StringVar(&destDir, "dest", "", "Target directory containing deduplicated items and cache.db")
	var precomputeWorkers int
	var precomputeForce bool
	precomputeCmd.IntVar(&precomputeWorkers, "workers", 0, "Worker count; default uses CPU core count")
	precomputeCmd.BoolVar(&precomputeForce, "force", false, "Recompute features even if cache entry exists")

	serveCmd := flag.NewFlagSet("serve", flag.ExitOnError)
	serveCmd.StringVar(&destDir, "dest", "", "Target directory containing deduplicated items and cache.db")
	var serveHost string
	var servePort int
	serveCmd.StringVar(&serveHost, "host", "127.0.0.1", "IP address to bind the web server to")
	serveCmd.IntVar(&servePort, "port", 8080, "Port for the web server")
	var cleanGroupsLogPath string
	serveCmd.StringVar(&cleanGroupsLogPath, "cleangroups-log", "", "Optional cleangroups log file for review UI; absolute paths must be provided here")

	if len(os.Args) < 2 {
		fmt.Println("Usage: photo-organizer <command> [options]")
		fmt.Println("Commands: scan, import, initcache, cleangroups, precompute, serve")
		fmt.Println("\nScan command options:")
		scanCmd.PrintDefaults()
		fmt.Println("\nImport command options:")
		importCmd.PrintDefaults()
		fmt.Println("\nInitCache command options:")
		initCacheCmd.PrintDefaults()
		fmt.Println("\nCleanGroups command options:")
		cleanGroupsCmd.PrintDefaults()
		fmt.Println("\nPrecompute command options:")
		precomputeCmd.PrintDefaults()
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

		ctx, stopSignals := newInitCacheContext()
		defer stopSignals()

		target.InitTargetDirCacheWithContext(ctx, destDir, cacheManager, target.InitCacheOptions{
			MoveDuplicates: moveDuplicates,
			SkipRebuild:    skipRebuild,
		})
	case "cleangroups":
		cleanGroupsCmd.Parse(os.Args[2:])
		if destDir == "" {
			log.Fatal("CleanGroups command requires a target directory specified with -dest.")
		}

		cacheManager, err := target.NewCacheManager(destDir, 100)
		if err != nil {
			log.Fatalf("Failed to initialize target directory cache: %v", err)
		}
		defer cacheManager.Close()

		ctx, stopSignals := newInitCacheContext()
		defer stopSignals()

		if _, err := target.CleanThumbnailGroupsWithContext(ctx, destDir, cacheManager, target.CleanGroupsOptions{
			Apply: applyCleanGroups,
		}); err != nil {
			log.Fatalf("CleanGroups failed: %v", err)
		}
	case "precompute":
		precomputeCmd.Parse(os.Args[2:])
		if destDir == "" {
			log.Fatal("Precompute command requires a target directory specified with -dest.")
		}

		dbPath := filepath.Join(destDir, "cache.db")
		sqliteDB, err := sql.Open("sqlite", dbPath+"?_busy_timeout=5000")
		if err != nil {
			log.Fatalf("Failed to open cache.db: %v", err)
		}
		defer sqliteDB.Close()

		_, _ = sqliteDB.Exec(`PRAGMA synchronous = OFF`)
		_, _ = sqliteDB.Exec(`PRAGMA journal_mode = WAL`)

		ctx, stopSignals := newInitCacheContext()
		defer stopSignals()

		if err := precompute.Run(ctx, destDir, sqliteDB, precompute.Options{
			Workers: precomputeWorkers,
			Force:   precomputeForce,
		}); err != nil {
			log.Fatalf("Precompute failed: %v", err)
		}
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
		server.SetCleanGroupsLogPath(cleanGroupsLogPath)
		if err := server.Start(serveHost, servePort, sqliteDB); err != nil {
			log.Fatalf("Web Server failed: %v", err)
		}
	default:
		log.Fatalf("Invalid command: %s\nUsage: photo-organizer <command> [options]", os.Args[1])
	}
}

func newInitCacheContext() (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 2)
	done := make(chan struct{})

	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	go func() {
		interrupts := 0
		for {
			select {
			case <-done:
				return
			case sig := <-signals:
				interrupts++
				if interrupts == 1 {
					log.Printf("Received %s, stopping initcache at the next safe point. Press Ctrl-C again to force exit.", sig)
					cancel()
					continue
				}
				log.Printf("Received %s again, forcing exit.", sig)
				os.Exit(130)
			}
		}
	}()

	return ctx, func() {
		close(done)
		signal.Stop(signals)
		cancel()
	}
}
