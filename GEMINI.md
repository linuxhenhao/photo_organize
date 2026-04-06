# Photo Organizer Project Context

## Project Overview
`photo_organize` is a Go-based command-line tool for organizing large collections of photos and videos. It scans source directories, extracts metadata (primarily creation time and file size) into an SQLite database, and organizes files into a target directory structure.

### Key Features
- **Metadata Management:** Uses SQLite (`photo.db`) to track file paths, sizes, creation times, and MMH3 hashes.
- **Intelligent Creation Time Extraction:** Prioritizes EXIF data (via `exiftool`), falling back to filename date patterns (e.g., `YYYY-MM-DD`), and finally platform-specific filesystem birth times.
- **Deduplication:** Identifies duplicate files using file size and MMH3 hashing (`murmur3`). Only one representative from each group of duplicates is imported.
- **Structured Organization:** Imports files into a `[target_dir]/YYYY/MM/DD/` hierarchy.
- **Conflict Resolution:** Appends suffixes (e.g., `-1`, `-2`) to filenames if content differs but names collide.
- **Performance Optimization:** 
    - Concurrent scanning and importing using Go goroutines (default 10 workers).
    - CGO-free SQLite driver (`modernc.org/sqlite`).
    - Cache management (`mmh3_hash_cache.txt`) to avoid redundant hashing of already organized files.

## Project Structure
- `cmd/photo-organizer/main.go`: Entry point and CLI flag parsing.
- `internal/`: Core application logic partitioned into domain-specific packages:
    - `db/`: SQLite driver setup and migrations.
    - `hasher/`: MMH3/dHash calculations and BK-Tree index.
    - `importer/`: File movement and deduplication logic.
    - `metadata/`: EXIF and filesystem attribute extraction.
    - `scanner/`: Concurrent directory traversal.
    - `target/`: Target directory state and cache management.
- `init.sql`: Database schema definition.
- `go.mod`: Dependency management (Go 1.24+).
- `photo.db`: Default SQLite database for metadata.

## Building and Running

### Prerequisites
- **Go:** 1.24 or higher.
- **Exiftool:** Must be installed and available in the system `PATH` for EXIF metadata extraction.

### Building
```bash
go build -o photo-organizer
```

### Core Commands
- **Scan:** Scans source directories and populates the database.
  ```bash
  ./photo-organizer scan -db photos.db -src test_data/source1,test_data/source2
  ```
- **Import:** Copies files from the database to the target directory.
  ```bash
  ./photo-organizer import -db photos.db -dest test_data/organized_photos
  ```
- **Init Cache:** Pre-hashes existing files in a target directory to speed up future imports.
  ```bash
  ./photo-organizer initcache -dest test_data/organized_photos
  ```

## Development Conventions
- **Concurrency:** Uses a worker pool pattern for CPU and I/O intensive tasks (scanning, hashing, copying).
- **Error Handling:** Errors are logged with context; the tool often continues processing other files after a single file failure.
- **Database:** SQLite is used with performance optimizations like `PRAGMA synchronous = OFF` and `journal_mode = MEMORY` during batch operations.
- **Platform Support:** Explicit support for Linux, macOS/BSD, and Windows via build tags and syscalls.

## External Dependencies
- `modernc.org/sqlite`: Pure Go SQLite driver.
- `github.com/twmb/murmur3`: Fast non-cryptographic hashing.
- `exiftool`: External CLI tool for rich metadata extraction.
