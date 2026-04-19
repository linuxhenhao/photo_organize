# Photo Organizer

## Overview
`photo_organize` is a high-performance Go-based command-line tool for organizing large collections of photos and videos. It scans source directories, extracts metadata into an SQLite database, and organizes files into a structured target directory with advanced deduplication capabilities, including **perceptual image matching**.

## Features
- **Parallel Scanning**: Uses a worker pool of 10 goroutines for concurrent metadata extraction.
- **Intelligent Metadata Extraction**: 
    - Prioritizes EXIF data (via `exiftool`) for accurate creation dates.
    - Fallback to filename date patterns and filesystem birth times.
- **Advanced Deduplication**:
    - **Binary Match**: Uses MMH3 hashing for exact file duplicates.
    - **Perceptual Match**: Uses **dHash** and a custom **BK-Tree** to identify visually similar images (e.g., thumbnails or different resolutions).
- **High-Performance Web UI Caching**: 
    - Full metadata and thumbnail relationships are cached in SQLite as native **JSON** objects.
    - Eliminates disk I/O when browsing duplicate groups in the Web UI.
- **Optimized Storage**: 
    - Database uses **WAL (Write-Ahead Logging)** mode for high-concurrency safety.
    - Transaction-based batch updates for performance.
    - Actor-pattern **CacheManager** for background persistence of target state using atomic SQLite JSON operations.
- **Structured Organization**: 
    - Imports files into a `[target_dir]/YYYY/MM/DD/` hierarchy.
    - Automatic conflict resolution with suffixing (e.g., `-1`, `-2`).

## Installation

### Prerequisites
- **Go**: 1.24 or higher.
- **Exiftool**: Must be installed and available in your system `PATH`.

### Building
```bash
go build -o photo-organizer ./cmd/photo-organizer
```

## Usage

### 1. Scan Source Directories
Scan your source folders to populate the metadata database.
```bash
./photo-organizer scan -db photos.db -src /path/to/source1,/path/to/source2
```
- `-db`: Path to the SQLite database (defaults to `photos.db`).
- `-src`: Comma-separated list of source directories.

### 2. Import into Target Directory
Copy files from the database to your organized photo gallery with deduplication.
```bash
./photo-organizer import -db photos.db -dest /path/to/organized_photos
```

### 3. Initialize Target Cache
Pre-index an existing organized directory to avoid re-calculating hashes during future imports.
```bash
./photo-organizer initcache -dest /path/to/organized_photos
```

### 4. Serve Web UI for Deduplication
Launch the interactive web interface to resolve visual duplicates.
```bash
./photo-organizer serve -dest /path/to/organized_photos -host 0.0.0.0 -port 8080
```
The server binds to `127.0.0.1` by default. Set `-host 0.0.0.0` or another address to listen on other interfaces.

## Development
- **Test Data**: Use `test_data/` for local experimentation.
- **Integration Tests**: Run `./integration_test.sh` to verify build and functionality.

## Database Schema
The tool uses two primary tables across its SQLite databases:
- `photos` (in `photo.db`): `source_path`, `size`, `create_time`, `mmh3_hash`, `phash`, `group_id`, `mime_type`.
- `file_cache` (in `cache.db`): `target_path`, `mmh3_hash`, `phash`, `size`, `metadata` (JSON), `thumbnails` (JSON Array).

## License
MIT
