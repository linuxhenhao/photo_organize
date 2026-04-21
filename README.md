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
    - **Perceptual Match**: Uses **dHash** and a custom **BK-Tree** to identify visually similar images.
    - **AI Verification**: Optional ORB feature matching (OpenCV) for deep verification of thumbnail/master relationships.
- **High-Performance Web UI**: 
    - Dedicated, lightweight Web UI binary (`photo-web-ui`) for duplicate resolution.
    - Full metadata and thumbnail relationships are cached in SQLite as native **JSON** objects.
- **Optimized Storage**: 
    - Database uses **WAL (Write-Ahead Logging)** mode for high-concurrency safety.
    - Transaction-based batch updates for performance.
- **Structured Organization**: 
    - Imports files into a `[target_dir]/YYYY/MM/DD/` hierarchy.
    - Automatic conflict resolution with suffixing (e.g., `-1`, `-2`).

## Installation

### Prerequisites
- **Go**: 1.24 or higher.
- **Exiftool**: Must be installed and available in your system `PATH`.
- **OpenCV (Optional)**: Required only if building `photo-organizer` with advanced ORB verification.

### Building
The project produces two main binaries:

1. **photo-organizer**: The core CLI for scanning, importing, and maintenance. Built with **OpenCV (gocv)** by default for advanced ORB verification.
   ```bash
   # Standard build (with OpenCV/ORB support)
   go build -tags gocv -o photo-organizer ./cmd/photo-organizer

   # Lightweight build (no OpenCV dependency)
   go build -o photo-organizer ./cmd/photo-organizer
   ```

2. **photo-web-ui**: The lightweight web interface for resolving duplicates. This binary has **no OpenCV dependency** and can run in any environment.
   ```bash
   go build -o photo-web-ui ./cmd/photo-web-ui
   ```

## Usage

### 1. Scan Source Directories
Scan your source folders to populate the metadata database.
```bash
./photo-organizer scan -db photos.db -src /path/to/source1,/path/to/source2
```

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
./photo-web-ui -dest /path/to/organized_photos -host 127.0.0.1 -port 8080
```
- `-dest`: The organized directory containing `cache.db`.
- `-host`: IP address to bind (default `127.0.0.1`).
- `-port`: Port for the web server (default `8080`).

## Development
- **Integration Tests**: Run `./integration_test.sh` to verify build and functionality.

## License
MIT
