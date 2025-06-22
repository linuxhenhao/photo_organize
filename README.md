# Photo Organizer

## Overview
A command-line tool that scans source directories into an SQLite database and organizes files into a target directory based on database metadata, supporting deduplication and structured organization.

## Features
- **Scan Command**: Traverse directories to collect file metadata (path, size, creation time) into SQLite.
  - Uses 10 goroutines for parallel processing.
  - Creation time priority: EXIF data > filename date patterns > file metadata.
  - Computes MMH3 hashes for files with the same size.
  - Assigns group IDs to files with identical hashes.
- **Import Command**: Copies files from the database to the target directory.
  - Deduplication: Only copies the first file per group ID.
  - Organization: Generates `[target_dir]/year/month/day/filename` path based on creation time.
  - Conflict resolution: Appends `-n` suffix (incrementing n) for different content with the same filename.
  - Progress tracking: Appends to `stats.txt` with forced flush every 200 entries.

## Installation
1. Install Go 1.18 or higher.
2. Install exiftool (for EXIF metadata extraction).
3. Clone the repository: `git clone https://github.com/your-repo/photo-organize.git`
4. Build: `go build -o photo-organizer`

## Usage
### Scan
`./photo-organizer scan -db photos.db -src /path/to/photos1,/path/to/photos2`
- `-db`: SQLite database path (default: photos.db)
- `-src`: Comma-separated source directories

### Import
`./photo-organizer import -db photos.db -dest /path/to/organized_photos`

### Initialize Cache (initcache)
`./photo-organizer initcache -dest /path/to/organized_photos`
- `-dest`: Path to the target directory (existing organized directory)
- Purpose: Pre-generate the MMH3 hash cache file `mmh3_hash_cache.txt` for the target directory, avoiding recalculating hashes for existing files during repeated imports. Improves efficiency of the `import` command for scenarios where the target directory already contains some files.
- `-db`: SQLite database path
- `-dest`: Target directory for organized photos

## Database Schema
| Column         | Type    | Description                          |
|----------------|---------|--------------------------------------|
| source_path    | TEXT    | Absolute source file path (primary key) |
| size           | INTEGER | File size in bytes                   |
| create_time    | TEXT    | Creation time in RFC3339 format      |
| mmh3_hash      | TEXT    | MMH3 hash (empty for unique sizes)   |
| group_id       | INTEGER | Group ID (0 for unique, >0 for duplicates) |

## License
MIT