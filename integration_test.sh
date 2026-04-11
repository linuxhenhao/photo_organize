#!/bin/bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TMP_DIR=$(mktemp -d /tmp/photo-organize-it-XXXXXX)
BIN="$TMP_DIR/photo-organizer"
TEST_DB="$TMP_DIR/test_photos.db"
READONLY_DEST="$TMP_DIR/readonly_dest"
MOVE_DEST="$TMP_DIR/move_target"
GENERATED_ROOT="$TMP_DIR/generated_sources"
REAL_THUMB_SOURCE="$GENERATED_ROOT/source_real_thumbs"
NON_PHOTO_SOURCE="$GENERATED_ROOT/source_non_photo"

cleanup() {
    rm -rf "$TMP_DIR"
}

record_files() {
    find "$1" -type f \
        ! -name 'cache.db' \
        ! -name 'cache.db-wal' \
        ! -name 'cache.db-shm' \
        | sort
}

show_tree() {
    if command -v tree >/dev/null 2>&1; then
        tree -a "$1"
    else
        find "$1" | sort
    fi
}

trap cleanup EXIT

echo "=== Building photo-organizer ==="
go build -o "$BIN" ./cmd/photo-organizer

echo "=== Generating real-image thumbnail fixtures ==="
go run ./scripts/gen_test_images/main.go -output-root "$GENERATED_ROOT" -generate-mock=false -generate-real-thumbs=true

echo "=== Generating non-photo fixtures ==="
mkdir -p "$NON_PHOTO_SOURCE"
printf 'photo-organize integration text fixture\n' >"$NON_PHOTO_SOURCE/2024-06-09_notes.txt"
printf 'not-a-real-mp4\n' >"$NON_PHOTO_SOURCE/2024-06-09_clip.mp4"
touch -t 202406091200 "$NON_PHOTO_SOURCE/2024-06-09_notes.txt" "$NON_PHOTO_SOURCE/2024-06-09_clip.mp4"

NON_PHOTO_COUNT=$(find "$NON_PHOTO_SOURCE" -type f | wc -l | tr -d ' ')
echo "Generated non-photo fixtures: $NON_PHOTO_COUNT"
show_tree "$NON_PHOTO_SOURCE"

if [ "$NON_PHOTO_COUNT" -lt 2 ]; then
    echo "ERROR: Non-photo fixtures were not generated"
    exit 1
fi

REAL_THUMB_COUNT=$(find "$REAL_THUMB_SOURCE" -type f | wc -l | tr -d ' ')
echo "Generated real-image thumbnails: $REAL_THUMB_COUNT"
show_tree "$REAL_THUMB_SOURCE"

if [ "$REAL_THUMB_COUNT" -eq 0 ]; then
    echo "ERROR: No real-image thumbnail fixtures were generated"
    exit 1
fi

echo "=== Running SCAN on ARW sources and generated thumbnails ==="
"$BIN" scan -db "$TEST_DB" -src "$ROOT_DIR/test_data/source,$ROOT_DIR/test_data/source1,$REAL_THUMB_SOURCE,$NON_PHOTO_SOURCE"

NON_PHOTO_SCANNED=$(sqlite3 "$TEST_DB" "SELECT count(*) FROM photos WHERE lower(source_path) LIKE '%.txt' OR lower(source_path) LIKE '%.mp4';")
NON_PHOTO_NOT_IMAGE=$(sqlite3 "$TEST_DB" "SELECT count(*) FROM photos WHERE (lower(source_path) LIKE '%.txt' OR lower(source_path) LIKE '%.mp4') AND phash = 'NOT_IMAGE';")

echo "Scanned non-photo rows: $NON_PHOTO_SCANNED"
echo "Scanned non-photo rows tagged NOT_IMAGE: $NON_PHOTO_NOT_IMAGE"

if [ "$NON_PHOTO_SCANNED" -lt 2 ]; then
    echo "ERROR: Scan did not record all non-photo fixtures"
    exit 1
fi

if [ "$NON_PHOTO_NOT_IMAGE" -lt 2 ]; then
    echo "ERROR: Non-photo fixtures were not tagged as NOT_IMAGE during scan"
    exit 1
fi

echo "=== Running IMPORT to readonly integration target ==="
"$BIN" import -db "$TEST_DB" -dest "$READONLY_DEST"

echo "=== Verifying imported target ==="
show_tree "$READONLY_DEST"

READONLY_CACHE_DB="$READONLY_DEST/cache.db"
META_COUNT=$(sqlite3 "$READONLY_CACHE_DB" "SELECT count(*) FROM file_cache WHERE metadata != '{}' AND metadata IS NOT NULL;")
THUMB_COUNT=$(sqlite3 "$READONLY_CACHE_DB" "SELECT count(*) FROM file_cache WHERE thumbnails != '[]' AND thumbnails IS NOT NULL;")
ARW_THUMB_COUNT=$(sqlite3 "$READONLY_CACHE_DB" "SELECT count(*) FROM file_cache WHERE lower(target_path) LIKE '%.arw' AND thumbnails != '[]' AND thumbnails IS NOT NULL;")
NON_PHOTO_CACHE_ROWS=$(sqlite3 "$READONLY_CACHE_DB" "SELECT count(*) FROM file_cache WHERE lower(target_path) LIKE '%.txt' OR lower(target_path) LIKE '%.mp4';")
NON_PHOTO_THUMB_ROWS=$(sqlite3 "$READONLY_CACHE_DB" "SELECT count(*) FROM file_cache WHERE (lower(target_path) LIKE '%.txt' OR lower(target_path) LIKE '%.mp4') AND thumbnails != '[]' AND thumbnails IS NOT NULL;")

echo "Entries with metadata after import: $META_COUNT"
echo "Entries with thumbnails after import: $THUMB_COUNT"
echo "ARW masters with thumbnails after import: $ARW_THUMB_COUNT"
echo "Imported non-photo rows: $NON_PHOTO_CACHE_ROWS"
echo "Imported non-photo rows with thumbnails: $NON_PHOTO_THUMB_ROWS"

if [ "$META_COUNT" -eq 0 ]; then
    echo "ERROR: No metadata found in readonly cache.db after import"
    exit 1
fi

if [ "$ARW_THUMB_COUNT" -eq 0 ]; then
    echo "ERROR: Import did not attach any generated thumbnails to ARW masters"
    exit 1
fi

if [ "$NON_PHOTO_CACHE_ROWS" -lt 2 ]; then
    echo "ERROR: Import did not preserve non-photo fixtures in cache.db"
    exit 1
fi

if [ "$NON_PHOTO_THUMB_ROWS" -ne 0 ]; then
    echo "ERROR: Import incorrectly attached thumbnails to non-photo fixtures"
    exit 1
fi

for file in \
    "$READONLY_DEST/2024/06/09/2024-06-09_notes.txt" \
    "$READONLY_DEST/2024/06/09/2024-06-09_clip.mp4"
do
    if [ ! -f "$file" ]; then
        echo "ERROR: Expected imported non-photo file missing: $file"
        exit 1
    fi
done

record_files "$READONLY_DEST" >"$TMP_DIR/before_readonly.txt"
rm -f "$READONLY_CACHE_DB" "$READONLY_CACHE_DB-wal" "$READONLY_CACHE_DB-shm"

echo "=== Running read-only INITCACHE on imported target ==="
"$BIN" initcache -dest "$READONLY_DEST"

record_files "$READONLY_DEST" >"$TMP_DIR/after_readonly.txt"
if ! diff -u "$TMP_DIR/before_readonly.txt" "$TMP_DIR/after_readonly.txt"; then
    echo "ERROR: Read-only initcache changed files on disk"
    exit 1
fi

THUMB_COUNT_FINAL=$(sqlite3 "$READONLY_CACHE_DB" "SELECT count(*) FROM file_cache WHERE thumbnails != '[]' AND thumbnails IS NOT NULL;")
ARW_THUMB_COUNT_FINAL=$(sqlite3 "$READONLY_CACHE_DB" "SELECT count(*) FROM file_cache WHERE lower(target_path) LIKE '%.arw' AND thumbnails != '[]' AND thumbnails IS NOT NULL;")
NON_PHOTO_THUMB_ROWS_FINAL=$(sqlite3 "$READONLY_CACHE_DB" "SELECT count(*) FROM file_cache WHERE (lower(target_path) LIKE '%.txt' OR lower(target_path) LIKE '%.mp4') AND thumbnails != '[]' AND thumbnails IS NOT NULL;")
echo "Entries with thumbnails after read-only initcache: $THUMB_COUNT_FINAL"
echo "ARW masters with thumbnails after read-only initcache: $ARW_THUMB_COUNT_FINAL"
echo "Non-photo rows with thumbnails after read-only initcache: $NON_PHOTO_THUMB_ROWS_FINAL"

if [ "$THUMB_COUNT_FINAL" -eq 0 ]; then
    echo "ERROR: No thumbnails linked in cache.db after read-only initcache"
    exit 1
fi

if [ "$ARW_THUMB_COUNT_FINAL" -eq 0 ]; then
    echo "ERROR: Read-only initcache did not preserve ARW thumbnail relationships"
    exit 1
fi

if [ "$NON_PHOTO_THUMB_ROWS_FINAL" -ne 0 ]; then
    echo "ERROR: Read-only initcache incorrectly attached thumbnails to non-photo fixtures"
    exit 1
fi

echo "=== Preparing move-duplicates integration target ==="
mkdir -p "$MOVE_DEST"
cp -R "$ROOT_DIR/test_data/source" "$MOVE_DEST/source"
cp -R "$ROOT_DIR/test_data/source1" "$MOVE_DEST/source1"
cp -R "$REAL_THUMB_SOURCE" "$MOVE_DEST/source_real_thumbs"
cp -R "$NON_PHOTO_SOURCE" "$MOVE_DEST/source_non_photo"

echo "=== Running duplicate-moving INITCACHE on copied target ==="
"$BIN" initcache -dest "$MOVE_DEST" -move-duplicates

echo "=== Verifying duplicate-moving target ==="
show_tree "$MOVE_DEST"

MOVE_CACHE_DB="$MOVE_DEST/cache.db"
MOVED_FILES=$(find "$MOVE_DEST/thumbnails" -type f 2>/dev/null | wc -l | tr -d ' ')
MASTER_ROWS_WITH_THUMBNAILS=$(sqlite3 "$MOVE_CACHE_DB" "SELECT count(*) FROM file_cache WHERE thumbnails != '[]' AND thumbnails IS NOT NULL;")
TOTAL_THUMBNAIL_ENTRIES=$(sqlite3 "$MOVE_CACHE_DB" "SELECT COALESCE(sum(json_array_length(thumbnails)), 0) FROM file_cache WHERE thumbnails != '[]' AND thumbnails IS NOT NULL;")
ARW_MASTERS_WITH_THUMBNAILS=$(sqlite3 "$MOVE_CACHE_DB" "SELECT count(*) FROM file_cache WHERE lower(target_path) LIKE '%.arw' AND thumbnails != '[]' AND thumbnails IS NOT NULL;")
NON_PHOTO_MOVE_ROWS=$(sqlite3 "$MOVE_CACHE_DB" "SELECT count(*) FROM file_cache WHERE lower(target_path) LIKE '%.txt' OR lower(target_path) LIKE '%.mp4';")
NON_PHOTO_MOVE_THUMB_ROWS=$(sqlite3 "$MOVE_CACHE_DB" "SELECT count(*) FROM file_cache WHERE (lower(target_path) LIKE '%.txt' OR lower(target_path) LIKE '%.mp4') AND thumbnails != '[]' AND thumbnails IS NOT NULL;")
NON_PHOTO_MOVED_FILES=$(find "$MOVE_DEST/thumbnails" -type f \( -name '*.txt' -o -name '*.mp4' \) 2>/dev/null | wc -l | tr -d ' ')

echo "Files moved into thumbnails/: $MOVED_FILES"
echo "Master rows with thumbnails after move-duplicates: $MASTER_ROWS_WITH_THUMBNAILS"
echo "Total thumbnail entries after move-duplicates: $TOTAL_THUMBNAIL_ENTRIES"
echo "ARW masters with thumbnails after move-duplicates: $ARW_MASTERS_WITH_THUMBNAILS"
echo "Non-photo rows after move-duplicates: $NON_PHOTO_MOVE_ROWS"
echo "Non-photo rows with thumbnails after move-duplicates: $NON_PHOTO_MOVE_THUMB_ROWS"
echo "Non-photo files moved into thumbnails/: $NON_PHOTO_MOVED_FILES"

if [ "$MOVED_FILES" -eq 0 ]; then
    echo "ERROR: move-duplicates did not move any files into thumbnails/"
    exit 1
fi

if [ "$MASTER_ROWS_WITH_THUMBNAILS" -eq 0 ]; then
    echo "ERROR: move-duplicates did not persist thumbnail links in cache.db"
    exit 1
fi

if [ "$TOTAL_THUMBNAIL_ENTRIES" -ne "$MOVED_FILES" ]; then
    echo "ERROR: thumbnail entry count ($TOTAL_THUMBNAIL_ENTRIES) does not match moved file count ($MOVED_FILES)"
    exit 1
fi

if [ "$ARW_MASTERS_WITH_THUMBNAILS" -eq 0 ]; then
    echo "ERROR: move-duplicates did not attach thumbnails to any ARW master"
    exit 1
fi

if [ "$NON_PHOTO_MOVE_ROWS" -lt 2 ]; then
    echo "ERROR: move-duplicates did not preserve non-photo fixtures in cache.db"
    exit 1
fi

if [ "$NON_PHOTO_MOVE_THUMB_ROWS" -ne 0 ]; then
    echo "ERROR: move-duplicates incorrectly attached thumbnails to non-photo fixtures"
    exit 1
fi

if [ "$NON_PHOTO_MOVED_FILES" -ne 0 ]; then
    echo "ERROR: move-duplicates moved non-photo fixtures into thumbnails/"
    exit 1
fi

for file in \
    "$MOVE_DEST/source_non_photo/2024-06-09_notes.txt" \
    "$MOVE_DEST/source_non_photo/2024-06-09_clip.mp4"
do
    if [ ! -f "$file" ]; then
        echo "ERROR: Expected non-photo source fixture missing after move-duplicates: $file"
        exit 1
    fi
done

echo "=== Integration Test Complete ==="
