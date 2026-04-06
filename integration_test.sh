#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "=== Building photo-organizer ==="
go build -o photo-organizer ./cmd/photo-organizer

echo "=== Generating mock test images and thumbnails ==="
go run scripts/gen_test_images/main.go

echo "=== Running SCAN on source, source1, repo, and mock folders ==="
# Remove previous db if exists to keep test clean
rm -f test_photos.db

./photo-organizer scan -db test_photos.db -src test_data/source,test_data/source1,test_data/source_mock,test_data/source_mock_thumbs

echo "=== Running IMPORT to integration_test_dest ==="
# Cleanup previous dest
rm -rf test_data/integration_test_dest

./photo-organizer import -db test_photos.db -dest test_data/integration_test_dest

echo "=== Verifying Target Directory ==="
echo "Contents of test_data/integration_test_dest:"
tree test_data/integration_test_dest || find test_data/integration_test_dest

echo "=== Verifying Cache Database Metadata and Thumbnails ==="
CACHE_DB="test_data/integration_test_dest/cache.db"
# Check if columns exist and have content
META_COUNT=$(sqlite3 "$CACHE_DB" "SELECT count(*) FROM file_cache WHERE metadata != '{}' AND metadata IS NOT NULL;")
THUMB_COUNT=$(sqlite3 "$CACHE_DB" "SELECT count(*) FROM file_cache WHERE thumbnails != '[]' AND thumbnails IS NOT NULL;")

echo "Entries with metadata: $META_COUNT"
echo "Entries with thumbnails: $THUMB_COUNT"

if [ "$META_COUNT" -eq 0 ]; then
    echo "ERROR: No metadata found in cache.db"
    exit 1
fi

echo "=== Running INITCACHE on integration_test_dest ==="
# Clear cache to force re-scan of destination and find duplicates
rm -f "$CACHE_DB"
# Testing the initcache capability that will also sweep for duplicates
./photo-organizer initcache -dest test_data/integration_test_dest

echo "=== Final Verification of Thumbnails ==="
THUMB_COUNT_FINAL=$(sqlite3 "$CACHE_DB" "SELECT count(*) FROM file_cache WHERE thumbnails != '[]' AND thumbnails IS NOT NULL;")
echo "Entries with thumbnails after initcache: $THUMB_COUNT_FINAL"

if [ "$THUMB_COUNT_FINAL" -eq 0 ]; then
    echo "ERROR: No thumbnails linked in cache.db after initcache"
    exit 1
fi

echo "=== Integration Test Complete ==="
