#!/bin/bash

# photo-org-rs Integration Test Script
# Full lifecycle: Scan -> Import -> Initcache -> Serve

set -euo pipefail

# 1. Setup paths
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT="$SCRIPT_DIR"
BIN="$SCRIPT_DIR/target/debug/photo-org"
TEST_WORKSPACE="/tmp/photo-org-test"
SRC_ROOT="$PROJECT_ROOT/test_data"
SCAN_DB="$TEST_WORKSPACE/scan.db"
CATALOG_DB="$TEST_WORKSPACE/catalog.db"
DEST_DIR="$TEST_WORKSPACE/organized"

cleanup() {
    echo "=== Cleaning up ==="
    rm -rf "$TEST_WORKSPACE"
    # Kill any background serve process
    pkill -f "photo-org serve" || true
}

trap cleanup EXIT

echo "=== Preparing Workspace ==="
mkdir -p "$TEST_WORKSPACE"
mkdir -p "$DEST_DIR"

echo "=== Building photo-org-rs ==="
cd "$SCRIPT_DIR"
cargo build --bin photo-org

echo "=== Stage 1: SCAN ==="
"$BIN" scan --scan-db "$SCAN_DB" --src "$SRC_ROOT/source" --src "$SRC_ROOT/source1" --src "$SRC_ROOT/problematic_images"

# Verify Scan results
ROWS=$(sqlite3 "$SCAN_DB" "SELECT COUNT(*) FROM source_items;")
echo "Scanned $ROWS items into $SCAN_DB"
if [ "$ROWS" -lt 5 ]; then
    echo "FAILED: Scan database too small"
    exit 1
fi

echo "=== Stage 2: IMPORT ==="
"$BIN" import --db "$CATALOG_DB" --scan-db "$SCAN_DB" --dest "$DEST_DIR"

# Verify Import
if [ ! -f "$CATALOG_DB" ]; then
    echo "FAILED: Catalog DB not created"
    exit 1
fi
IMPORTED_COUNT=$(sqlite3 "$CATALOG_DB" "SELECT COUNT(*) FROM target_items;")
echo "Imported $IMPORTED_COUNT items"

# Verify groups created during import
GROUPED_COUNT=$(sqlite3 "$CATALOG_DB" "SELECT COUNT(*) FROM target_items WHERE group_id IS NOT NULL;")
echo "Items in visual groups: $GROUPED_COUNT"

echo "=== Stage 3: INITCACHE (Testing 3-Stage Pipeline & Resumption) ==="
# We delete the catalog DB but keep the files to simulate adopting an existing repo
rm "$CATALOG_DB"
echo "Catalog DB deleted. Re-running initcache..."

# Enable profiling to see the 3 stages in logs
export PHOTO_ORG_PROFILE_INITCACHE=1
"$BIN" initcache --db "$CATALOG_DB" --dest "$DEST_DIR"

# Verify Initcache results
ADOPTED_COUNT=$(sqlite3 "$CATALOG_DB" "SELECT COUNT(*) FROM target_items;")
COMPLETED_COUNT=$(sqlite3 "$CATALOG_DB" "SELECT COUNT(*) FROM target_items WHERE group_status = 'completed';")
echo "Adopted $ADOPTED_COUNT items, $COMPLETED_COUNT marked as completed"

if [ "$ADOPTED_COUNT" -ne "$IMPORTED_COUNT" ]; then
    echo "FAILED: Initcache count ($ADOPTED_COUNT) does not match original import count ($IMPORTED_COUNT)"
    exit 1
fi

if [ "$COMPLETED_COUNT" -ne "$ADOPTED_COUNT" ]; then
    echo "FAILED: Not all items marked as completed"
    exit 1
fi

echo "=== Stage 4: SERVE (API Verification) ==="
# Run serve in background
"$BIN" serve --db "$CATALOG_DB" --dest "$DEST_DIR" --port 9999 --host 127.0.0.1 &
SERVE_PID=$!

# Wait for server to start
sleep 2

echo "Testing /api/groups endpoint..."
RESPONSE=$(curl -s "http://127.0.0.1:9999/api/groups?page=1&limit=20")

# Check if JSON contains total_groups
if echo "$RESPONSE" | grep -q "total_groups"; then
    echo "SUCCESS: API responded with paged groups"
    echo "$RESPONSE" | python3 -m json.tool | head -n 20
else
    echo "FAILED: API response invalid"
    echo "Response: $RESPONSE"
    exit 1
fi

# Test image serving (just check status code)
FIRST_PATH=$(sqlite3 "$CATALOG_DB" "SELECT target_path FROM target_items LIMIT 1;")
ENCODED_PATH=$(python3 -c "import urllib.parse; print(urllib.parse.quote('''$FIRST_PATH'''))")
IMG_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:9999/image?path=$ENCODED_PATH&size=400")

if [ "$IMG_STATUS" -eq 200 ]; then
    echo "SUCCESS: Image serving endpoint returned 200 OK"
else
    echo "FAILED: Image serving returned $IMG_STATUS"
    exit 1
fi

echo "=== ALL INTEGRATION TESTS PASSED ==="
