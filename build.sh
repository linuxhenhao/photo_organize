#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUTPUT_DIR="$ROOT_DIR/output"
OUTPUT_BIN="$OUTPUT_DIR/photo_organize"

mkdir -p "$OUTPUT_DIR"

cd "$ROOT_DIR"
go build -buildvcs=false -o "$OUTPUT_BIN" ./cmd/photo-organizer

echo "Built $OUTPUT_BIN"
