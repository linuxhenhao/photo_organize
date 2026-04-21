#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUTPUT_DIR="$ROOT_DIR/output"

mkdir -p "$OUTPUT_DIR"

cd "$ROOT_DIR"

# Build core organizer with OpenCV support
echo "Building photo-organizer with gocv..."
go build -buildvcs=false -tags gocv -o "$OUTPUT_DIR/photo-organizer" ./cmd/photo-organizer
echo "Built $OUTPUT_DIR/photo-organizer"

# Build web UI (lightweight, no OpenCV dependency needed for web resolution)
echo "Building photo-web-ui..."
go build -buildvcs=false -o "$OUTPUT_DIR/photo-web-ui" ./cmd/photo-web-ui
echo "Built $OUTPUT_DIR/photo-web-ui"
