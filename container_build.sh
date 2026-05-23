#!/usr/bin/env bash

set -euo pipefail

REPO_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
IMAGE="${DOCKER_RUST_BUILD_IMAGE:-photo-org-build:bookworm}"
CARGO_CACHE_ROOT="${CARGO_CACHE_ROOT:-$HOME/.cargo}"

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is not installed or not in PATH" >&2
    exit 1
fi

# Build the image if it doesn't exist
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "Building build image: $IMAGE..."
    docker build -t "$IMAGE" "$REPO_DIR"
fi

mkdir -p \
    "$CARGO_CACHE_ROOT/registry" \
    "$CARGO_CACHE_ROOT/git"

echo "Running release build in container..."
docker run --rm \
    --user "$(id -u):$(id -g)" \
    --workdir /workspace \
    -e "CARGO_HOME=/cargo-home" \
    -e "CARGO_NET_GIT_FETCH_WITH_CLI=true" \
    --volume "$REPO_DIR:/workspace" \
    --volume "$CARGO_CACHE_ROOT/registry:/cargo-home/registry" \
    --volume "$CARGO_CACHE_ROOT/git:/cargo-home/git" \
    "$IMAGE" \
    cargo build --release "$@"
