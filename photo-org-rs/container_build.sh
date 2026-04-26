#!/usr/bin/env bash

set -euo pipefail

CRATE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "$CRATE_DIR/.." && pwd)
IMAGE="${DOCKER_RUST_BUILD_IMAGE:-photo-organize-bookworm-gocv:latest}"
CARGO_CACHE_ROOT="${CARGO_CACHE_ROOT:-$HOME/.cargo}"

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is not installed or not in PATH" >&2
    exit 1
fi

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "docker image not found: $IMAGE" >&2
    echo "Set DOCKER_RUST_BUILD_IMAGE or build/tag the image first." >&2
    exit 1
fi

mkdir -p \
    "$CARGO_CACHE_ROOT/registry" \
    "$CARGO_CACHE_ROOT/git"

docker run --rm \
    --workdir /workspace/photo-org-rs \
    -e "CARGO_HOME=/cargo-home" \
    -e "CARGO_NET_GIT_FETCH_WITH_CLI=true" \
    -e "PATH=/root/.cargo/bin:/go/bin:/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
    --volume "$REPO_DIR:/workspace" \
    --volume "$CARGO_CACHE_ROOT/registry:/cargo-home/registry" \
    --volume "$CARGO_CACHE_ROOT/git:/cargo-home/git" \
    "$IMAGE" \
    cargo build --release "$@"
