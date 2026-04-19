#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
IMAGE="${DOCKER_BUILD_IMAGE:-photo-organize-bookworm-gocv:latest}"

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is not installed or not in PATH" >&2
    exit 1
fi

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "docker image not found: $IMAGE" >&2
    echo "Set DOCKER_BUILD_IMAGE or build/tag the image first." >&2
    exit 1
fi

docker run --rm \
    --user "$(id -u):$(id -g)" \
    --workdir /workspace \
    --volume "$ROOT_DIR:/workspace" \
    "$IMAGE" \
    bash -lc "./build.sh"
