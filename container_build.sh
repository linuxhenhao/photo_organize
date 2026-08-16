#!/usr/bin/env bash

set -euo pipefail

REPO_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
IMAGE="${DOCKER_RUST_BUILD_IMAGE:-photo-org-build:bookworm}"
CARGO_CACHE_ROOT="${CARGO_CACHE_ROOT:-$HOME/.cargo}"

select_engine() {
    if [[ -n "${CONTAINER_ENGINE:-}" ]]; then
        if ! command -v "$CONTAINER_ENGINE" >/dev/null 2>&1; then
            echo "CONTAINER_ENGINE=$CONTAINER_ENGINE is not installed or not in PATH" >&2
            exit 1
        fi
        printf '%s\n' "$CONTAINER_ENGINE"
        return
    fi
    if command -v podman >/dev/null 2>&1; then
        printf 'podman\n'
        return
    fi
    if command -v docker >/dev/null 2>&1; then
        printf 'docker\n'
        return
    fi
    echo "neither podman nor docker is installed or in PATH" >&2
    exit 1
}

ENGINE=$(select_engine)
echo "Using container engine: $ENGINE"

# Build the image if it doesn't exist
if ! "$ENGINE" image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "Building build image: $IMAGE..."
    "$ENGINE" build -t "$IMAGE" "$REPO_DIR"
fi

mkdir -p \
    "$CARGO_CACHE_ROOT/registry" \
    "$CARGO_CACHE_ROOT/git"

# Rootless podman already maps container root to the host user. Docker's
# --user $(id -u):$(id -g) would instead land on a subordinate UID and make
# bind-mounted Cargo caches unwritable.
RUN_USER_ARGS=()
if [[ "$ENGINE" != podman || "$(id -u)" -eq 0 ]]; then
    RUN_USER_ARGS+=(--user "$(id -u):$(id -g)")
fi

echo "Running release build in container..."
"$ENGINE" run --rm \
    "${RUN_USER_ARGS[@]}" \
    --workdir /workspace \
    -e "CARGO_HOME=/cargo-home" \
    -e "CARGO_NET_GIT_FETCH_WITH_CLI=true" \
    --volume "$REPO_DIR:/workspace" \
    --volume "$CARGO_CACHE_ROOT/registry:/cargo-home/registry" \
    --volume "$CARGO_CACHE_ROOT/git:/cargo-home/git" \
    "$IMAGE" \
    cargo build --release "$@"
