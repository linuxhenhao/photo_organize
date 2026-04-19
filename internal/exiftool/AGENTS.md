# Module Guidelines

## Scope
`internal/exiftool` wraps the shared `exiftool` worker pool used by metadata extraction and media probing.

## Change Rules
Keep process lifecycle, concurrency, and command invocation details isolated here. Avoid leaking shell-specific assumptions into callers. Be conservative with pool shutdown and reuse behavior because scanner and metadata paths depend on it under load.

## Testing
Cover pool startup, request handling, and cleanup in `pool_test.go`. Prefer deterministic tests over timing-sensitive assertions.
