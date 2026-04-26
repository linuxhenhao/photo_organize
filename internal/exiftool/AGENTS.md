# Module Guidelines

## Scope
`internal/exiftool` wraps the shared `exiftool` worker pool used by metadata extraction, MIME probing, and embedded preview extraction.

The rest of the repo should treat this as the single place where exiftool process lifecycle, query options, and binary extraction behavior live.

## Change Rules
Keep process lifecycle, concurrency, and command invocation details isolated here.
Avoid leaking shell-specific assumptions into callers.
Be conservative with pool shutdown and reuse behavior because scanner, metadata, and hasher paths depend on it under load.

## Testing
Cover pool startup, request handling, and cleanup in `pool_test.go`. Prefer deterministic tests over timing-sensitive assertions.
