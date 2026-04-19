# Module Guidelines

## Scope
`internal/target` manages target directory indexing, cache persistence, duplicate relocation into `thumbnails/`, and cleanup flows such as `cleangroups`.

## Change Rules
This package mutates both disk layout and `cache.db`, so consistency matters more than local elegance. Keep `CacheManager` as the coordination point for cache writes. For `initcache` and cleanup changes, preserve dry-run versus apply behavior and do not silently drop thumbnail relationships. Thumbnail JSON entries may carry optional cached identifiers such as `mmh3_hash`; read-only refreshes may backfill those fields but must not move files. Keep first-stage and second-stage thumbnail validation data separate: the broad `dHash` lookup lives in `file_cache.dhash`, while heavier derivative-confirmation features belong to the dedicated feature-cache path used by `cleangroups`.

## Testing
Update the matching `*_test.go` file for cache, cleanup, or concurrency changes. Run `./integration_test.sh` for any edit that can change target layout, thumbnail moves, or cache contents.
