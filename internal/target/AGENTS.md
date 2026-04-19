# Module Guidelines

## Scope
`internal/target` manages target directory indexing, cache persistence, duplicate relocation into `thumbnails/`, and cleanup flows such as `cleangroups`.

## Change Rules
This package mutates both disk layout and `cache.db`, so consistency matters more than local elegance. Keep `CacheManager` as the coordination point for cache writes. For `initcache` and cleanup changes, preserve dry-run versus apply behavior and do not silently drop thumbnail relationships.

## Testing
Update the matching `*_test.go` file for cache, cleanup, or concurrency changes. Run `./integration_test.sh` for any edit that can change target layout, thumbnail moves, or cache contents.
