# Module Guidelines

## Scope
`internal/web` serves the duplicate-resolution UI, static assets, preview/image endpoints, and duplicate-resolution APIs.

It reads target data through `target.CacheManager`, resolves paths relative to the destination root, and updates duplicate decisions through the same cache and database layer used by `initcache` and `import`.
Static assets live under `static/` and are served without a frontend framework.

## Change Rules
Keep request validation, path-safety checks, and cache/database updates explicit.
Do not weaken destination-root path protections when adding endpoints.

The backend should keep preview extraction, thumbnail lookup, and path resolution consistent with the target layout and on-disk thumbnail systems.
If you change resolve flows, preserve the existing logging and request sequencing so operators can correlate actions with a specific file or group.

## Testing
Update `server_test.go` for API, serving, preview, or path-resolution changes. Add UI asset changes conservatively and verify that backend responses remain compatible with the existing static frontend.
