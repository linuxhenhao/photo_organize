# Module Guidelines

## Scope
`internal/web` serves the duplicate-resolution UI, static assets, preview/image endpoints, and duplicate resolution APIs.

## Change Rules
Keep request validation, path-safety checks, and cache/database updates explicit. Do not weaken destination-root path protections when adding endpoints. Static assets under `static/` should remain framework-free unless the project intentionally changes direction.

## Testing
Update `server_test.go` for API, serving, or path-resolution changes. Add UI asset changes conservatively and verify that backend responses remain compatible with the existing static frontend.
