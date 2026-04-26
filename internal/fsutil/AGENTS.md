# Module Guidelines

## Scope
`internal/fsutil` holds generic filesystem helpers that can be reused across packages without importing higher-level behavior.

Current helpers are intentionally small and side-effect aware. Keep path pruning, empty-directory cleanup, and similar utilities here only when they are reusable outside import or cleanup flows.

## Change Rules
Keep helpers independent from database, web, or target-cache concerns.
If a function is specific to thumbnail cleanup, importing, or a single command flow, it belongs in that package instead.

## Testing
Add focused tests alongside new helpers. Use temporary directories and assert on the final filesystem state rather than internal steps.
