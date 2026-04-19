# Module Guidelines

## Scope
`internal/fsutil` holds generic filesystem helpers that can be reused across packages without importing higher-level behavior.

## Change Rules
Keep helpers small, side-effect aware, and independent from database or web concerns. If a function is specific to thumbnail cleanup or importing, it belongs in that package instead.

## Testing
Add focused tests alongside new helpers. Use temporary directories and assert on the final filesystem state rather than internal steps.
