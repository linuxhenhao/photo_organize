# Module Guidelines

## Scope
`internal/migrate` contains one-off and forward-compatible SQLite migrations (schema fixes, column renames, lightweight data normalization). It must not contain business logic.

## Change Rules
Migrations must be idempotent and safe to re-run. Always prefer in-place `ALTER TABLE ... RENAME COLUMN ...` when available, and provide a rebuild fallback for older SQLite engines. Any migration that changes an on-disk DB should provide a consistent backup option (CLI or operator workflow).

