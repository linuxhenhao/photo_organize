# Module Guidelines

## Scope
`internal/migrate` contains one-off and forward-compatible SQLite migrations, schema fixes, column renames, backups, and lightweight data normalization.

The current callers use:
`MigratePhotosHashColumn` for `photos.db`,
`MigrateFileCacheHashColumn` for `cache.db`,
`BackupSQLiteDB` for pre-migration snapshots,
and `IntegrityCheck` for post-migration validation.

## Change Rules
Migrations must be idempotent and safe to re-run.
Always prefer in-place `ALTER TABLE ... RENAME COLUMN ...` when available, and provide a rebuild fallback for older SQLite engines.

Any migration that changes an on-disk DB should provide a consistent backup option through CLI or operator workflow.
Do not add business logic here.
