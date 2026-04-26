# Module Guidelines

## Scope
`internal/db` owns SQLite schema setup, migrations, and low-level database helpers.

It currently initializes `photos.db`, enables WAL-oriented pragmas, and coordinates migration helpers from `internal/migrate`.
Keep SQL, schema constants, and migration behavior here instead of spreading them across callers.

## Change Rules
Preserve backward compatibility for existing `photos.db` and `cache.db` files.
Prefer additive migrations over destructive schema edits.

If you touch SQL used by multiple packages, keep query behavior stable and document any new columns or indexes in the calling code.

## Testing
Update `db_test.go` for schema or migration changes. Use temporary databases in tests and verify both fresh initialization and upgrade paths when behavior changes.
