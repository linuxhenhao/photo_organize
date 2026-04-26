# Module Guidelines

## Scope
`internal/scanner` walks source directories, extracts metadata, and records scan results into `photos.db`.

It inserts `source_path`, `size`, `create_time`, and `mime_type`, then invokes `internal/hasher` to fill `mmh3_hash`, first-stage `dhash`, and `group_id`.

## Change Rules
Keep scan throughput and fault tolerance in mind. Scanner behavior should tolerate mixed media directories and partial extraction failures without crashing the whole run.

Leave metadata extraction details in `internal/metadata` and persistence details in `internal/db` where possible.
The scanner should continue to skip already-seen paths rather than rewriting them on every run.

## Testing
Add targeted tests if scanner logic grows beyond the current surface area. Run integration coverage for changes that affect which files are discovered, inserted, or grouped.
