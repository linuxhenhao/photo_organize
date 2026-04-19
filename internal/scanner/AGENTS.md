# Module Guidelines

## Scope
`internal/scanner` walks source directories, extracts metadata, and records scan results into the database.

## Change Rules
Keep scan throughput and fault tolerance in mind. Scanner behavior should tolerate mixed media directories and partial extraction failures without crashing the whole run. Leave metadata extraction details in `internal/metadata` and persistence details in `internal/db` where possible.

## Testing
Add targeted tests if scanner logic grows beyond the current surface area. Run integration coverage for changes that affect which files are discovered or stored.
