# Module Guidelines

## Scope
`internal/dedupe` decides whether two files form a directional derivative relationship and which item should be treated as the canonical master.

This package combines path policy, metadata comparison, first-stage dHash distance, color signature distance, and optional ORB verification. It is used by `import`, `initcache`, `cleangroups`, and the web UI when the code needs to rank or revalidate candidate thumbnail/master pairs.

## Change Rules
Keep this package pure comparison logic. Do not add file I/O, database writes, or cache mutation here.
Relationships are directional: `child -> parent` is not symmetric, and path policy is allowed to reject a pair before visual checks run.

If you change thresholds or master-preference rules, make the intent visible in code. The current flow is:
metadata gates first, then dHash distance, then color signature, then ORB confirmation.
Keep the first-stage `dHash` and the heavier second-stage features conceptually separate.

`CompareMasterPreference` should continue to prefer likely master paths, then RAW sources when dimensions are compatible, then larger images, then larger file size.

## Testing
Extend `thumbnail_test.go` with representative parent/child, master-preference, and non-match cases. Favor explicit fixtures that show why a relationship should or should not be accepted.
