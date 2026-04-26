# Module Guidelines

## Scope
`internal/importer` reads scan results from `photos.db` and copies the canonical file for each group into the target tree.

It coordinates exact-match skipping, visual duplicate confirmation, name conflict resolution, and thumbnail routing under `thumbnails/`.
`importer.go` performs the worker-driven copy loop.
`coordinator.go` handles reservation, in-flight dedupe, and final cache updates.

## Change Rules
Preserve import idempotence and concurrent safety. Reservation logic in `coordinator.go` and copy behavior in `importer.go` must stay aligned.
Do not bypass the coordinator when adding new import paths, and keep cache updates consistent with on-disk results.

Visual duplicate confirmation should prefer the shared second-stage cache in `cache.db` via the precompute resolver.
Cache misses may compute-and-backfill features, but planning must not move files before a reservation is committed.

The coordinator decides whether a task is skipped, waits behind an in-flight reservation, copies as a master, or reroutes to a thumbnail destination. Keep those states explicit in code and logs.

## Testing
Extend `importer_test.go` for copy, rollback, in-flight coordination, and thumbnail-routing changes. Run the integration script for behavior that affects final target layout, duplicate handling, or thumbnail linking.
