# Module Guidelines

## Scope
`internal/importer` reads scanned metadata and copies files into the organized target tree while coordinating duplicate handling and thumbnail attachment.

## Change Rules
Preserve import idempotence and concurrent safety. Reservation logic in `coordinator.go` and copy behavior in `importer.go` must stay aligned. Do not bypass the coordinator when adding new import paths, and keep cache updates consistent with on-disk results.

## Testing
Extend `importer_test.go` for copy, rollback, and in-flight coordination changes. Run the integration script for behavior that affects final target layout or thumbnail linking.
