# Module Guidelines

## Scope
`internal/metadata` extracts timestamps, dimensions, and media metadata from files using EXIF data, filename heuristics, and platform-specific birth time fallbacks.

## Change Rules
Keep platform-specific behavior in the existing `birthtime_*` files. Extraction should degrade gracefully when EXIF or image decoding is unavailable. Avoid embedding scanner-specific policy here; this package should return metadata, not decide workflow.

## Testing
Update `extractor_test.go` or `metadata_test.go` when changing precedence or fallback behavior. Add cross-platform-safe tests where possible and keep OS-specific assumptions isolated.
