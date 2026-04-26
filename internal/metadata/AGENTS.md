# Module Guidelines

## Scope
`internal/metadata` extracts timestamps, MIME type, dimensions, and media metadata from files using EXIF data, filename heuristics, and platform-specific birth-time fallbacks.

`GetMetadata` is used by the scanner to populate `photos.db`.
`ExtractImageMetaJson` supplies the JSON metadata strings stored in target and cache records.

## Change Rules
Keep platform-specific behavior in the existing `birthtime_*` files.
Extraction should degrade gracefully when EXIF, filesystem birth time, or image decoding is unavailable.

Do not embed scanner or importer policy here. This package should return metadata and leave workflow decisions to callers.
If you change precedence between EXIF, filename, birth time, or modification time, make the ordering explicit and keep it cross-platform safe.

## Testing
Update `extractor_test.go` or `metadata_test.go` when changing precedence or fallback behavior. Add cross-platform-safe tests where possible and keep OS-specific assumptions isolated.
