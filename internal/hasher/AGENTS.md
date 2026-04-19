# Module Guidelines

## Scope
`internal/hasher` owns exact hashing, the cached dHash stored in the `phash` column, full perception hashing helpers, BK-tree search, and related update flows for media matching.

## Change Rules
Treat hash format stability as important. Changes here can affect duplicate grouping, cache keys, and import decisions across the project. Keep pure hashing logic separate from orchestration, and preserve the distinction between image-capable inputs and `NOT_IMAGE` cases. Visual hashing should continue to degrade gracefully for complex formats by preferring embedded previews when available, including RAW-derived previews that decode as TIFF/WebP/BMP rather than only JPEG/PNG. Keep the stage split clear: `file_cache.phash` means first-stage `dHash`, while full perception hash belongs to the heavier second-stage derivative checks and must not silently reuse the `phash` name.
Prefer the explicit `dHash` API names in code (`CalculateDHash`, `DHashToString`, `StringToDHash`). The old `*PHash*` helpers remain as compatibility aliases only.

## Testing
Update the nearest `*_test.go` file for algorithm, threshold, or updater changes. Validate both correctness and edge cases such as unsupported formats, identical images, and near-duplicates.
