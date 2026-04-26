# Module Guidelines

## Scope
`internal/hasher` owns exact MMH3 hashing, first-stage dHash, legacy pHash aliases, full perception hash helpers, color signatures, BK-tree search, and hash update flows for the `photos` and `file_cache` tables.

`CalculateHash` computes the exact file-level MMH3 hash.
`CalculateDHash` is the canonical first-stage visual hash.
`CalculateFullPerceptionHash` is the stronger perception hash used for second-stage checks.
`CalculateColorSignature` and `ColorSignatureDistance` support derivative confirmation.
`UpdateHashes` and `AssignGroupIDs` are orchestration helpers that write back to SQLite.

## Change Rules
Treat hash format stability as important. Changes here affect duplicate grouping, cache keys, and import decisions across the project.

Keep pure hashing logic separate from orchestration, and preserve the distinction between image-capable inputs and `NOT_IMAGE` cases.
Visual hashing should continue to prefer embedded previews when available, including RAW-derived previews that decode as TIFF/WebP/BMP.

Keep the stage split clear: `file_cache.dhash` means first-stage `dHash`, while full perception hash belongs to the heavier second-stage derivative checks and must not silently reuse the dHash name.
Prefer the explicit dHash API names in code (`CalculateDHash`, `DHashToString`, `StringToDHash`). The old `*PHash*` helpers remain as compatibility aliases only.

`UpdateHashes` currently writes `NOT_IMAGE` for non-image files and `UNSUPPORTED` when image hashing fails after the file is known to be image-capable. Preserve that behavior unless callers are updated too.

## Testing
Update the nearest `*_test.go` file for algorithm, threshold, or updater changes. Validate both correctness and edge cases such as unsupported formats, identical images, preview fallback, and near-duplicates.
