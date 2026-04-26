# Module Guidelines

## Scope
`internal/precompute` implements offline feature precomputation for target caches.

It discovers `file_cache` rows with thumbnail relationships, deduplicates them by `mmh3_hash`, and persists second-stage visual features into `visual_feature_cache` inside `cache.db`.
The persisted payload currently includes color signature data and ORB serialization, and it is consumed through `Resolver`.

## Change Rules
Keep this package focused on orchestration: discovery, concurrency, persistence, and logging.
Do not move file-layout mutation or thumbnail relationship logic here; that stays in `internal/target`.
Keep OpenCV types isolated behind `internal/vision` helpers.

This package is for second-stage derivative-confirmation features only.
The first-stage `dHash` remains identified with `file_cache.dhash` and should not be treated as the same cache layer.

`Run` should continue to skip already-cached `mmh3_hash` values unless forced.
`Resolver` should remain safe for repeated lookups, support in-flight de-duplication, and close any ORB resources it owns.

## Testing
Add unit tests for row selection, deduplication, cache skip logic, and resolver fallback behavior. Prefer small deterministic JPEG fixtures created at test time; avoid depending on large committed fixtures unless the behavior specifically requires them.
