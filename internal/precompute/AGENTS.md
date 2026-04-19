# Module Guidelines

## Scope
`internal/precompute` implements offline feature precomputation for target caches. It discovers cache rows with non-empty `thumbnails` relationships and persists visual features into a dedicated table inside `cache.db`.

## Change Rules
Keep this package focused on orchestration: discovery, concurrency, persistence, and logging. Do not move file-layout mutation or thumbnail relationship logic here; that stays in `internal/target`. Keep OpenCV types isolated behind `internal/vision` helpers. This package is for second-stage derivative-confirmation features only: full perception hash, color signature, and ORB. The first-stage `dHash` remains identified with `file_cache.phash` and should not be treated as the same cache layer.

## Testing
Add unit tests for row selection, deduplication, and cache skip logic. Prefer small deterministic JPEG fixtures created at test time; avoid depending on large committed fixtures unless the behavior specifically requires them.
