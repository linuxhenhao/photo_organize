# Feature Cache Plan

## Goal
Add a new `precompute` command that precomputes and persists visual features used by thumbnail-group validation:

- full perception hash
- color signature
- ORB feature

The command must:

- only process files related to cache rows whose `thumbnails` column is present and not empty
- skip rows where `thumbnails` is `NULL`, `''`, or `'[]'`
- use `mmh3_hash` as the cache key
- run in parallel, with default worker count based on CPU cores
- allow worker count override by CLI flag

## Command Shape
Add a new CLI command:

```bash
photo_organize precompute -dest repo
```

Initial flags:

- `-dest`: target repository containing `cache.db`
- `-workers`: optional; default `runtime.NumCPU()`
- `-force`: recompute even if cached features already exist

Optional later flags:

- `-limit`
- `-start-after`
- `-features`

First implementation should keep the surface area small and only ship the required flags.

## Scope Selection
Only process files tied to rows where `file_cache.thumbnails` is meaningful.

Selection rules:

- include rows where `thumbnails IS NOT NULL`
- exclude rows where `thumbnails = ''`
- exclude rows where `thumbnails = '[]'`

For each included row:

- include the master file itself
- include every thumbnail path listed in that row's `thumbnails` JSON

Deduplication rules:

- dedupe by `mmh3_hash`
- if multiple paths share the same `mmh3_hash`, compute once and reuse the stored result

Notes:

- master rows already have `mmh3_hash`
- thumbnail entries should use `thumbnails[].mmh3_hash`
- if a thumbnail entry is still missing `mmh3_hash`, log and skip it in the first version rather than recomputing hash during lookup
- operators should run readonly `initcache` backfill first if needed

## Storage
Create a new table in `cache.db`, separate from `file_cache`.

Suggested name:

- `visual_feature_cache`

Schema direction:

```sql
CREATE TABLE IF NOT EXISTS visual_feature_cache (
    mmh3_hash TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    perception_hash TEXT NOT NULL DEFAULT '',
    color_signature BLOB,
    orb_keypoints BLOB,
    orb_descriptors BLOB,
    orb_rows INTEGER NOT NULL DEFAULT 0,
    orb_cols INTEGER NOT NULL DEFAULT 0,
    orb_type INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (mmh3_hash, feature_version)
);
```

Why a separate table:

- avoids bloating `file_cache`
- avoids inflating `thumbnails` JSON
- allows algorithm versioning without touching base cache rows
- makes `precompute` and later `cleangroups` lookup straightforward

## Versioning
Define a single feature version constant, for example:

- `visualFeatureVersion = "v1"`

This version must cover:

- full perception hash algorithm
- color signature algorithm
- image preprocessing rules
- ORB parameters
- ORB serialization format

Any incompatible change must bump `feature_version`.

## Package Layout
Add a new orchestration package:

- `internal/precompute`

Responsibilities:

- discover eligible files from `cache.db`
- dedupe work items
- run worker pool
- compute features
- persist results
- emit progress logs

Keep pure feature extraction in existing lower-level packages where practical, but do not bury the orchestration in `internal/target`, `internal/hasher`, or `internal/vision`.

## Work Item Model
Each queued item should minimally carry:

- `Path`
- `MMH3`
- `Kind` (`master` or `thumbnail`)
- `SourceMaster` or owner row path for logging

Processing flow:

1. enumerate rows with non-empty `thumbnails`
2. build master + thumbnail work items
3. dedupe by `mmh3_hash`
4. check existing `visual_feature_cache`
5. skip unless `-force`
6. queue remaining items to workers
7. write results through a serialized DB writer

## Parallelism
Default worker count:

- `runtime.NumCPU()`

Override:

- `-workers=N`

Implementation model:

- one producer
- `N` worker goroutines
- one writer goroutine for SQLite upserts

The writer should batch or serialize writes to avoid SQLite contention.

## Feature Computation
All three features are required in the first implementation:

- full perception hash
- color signature
- ORB feature

### Full Perception Hash
Use the existing full perception hash helper, with the clearer naming already introduced in `internal/hasher`.

Persist as:

- string form or fixed-width integer text

### Color Signature
Use existing color signature logic first.

Future optimization:

- compute from a normalized downscaled image rather than scanning full-resolution pixels

That optimization is not required for the first `precompute` implementation.

### ORB Feature
ORB feature must be implemented and cached in this first version.

Required cached content:

- keypoints
- descriptors
- descriptor shape/type metadata needed for reconstruction

Because current ORB verification uses matched point coordinates plus RANSAC, descriptors alone are not enough.

The stored ORB payload therefore needs:

- serialized keypoints
- serialized descriptor bytes
- row count
- column count
- OpenCV mat type

## ORB Serialization
Add a small serialization layer under `internal/vision` or `internal/precompute`, but keep OpenCV-specific details isolated from unrelated packages.

Suggested approach:

- encode keypoints into a compact binary format
- encode descriptor mat into raw bytes
- store mat metadata separately

On load:

- rebuild `[]gocv.KeyPoint`
- rebuild `gocv.Mat`

Implementation constraints:

- close any reconstructed `gocv.Mat` explicitly
- avoid leaking OpenCV resources
- keep serialization format versioned through `feature_version`

## Decode Reuse
The first version does not have to fully solve single-open shared decoding across hasher and ORB pipelines.

However, design the worker flow so that:

- feature computation for a given file happens once per `precompute` run
- the persisted result can later eliminate repeated `cleangroups` recomputation

If practical during implementation, prefer a unified decode path inside the worker so that:

- image decode/preview extraction is done once
- full perception hash and color signature reuse the same decoded image
- ORB preprocessing reuses the same source bytes or normalized image

This is a desirable implementation detail, but persisted feature caching is the primary goal.

## Cache Lookup Rules
Before computing any item:

1. look up `(mmh3_hash, feature_version)` in `visual_feature_cache`
2. if found and `-force=false`, skip
3. otherwise compute and upsert

This ensures:

- same content under multiple paths only computes once
- reruns mostly skip completed work

## Logging
Add structured logs for long-running execution.

Required events:

- `precompute_start`
- `precompute_progress`
- `precompute_skip_cached`
- `precompute_failed`
- `precompute_done`

Recommended fields:

- `workers`
- `queued`
- `processed`
- `skipped`
- `failed`
- `feature_version`
- `current_path`
- `mmh3_hash`

Progress emission should be time-based and count-based:

- every 30 seconds
- and every 50 processed items

Do not rely only on file count triggers.

## Database Integration
Add table creation/migration near existing cache initialization code, but keep feature-cache CRUD separate from `file_cache` CRUD.

Needed operations:

- ensure feature table exists
- lookup by `(mmh3_hash, feature_version)`
- upsert feature row
- optional count/query helpers for tests

## Later Cleangroups Integration
Do not couple this first change to a large `cleangroups` refactor.

Recommended rollout:

1. land `precompute` and feature table
2. verify performance and DB growth
3. update `cleangroups` to read from `visual_feature_cache`
4. only fall back to on-demand computation on cache misses

This keeps risk lower and makes performance gains measurable.

## Testing Plan
### Unit Tests
Add tests for:

- eligible row selection
- thumbnail JSON parsing and dedupe by `mmh3_hash`
- skip behavior when cache entry already exists
- `-force` recompute behavior
- ORB serialization and reconstruction round-trip

### Integration Tests
Add tests that:

- create a temporary `cache.db`
- populate rows with `thumbnails = NULL`, `''`, `'[]'`, and populated JSON
- verify only populated rows contribute work
- verify master + thumbnail files are included
- verify repeated `mmh3_hash` values compute once
- verify `visual_feature_cache` rows are written

### Fixture Tests
Use existing real fixtures where available:

- ARW fixture
- CR2 fixture

Verify:

- RAW-backed files can be precomputed successfully
- second run mostly skips cached rows
- ORB payload is present and loadable

## Implementation Order
1. Add `precompute` CLI entry and flags.
2. Add `visual_feature_cache` table creation and CRUD helpers.
3. Add eligible-row discovery from `file_cache`.
4. Add worker pool and progress logging.
5. Add full perception hash persistence.
6. Add color signature persistence.
7. Add ORB extraction, serialization, and persistence.
8. Add tests.
9. Benchmark on NAS container with the real repo.

## Benchmark Plan
After implementation:

1. build with `container_build.sh`
2. upload binary to `/volume3/DocsAndMedia/photo_organize2`
3. run inside container `773f2d87fee4`
4. run `precompute` against `/volume3/DocsAndMedia/Multimedia/repo`
5. compare later `cleangroups` throughput before and after feature-cache consumption is integrated

## Risks
- `mmh3_hash` collisions are possible but acceptable as an engineering tradeoff for now
- ORB payloads can make `cache.db` significantly larger
- OpenCV resource cleanup must be handled carefully
- RAW preview extraction consistency affects feature stability
- algorithm parameter changes require `feature_version` bump

## Non-Goals For First Step
- no attempt to process rows with empty or missing `thumbnails`
- no full-library feature precompute
- no path-keyed feature cache
- no direct mutation of file layout
- no forced `cleangroups` integration in the same change
