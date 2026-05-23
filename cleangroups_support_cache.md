# CleanGroups Support Cache Plan

## Goal
Update `cleangroups` so it can use precomputed visual features from `visual_feature_cache` instead of repeatedly recalculating them from files during cleanup.

The change must preserve current behavior:

- same cleanup decisions
- same fallback behavior when features are unavailable
- no requirement that precompute has already been run

`cleangroups` should prefer persisted features, but must remain correct when cache rows are missing, stale, or partially unusable.

## Current Problem
`cleangroups` currently revalidates thumbnail relationships by repeatedly computing:

- dHash from file decode
- full perception or color-style checks from file decode
- ORB keypoints/descriptors from OpenCV decode and preprocessing

This work is repeated:

- for the current master
- for each thumbnail
- again for each rehome candidate
- again across many groups during a long run

The result is excessive decode, resize, preview extraction, and ORB work.

## Design Goal
Introduce a feature-resolution path so `cleangroups` can:

1. identify the file by `mmh3_hash`
2. load precomputed features from `visual_feature_cache`
3. use those features directly in derivative validation
4. fall back to current on-demand computation only when needed

This should cut repeated work without changing cleanup semantics.

## Integration Strategy
The integration should be done in layers:

1. add a read path for `visual_feature_cache`
2. add a runtime resolver that can return features for a file
3. update `dedupe` and `vision` to consume resolved features
4. switch `cleangroups` to use the resolver
5. keep on-demand fallback for cache misses and broken cache rows

## Feature Read Layer
Add a read/query API for `visual_feature_cache`, likely in `internal/precompute` or a small shared package close to it.

The reader should support:

- lookup by `(mmh3_hash, feature_version)`
- returning a structured feature object
- ORB keypoint/descriptors reconstruction
- explicit cleanup of reconstructed OpenCV mats

Suggested returned data:

- `PerceptionHash`
- `ColorSignature`
- `ORBKeypoints`
- `ORBDescriptors`
- `ORBRows`
- `ORBCols`
- `ORBType`
- `FeatureVersion`

The load layer should not know anything about cleanup policy. It should only handle persistence and reconstruction.

## Runtime Resolver
Add a light runtime resolver used by `cleangroups`.

Responsibilities:

- accept a file identity and path
- look up persisted features by `mmh3_hash`
- memoize per-run results in memory
- return either loaded cached features or freshly computed features
- expose whether the result was a cache hit or a fallback compute

The resolver should cache by `mmh3_hash`, not by path.

This matters because:

- paths can move
- `rehome` changes path relationships
- the same content can appear under multiple paths

## Cleangroups Call Sites
The resolver should be wired into these paths:

- thumbnail revalidation against its current master
- rehome candidate validation
- any path that currently triggers repeated ORB verification

Most likely call chain:

- `internal/target/clean_groups.go`
- `internal/dedupe/thumbnail.go`
- `internal/vision/opencv_verifier.go`

The goal is to stop these layers from always assuming “given a path, recompute everything”.

## Dedupe Refactor
`internal/dedupe` should grow a feature-aware validation path.

Instead of only:

- `RevalidateDerivative(childPath, childMeta, childSize, parentPath, parentMeta, parentSize)`

add a path that can accept pre-resolved features, for example:

- child feature bundle
- parent feature bundle

That validation path should still preserve:

- metadata dimension checks
- aspect ratio checks
- parent preference logic
- ORB confirmation logic

Only the source of expensive visual features changes.

## Vision Refactor
`internal/vision` should support ORB verification from already-available feature data.

Today it assumes:

- load image
- normalize
- detect keypoints/descriptors
- match
- run geometric verification

It should also support:

- match from preloaded keypoints/descriptors
- reconstruct descriptors from persisted bytes
- keep the geometric verification logic unchanged

Suggested split:

- one function extracts ORB features from a path
- one function verifies using two ORB feature sets

This keeps OpenCV-specific logic isolated while allowing cache reuse.

## Fallback Rules
Fallback behavior must be explicit and safe.

Cases that should fall back to on-demand compute:

- no `mmh3_hash` available
- no row in `visual_feature_cache`
- `feature_version` mismatch
- ORB keypoint decode fails
- ORB descriptor reconstruction fails
- persisted row is incomplete

Fallback must:

- preserve correctness
- log why cache could not be used
- avoid crashing the cleanup run

## Logging
Add structured logging around feature resolution so the real performance effect can be measured.

Recommended event types:

- `feature_cache_hit`
- `feature_cache_miss`
- `feature_cache_fallback`
- `feature_cache_invalid`
- `feature_cache_compute`

Suggested fields:

- `mmh3_hash`
- `path`
- `reason`
- `source` (`persisted` or `computed`)
- `phase` (`revalidate` or `rehome`)

These logs should be concise but sufficient for later NAS benchmarking.

## Memory / Resource Handling
ORB descriptor mats reconstructed from persisted bytes are native OpenCV resources.

Implementation must ensure:

- reconstructed `gocv.Mat` is closed
- runtime resolver has a clear `Close()` method
- `cleangroups` closes resolver-owned resources even on early returns

This is a hard requirement. Avoid leaking mats across long cleanup runs.

## Resolver Scope
The runtime resolver should live only for a single `cleangroups` run.

That gives:

- reuse within the run
- no stale cross-process state
- easy cleanup of reconstructed mats

It should not mutate persisted cache contents in the first integration step.

## Rollout Strategy
Do not combine all changes into one opaque refactor.

Recommended order:

1. add persisted feature reader + ORB reconstruction
2. add runtime resolver with per-run memoization
3. add feature-aware `dedupe` entrypoints
4. add ORB verification from feature sets
5. wire resolver into `cleangroups`
6. add logs and fallback counters
7. benchmark on NAS

This keeps each step verifiable and makes regressions easier to isolate.

## Testing Plan
### Unit Tests
Add tests for:

- visual feature cache row load
- ORB reconstruction from persisted bytes
- resolver cache hit behavior
- resolver fallback behavior

### Behavior Tests
Add tests that verify:

- `cleangroups` result matches existing behavior when cache is present
- `cleangroups` result matches existing behavior when cache is absent
- damaged ORB payload falls back rather than failing the run

### Integration Tests
Build temporary `cache.db` cases where:

- one master and thumbnail both have cached features
- rehome candidate exists and is validated from cache
- missing cache rows force recomputation

Assertions should check:

- cleanup result
- persisted rows remain unchanged
- logs show hit/miss/fallback behavior

## Performance Validation
After implementation:

1. build with `container_build.sh`
2. upload binary as `/volume3/DocsAndMedia/photo_organize2`
3. run inside container `773f2d87fee4`
4. compare baseline `cleangroups` and cache-backed `cleangroups`
5. record:
   - wall time
   - groups per minute
   - thumbnails per minute
   - cache hit ratio
   - fallback count

This validation is necessary before deciding whether to restart the long-running cleanup from scratch.

## Non-Goals For First Step
- no automatic backfill from `cleangroups` into `visual_feature_cache`
- no schema change to `file_cache`
- no path-keyed feature cache
- no change to cleanup decision policy
- no web UI change tied to this integration
