# Rust Rewrite Design Spec

Date: 2026-04-25

## Summary
Rewrite `photo_organize` as a single Rust program focused on three commands only:

- `scan`
- `import`
- `initcache`
- `serve`

Everything else in the current Go program is intentionally removed:

- no `cleangroups`
- no `precompute`
- no `convertdb`

The rewrite should rely on native Rust crates for metadata extraction, hashing, SQLite access, image decoding, and HTTP serving. No `exiftool`. No OpenCV. No background cleanup pipeline.

The core product idea becomes:

1. scan source trees into a database
2. import canonical files into a date-based target tree
3. adopt an existing target tree in place with `initcache`
4. use the web UI to manually resolve visual duplicate groups

This narrows the product to the workflows that matter and removes the extra cache-maintenance machinery that exists mainly to support automatic or semi-automatic derivative cleanup.

## Goals
- Keep the current useful workflow: source scan, organized import, manual duplicate resolution.
- Replace external runtime dependencies with native Rust crates.
- Simplify the data model so the web server does not depend on extra offline maintenance commands.
- Use `pHash` as the single visual hash for candidate grouping.
- Require AKAZE double-check before creating visual duplicate groups.
- Keep exact duplicate detection separate from visual duplicate grouping.
- Preserve target layout: `DEST/YYYY/MM/DD/...`.
- Keep the web server local-first and safe for destructive actions.

## Non-Goals
- No automatic thumbnail cleanup command.
- No offline feature-precompute command.
- No OpenCV-based ORB verification.
- No migration compatibility promise for the current Go `photos.db` and `cache.db` schema.
- No cloud sync, remote API, or multi-user service mode.

## Product Shape
Use one binary, for example `photo-org`.

Commands:

- `photo-org scan --scan-db /data/scan-phone.db --src /mnt/phone`
- `photo-org import --db catalog.db --dest /photos/library`
- `photo-org import --db catalog.db --scan-db /data/scan-phone.db --dest /photos/library`
- `photo-org import --db catalog.db --src /mnt/phone --dest /photos/library`
- `photo-org initcache --db catalog.db --dest /photos/library`
- `photo-org serve --db catalog.db --dest /photos/library --host 127.0.0.1 --port 8080`

This keeps the operational model simple:

- `scan` builds or refreshes source inventory in a separate scan database
- `import` copies new canonical files into the target tree and records visual group membership
- `initcache` adopts an existing target tree into the database without copying files and without introducing a durable source-side scan database for `--dest`
- `serve` shows visual groups and applies manual keep/delete decisions

## Why Remove The Other Commands
The current Go program grew extra commands because the target cache and derivative-validation pipeline became a separate subsystem.

For the Rust rewrite, that extra subsystem should be removed instead of ported.

Reasons:

- manual web dedup is the real conflict-resolution tool
- `pHash` is fast enough to be computed inline during scan/import for this workload
- the operational cost of extra commands is higher than the runtime savings they provide
- native Rust metadata and image crates let the common path stay self-contained

`initcache` is the one exception that must remain. Existing organized target repositories need an in-place adoption path. Re-copying the library only to reconstruct DB state is not acceptable.

## Proposed Architecture

### High-level modules
- `cli`: `clap` command parsing
- `db`: schema setup, queries, transactions
- `scan`: directory traversal and source inventory refresh
- `meta`: timestamps, EXIF, filename parsing, MIME/type detection
- `hash`: exact hash and visual hash
- `vision`: AKAZE feature extraction and match verification
- `import`: canonical selection, target path planning, copy, rename, and group assignment
- `web`: local HTTP server and HTML/JS frontend
- `preview`: preview extraction and image serving
- `cache`: in-memory hot caches and DB-backed feature cache
- `fs`: path safety, file moves, rename helpers, empty-dir cleanup

Implementation note:

- file discovery, metadata extraction, exact hashing, and image feature extraction should be implemented once as a shared fact-collection pipeline
- `exact_hash`, `pHash`, `phash_bits`, and image dimensions are base per-file facts and belong on `source_items` or `target_items`
- second-stage cached feature data refers to expensive confirmation data such as AKAZE descriptors, not to base per-file facts
- the destination of those collected facts should be pluggable
- `scan` writes source facts into `source_items`
- `initcache` writes target facts directly into `target_items`
- shared traversal and feature code is desirable; shared durable row semantics are not

### Runtime model
- `scan` and `import` are synchronous pipelines with controlled thread parallelism via `rayon`
- `serve` uses `tokio` + `axum`
- SQLite access stays simple and explicit, using `rusqlite`

The rewrite should avoid an all-async architecture for the whole program. Most heavy work here is filesystem, image decode, and SQLite transactions, not network concurrency.

## Crate Selection
These versions and capabilities were checked from current docs on 2026-04-25.

Core crates:

- `clap 4.5.60` for CLI parsing
- `rusqlite 0.39.0` for SQLite
- `tokio 1.51.1` for the web runtime
- `axum 0.8.9` for the HTTP server
- `tower-http 0.6.8` for tracing and static-file middleware
- `serde 1.0.228` and `serde_json` for API payloads
- `tracing` and `tracing-subscriber 0.3.23` for structured logs

Filesystem and concurrency:

- `walkdir 2.5.0` for deterministic recursive traversal
- `rayon 1.12.0` for CPU-bound parallel work

Metadata and media:

- `kamadak-exif 0.6.1` for pure-Rust EXIF parsing
- `infer 0.19.0` for file type and MIME sniffing
- `image 0.25.10` for image decode and resize
- `img_hash 3.2.0` for perceptual hashing, including DCT `pHash`
- `akaze 0.7.0` for AKAZE keypoint extraction used in visual duplicate confirmation
- `mp4parse 0.17.0` as the primary Rust path for MP4/MOV/ISO BMFF container metadata
- `matroska 0.30.0` as the Rust path for MKV container metadata

Exact hashing:

- recommended: `blake3 1.8.4`
- compatibility option: `murmur3 0.5.2`

Recommendation: use `blake3` for exact duplicates in the Rust rewrite. It is a better default than MMH3 for a new implementation because collision resistance matters more than preserving the old exact-hash choice. If compatibility with existing exported identifiers is required, use `murmur3` instead.

## Why `pHash` Plus AKAZE
The rewrite should remove the separate `dHash` layer and use `pHash` as the visual candidate hash, with AKAZE as a required second check before any visual duplicate group is created.

Reasoning:

- local benchmarking in this repo did not show a meaningful end-to-end runtime win for `dHash`
- `pHash` is the stronger signal for manual duplicate grouping
- removing `dHash` simplifies the schema, cache semantics, and mental model
- AKAZE provides a local-feature confirmation step that is more appropriate than hash-only grouping when the product must avoid wrong duplicate groups
- without `precompute`, there is less value in splitting the visual pipeline into multiple staged hash layers beyond `pHash` candidate generation and AKAZE confirmation

Visual duplicate flow in the rewrite:

1. compute exact hash
2. if exact duplicate, keep one canonical target file and do not create a duplicate group
3. if image-capable, compute `pHash`
4. use Hamming-distance threshold to select candidate visual neighbors
5. run AKAZE keypoint extraction and matching against those candidates
6. create a visual duplicate group only if the AKAZE check passes
7. resolve the resulting group in the web UI

## Metadata Strategy
The Rust rewrite must not rely on `exiftool`.

Timestamp precedence:

1. EXIF capture time when available
2. filename date heuristic
3. filesystem birth time when available
4. modification time fallback

Image metadata:

- dimensions from `image`
- EXIF from `kamadak-exif`
- MIME from `infer` plus extension fallback

RAW support:

- attempt EXIF extraction directly with `kamadak-exif`
- for visual hashing, prefer embedded preview bytes when available
- if a RAW image cannot yield a preview or decodable image, keep exact dedup only and mark visual hash and AKAZE confirmation as unavailable

Video support:

- first version should extract video date metadata when supported by the container parser
- use container/media metadata date first for supported formats such as MP4/MOV and MKV
- if no usable container timestamp is available, fall back to filename date heuristic
- then fall back to filesystem birth time
- then fall back to modification time

This keeps video handling materially better than generic-file fallback without requiring a full multimedia stack in v1.

## Data Model
Use one durable target database: `catalog.db`.

Use separate scan databases for source-side ingest state.
One outer source gets one scan DB, for example:
- `/data/scan-phone.db`
- `/data/scan-camera-card.db`
- `/data/scan-downloads.db`

Do not mix long-lived target state and source-side ingest state in the same database.

Persistence boundary:

- `catalog.db` is the durable database for the managed target repository
- `scan-*.db` files are source-side staging state only
- `scan-*.db` files are intentionally disposable; deleting one only loses source inventory history and forces a re-scan
- removable or temporary sources such as USB devices should not leave behind required durable state in `catalog.db`
- `initcache` is target adoption, not source ingest, so it must not create a durable `source_items` database for the target tree

### Tables

In `scan-*.db`:

`source_items`
- `id INTEGER PRIMARY KEY`
- `source_path TEXT UNIQUE NOT NULL`
- `size_bytes INTEGER NOT NULL`
- `mime_type TEXT NOT NULL DEFAULT ''`
- `created_at TEXT NOT NULL`
- `exact_hash TEXT NOT NULL DEFAULT ''`
- `phash TEXT NOT NULL DEFAULT ''`
- `phash_bits INTEGER NOT NULL DEFAULT 0`
- `width INTEGER NOT NULL DEFAULT 0`
- `height INTEGER NOT NULL DEFAULT 0`
- `scan_status TEXT NOT NULL`
- `last_scanned_at TEXT NOT NULL`
- `meta_json TEXT NOT NULL DEFAULT '{}'`

In `catalog.db`:

`target_items`
- `id INTEGER PRIMARY KEY`
- `target_path TEXT UNIQUE NOT NULL`
- `size_bytes INTEGER NOT NULL`
- `mime_type TEXT NOT NULL DEFAULT ''`
- `created_at TEXT NOT NULL`
- `exact_hash TEXT NOT NULL DEFAULT ''`
- `phash TEXT NOT NULL DEFAULT ''`
- `phash_bits INTEGER NOT NULL DEFAULT 0`
- `width INTEGER NOT NULL DEFAULT 0`
- `height INTEGER NOT NULL DEFAULT 0`
- `group_id INTEGER`
- `keep_state TEXT NOT NULL DEFAULT 'undecided'`
- `is_group_primary INTEGER NOT NULL DEFAULT 0`
- `origin_source_id INTEGER`
- `meta_json TEXT NOT NULL DEFAULT '{}'`

`operations_log`
- `id INTEGER PRIMARY KEY`
- `kind TEXT NOT NULL`
- `payload_json TEXT NOT NULL`
- `created_at TEXT NOT NULL`

### Notes
- exact duplicates do not create groups
- `group_id` is a visual cluster id stored directly on `target_items`
- `keep_state` is one of `undecided`, `kept`, or `rejected`
- `is_group_primary` marks the preferred representative among grouped files
- AKAZE-confirmed visual groups use one shared `group_id`
- `source_items` live in separate scan DBs and are not part of the durable target catalog
- there is no `thumbnails` JSON column
- there is no separate `visual_feature_cache`
- manual resolution is modeled as row updates on `target_items`, with history in `operations_log`

This schema is much easier to reason about than the current `file_cache + thumbnails JSON` model and simpler than a dedicated `duplicate_groups` object model.

## Scan Command Design
`scan` walks one or more source roots and upserts rows in `source_items`.

### Behavior
- recursively enumerate files
- gather stat info
- detect MIME/type
- extract timestamp and basic metadata
- compute exact hash for every file
- compute `pHash` only for decodable image-like inputs
- do not attempt AKAZE matching during `scan`; `scan` computes per-file facts, not cross-file grouping
- upsert by `source_path`
- mark missing files as stale rather than deleting immediately

### Parallelism
- one traversal thread
- bounded worker pool for metadata and hashing
- one SQLite writer thread or batched transaction stage

### Idempotence
Re-running `scan` on the same roots should be safe and cheap when unchanged files can be skipped by a fast fingerprint such as `(size, mtime, inode if available)`.

### Retention
- `scan` output is staging data, not catalog data
- operators may keep one scan DB per removable source when that is convenient
- operators may also delete a scan DB immediately after import with no effect on the managed target repository
- the system must never require old `source_items` rows to preserve target-side correctness

### Timestamp precedence
Photo files:

1. EXIF capture time
2. filename date heuristic
3. filesystem birth time
4. modification time

Video files:

1. container/media metadata date
2. filename date heuristic
3. filesystem birth time
4. modification time

All accepted timestamps should be normalized into one internal representation before writing to the database. If container metadata is present but obviously malformed, fall back to the next source and log the conflict.

## Import Command Design
`import` reads `source_items`, chooses canonical files, copies them into the target tree, and records visual group membership in `target_items`.

`import` may also auto-run a scan when `--src` is provided directly. In that mode:
- the command creates or refreshes a scan DB for that source
- then imports from that scan DB into `catalog.db`
- the architecture still treats scan and import as separate phases even when the CLI combines them

Source-side persistence rules:

- `import` reads from a source scan DB, not from `catalog.db`
- when `--src` is provided directly, the implementation may use a user-specified scan DB or create a temporary one as an implementation detail
- whichever path is chosen, source inventory remains separate from `catalog.db`
- imported target correctness must not depend on retaining old `source_items` rows after import completes

### Canonical import rules
- group exact duplicates by `exact_hash`
- import exactly one canonical file for each exact-hash group
- choose the canonical source using explicit ranking:
  - preferred media type and extension
  - larger dimensions when available
  - larger file size
  - lexicographically stable final tie-break

### Target layout
- `DEST/YYYY/MM/DD/file.ext`
- conflict resolution by suffixing: `name-1.ext`, `name-2.ext`

### Visual grouping
After canonical import of an item:

1. search existing `target_items` with non-empty `phash`
2. compare Hamming distance against a threshold
3. for each plausible candidate, run AKAZE feature extraction and matching
4. if no confirmed visual match exists, leave `group_id = NULL`
5. if there are confirmed visual matches, collect the matched rows
6. if at least one matched row already has a `group_id`, reuse that `group_id`
7. if confirmed matches exist but none has a `group_id`, mint a new `group_id`
8. if confirmed matches span multiple existing `group_id` values, merge them into one chosen `group_id`
9. assign the imported row to the final `group_id`
10. initialize grouped rows with `keep_state = 'undecided'`
11. optionally auto-suggest one `is_group_primary = 1` row based on ranking
12. do not auto-delete or auto-move the new file
13. let the web UI decide what to keep

This preserves safety. The Rust rewrite should not silently hide, move, or rewrite files just because a visual hash matched.

Visual grouping applies to images only in v1.
Videos participate in metadata extraction and import date placement, but they are not part of the AKAZE-backed visual duplicate pipeline.

### Exact duplicate handling
Exact duplicates should not produce multiple imported copies and should not create duplicate groups. They can be recorded in `operations_log` and optionally in a `source_to_target_map` table if provenance tracking is needed.

## Initcache Command Design
`initcache` walks an existing target repository in place and populates `target_items` plus visual group membership without copying files.

### Behavior
- recursively enumerate files under `--dest`
- gather stat info, metadata, exact hash, and `pHash`
- upsert rows in `target_items`
- propose visual candidates by `pHash` threshold
- require AKAZE confirmation before assigning visual `group_id` values
- merge overlapping groups when a new confirmed match connects previously separate groups
- initialize grouped rows with `keep_state = 'undecided'`
- mark missing DB rows stale if files disappeared
- on repeated runs, use stored target facts to skip unchanged files when `size_bytes` and persisted `modified_at` are unchanged; only re-read file content and recompute `exact_hash` when that fingerprint changes

Database boundary:

- `initcache` writes durable state only to `catalog.db`
- `initcache` does not own or maintain a `source_items` table for the target tree
- `initcache` should not require a persistent `initcache-scan.db`
- if the implementation temporarily stages scan results before commit, that staging data must be treated as ephemeral scratch state, not as part of the user-visible persistence model
- deleting any temporary `initcache` scratch DB must not lose durable target state beyond requiring the current run to restart

Implementation preference:

- `target_items` should be treated as the durable sink for `initcache`
- `target_items` already contains the per-file facts that matter for adopted target files, plus target-only review and grouping state
- the preferred code structure is a shared scan/fact pipeline with different sink strategies, not a fake source-side pass that persists target files into `source_items`

### Non-behavior
- do not move files
- do not create a `thumbnails/` subsystem
- do not perform automatic cleanup
- do not depend on a feature precompute cache
- do not treat `--dest` as a long-lived source inventory root

### Exclusions
By default, `scan` and `initcache` should exclude:
- hidden directories
- trash directories
- internal tool directories such as `.photo-org`

These exclusions should be configurable, but the default must protect the tool from indexing its own trash and implementation artifacts.

## Web Server Design
`serve` is a local web UI for manual duplicate resolution.

### Endpoints
- `GET /api/groups`
- `POST /api/groups/:id/resolve`
- `GET /image?path=...`
- `GET /api/groups/:id/archive` optional but useful
- static frontend at `/`

### UI behavior
- list visual groups
- show all members in a group
- show image previews and metadata
- allow:
  - keep one, delete rest
  - keep multiple
  - promote a different primary
  - leave some files undecided and return later

### Resolution behavior
Resolution must be transactional at the database layer and conservative on disk:

1. validate all requested paths are under `--dest`
2. lock all rows in the target group
3. move rejected files into a trash folder under `DEST`
4. apply any needed renames
5. update `target_items`
6. log the operation

No path supplied by the client may escape the destination root.

Trash behavior:
- rejected files should be moved, not permanently deleted, in v1
- the trash location should live under the managed destination, for example `DEST/.photo-org/trash/`
- trash moves must preserve enough information to avoid filename collisions

### Candidate group query
The web UI should discover candidate groups from `target_items.group_id`, not from review history.

Pending groups are:
- rows with `group_id IS NOT NULL`
- grouped by `group_id`
- shown only when at least one member has `keep_state = 'undecided'`

Reviewed groups are:
- rows with `group_id IS NOT NULL`
- all members have `keep_state IN ('kept', 'rejected')`

This keeps grouping and review state separate:
- `group_id` answers which files are visually related
- `keep_state` answers what the user decided
- `is_group_primary` answers which grouped file is the preferred representative

Reviewed groups remain in the catalog after review. They do not dissolve automatically when all members are decided.

## Preview Strategy
The current Go web server has complicated Synology and UGOS thumbnail lookup logic because it integrates with NAS thumbnail systems.

The Rust rewrite should not depend on NAS-specific thumbnail stores.

Preview order:

1. use the original image directly if browser-safe and reasonably sized
2. if running on UGREEN NAS OS and an existing system thumbnail is discoverable, use it for web preview serving only
3. else generate a preview on demand for the current response only
4. for RAW files, use embedded preview when available

This keeps the program portable while still taking advantage of existing NAS thumbnails when available.

UGREEN NAS OS thumbnail rules:
- existing system thumbnails may be used only by the web UI preview path
- they must not be used for exact hash computation
- they must not be used for `pHash`
- they must not be used for AKAZE extraction or duplicate confirmation
- they must not be treated as source-of-truth library data

The duplicate pipeline must always use the real media file or its embedded RAW preview, not a NAS-generated thumbnail.

## Performance, Caching, and Concurrency
The Rust rewrite must treat target-side visual-feature extraction and DB mutation as the primary performance-sensitive subsystems.

The main rule is:
- expensive work is computed lazily
- repeated target-side analysis work is persisted
- mutations are serialized where correctness requires it
- no cache is allowed to weaken correctness

### Performance Goals
- `scan` should be able to saturate metadata and hash workers without corrupting the database
- `import` should overlap file copy, metadata read, and candidate matching without race conditions
- `initcache` should be able to rebuild target state from an existing library without redoing the same expensive image work repeatedly
- `serve` should return group pages and previews quickly even on large libraries

### Core Cache Policy
Only target-directory cache is strategically valuable in the long term.

That means:
- the durable cache should be centered on managed target files
- `pHash` and AKAZE-derived data should be saved in the database
- preview artifacts are transient convenience data, not cache state
- source-side scan state lives in separate scan DBs and should not drive target-catalog cache growth
- source-side scan state may be deleted at any time without invalidating target-side durable state

If there is a tradeoff between preview-related work and persisting target-side visual features, prefer persisted target-side visual features.

### Preview Pipeline
Preview generation is a separate pipeline from visual duplicate detection.

Preview order for a requested file:

1. if the original file is directly browser-safe and below a configured size threshold, serve the original
2. else, when running on UGREEN NAS OS, check for an existing NAS-generated thumbnail for that file
3. else generate the preview once for the current response and serve it without persisting it to a preview cache

Preview generation rules:
- decode once per request path
- orient according to metadata before resize when applicable
- resize to a small fixed set of presets such as `320`, `640`, and `1600`
- encode as JPEG or WebP with a fixed quality preset
- do not persist preview files to a long-term on-disk cache

UGREEN NAS OS behavior:
- NAS thumbnail discovery should be implemented as an optional preview adapter
- it should be best-effort only
- failures in NAS thumbnail lookup must fall back to the normal preview pipeline
- the result may be cached as an in-memory lookup result, but not as duplicate-analysis input

### AKAZE Compute Strategy
AKAZE is required for visual duplicate confirmation, so it must be handled as a bounded expensive resource.

Rules:
- never run AKAZE on every pair in the library
- use `pHash` to shortlist candidates first
- decode once per file per matching batch when possible
- compute AKAZE descriptors once per file and reuse them for all candidate comparisons in that operation

AKAZE work should be performed in:
- `import` when a new canonical file is evaluated against nearby existing target items
- `initcache` when constructing visual groups for an existing target tree

AKAZE should not run in:
- `scan`
- web list endpoints
- any generic background maintenance loop

### Feature Cache
Even without a `precompute` command, the program should use a lazy feature cache.

Recommended table:

`feature_cache`
- `exact_hash TEXT NOT NULL`
- `size_bytes INTEGER NOT NULL`
- `akaze_keypoints INTEGER`
- `akaze_descriptors BLOB`
- `feature_version INTEGER NOT NULL`
- `updated_at TEXT NOT NULL`
- `PRIMARY KEY (exact_hash, size_bytes)`

Design rules:
- cache entries are created on demand during `import`, `initcache`, or explicit target-side analysis
- there is no separate command to populate the table
- the cache is read-through and write-through
- the cache key is content-based `exact_hash + size_bytes`, not the path
- base item facts such as `exact_hash`, `pHash`, `phash_bits`, and dimensions remain on `source_items` or `target_items`
- cache misses are allowed and only affect performance, not correctness

Feature cache policy:
- this is the primary durable performance cache in the system
- it should be used only for managed target files
- source-side scan rows do not need heavyweight long-term feature persistence
- `serve` should reuse this table whenever visual-group operations need feature data

If AKAZE serialization turns out to be too large or unstable in the first implementation, the minimum acceptable fallback is:
- keep AKAZE in an in-memory per-run cache only

But the preferred design is persisted lazy AKAZE cache keyed by exact hash, because `initcache` and `serve` will otherwise repeatedly pay the same feature cost.

This is better than any preview-oriented caching strategy because:
- visual grouping depends on `pHash` and AKAZE, not preview files
- managed target files are long-lived, so feature rows have durable reuse value
- DB-backed feature reuse helps both `initcache` and later web review
- previews can always be regenerated on demand, but feature recomputation is repeated CPU cost

### In-Memory Caches
Use small, bounded in-memory caches for hot data only.

Recommended hot caches:
- UGREEN thumbnail lookup cache
- decoded image metadata cache
- feature cache row cache by exact hash
- group list page cache for the web server

Design requirements:
- bounded by count or memory budget
- safe for concurrent reads
- values must be immutable after insertion
- eviction must only affect performance

Avoid unbounded maps keyed by path. Source and target libraries can be large enough to turn a naive cache into a memory leak.

In-memory cache priority:
1. feature rows by exact hash
2. group list/query results
3. metadata decode results
4. UGREEN thumbnail lookup results

This ordering reflects the actual long-term value of each cache.

### Parallelism Model
The rewrite should use staged parallelism with bounded worker pools.

`scan`:
- one traversal producer
- bounded metadata/hash worker pool
- one DB writer stage using batched transactions

`import`:
- one reader over staged scan rows
- bounded copy workers
- bounded hash and AKAZE workers
- one DB mutation coordinator for group assignment and final row updates

`initcache`:
- one traversal producer over target files
- bounded metadata/hash workers
- bounded AKAZE workers
- one DB mutation coordinator for group assignment and merge updates

`serve`:
- async HTTP handling with `tokio`
- blocking image decode and AKAZE work moved to dedicated blocking pools
- DB writes for group resolution serialized per group or through one mutation path

### Target-Only Feature Persistence
Feature persistence should happen only after a file is part of the managed target repository.

That means:
- `scan` may compute transient `pHash` for staging and planning
- `scan` should not persist heavyweight AKAZE feature rows as durable cache
- `import` should persist feature rows for imported target files
- `initcache` should persist feature rows for adopted target files

This keeps durable cache aligned with the actual managed library instead of with arbitrary removable sources.

### SQLite Concurrency
SQLite should remain the source of truth and should not be stressed with uncontrolled concurrent writers.

Rules:
- enable WAL mode
- use one explicit write connection per process for mutation-heavy flows
- use short-lived read connections or a read pool for concurrent queries
- keep write transactions short
- batch inserts and updates where possible
- do not hold DB transactions open while performing image decode, AKAZE computation, or file copy

Correct pattern:
1. read candidate rows
2. release read transaction
3. do expensive compute outside the DB transaction
4. reopen a short write transaction
5. re-check assumptions if needed
6. commit quickly

This is especially important for `import`, `initcache`, and web group resolution.

### Group Assignment Concurrency
`group_id` assignment and merge logic is correctness-sensitive.

Rules:
- all `group_id` writes and group merges must go through one mutation coordinator
- two workers must never mint independent new groups for the same connected component
- merging groups must be transactional
- after expensive matching work completes, the final group decision must re-read the latest group state before commit

Recommended pattern:
- workers compute candidate match edges
- a single coordinator applies:
  - new `group_id` minting
  - reuse of existing `group_id`
  - merge of multiple `group_id` values
  - initialization of `keep_state`
  - update of `is_group_primary`

This keeps correctness simple and avoids subtle lost-update bugs.

### File Import Safety
Target file creation must also be race-safe.

Rules:
- reserve the final target path before copy
- never let two workers copy into the same final path
- write imports to a temporary sibling path first when practical
- rename into the reserved final path once the copy completes
- only update `target_items` after the file is durable enough to be treated as real

### Web Server Caching
The web server should not recompute expensive data on every request.

Recommended caching behavior:
- cache serialized group-list responses for a short TTL
- invalidate affected group-list cache entries after group resolution
- cache feature rows by exact hash

Do not cache mutable review state without invalidation. Group-resolution endpoints must invalidate or bump any cached group views that they touch.

The web server should prefer DB-backed feature reuse over filesystem preview reuse for performance-critical duplicate operations.

### Cancellation and Recovery
Long-running operations should support cancellation at safe points.

Rules:
- allow `scan`, `import`, and `initcache` to stop between items
- do not leave partial DB rows that claim a file was fully processed when it was not
- do not leave partial target files at their final path
- on restart, treat temp files as garbage-collectable

### Suggested Defaults
- metadata/hash worker count: CPU count or CPU count minus one
- AKAZE worker count: smaller than the general worker pool, for example `min(physical_cores / 2, 4..8)` depending on memory pressure
- one DB writer coordinator per process
- bounded in-memory caches sized conservatively, then tuned from measurements

### Measurement Plan
The implementation should include benchmarks and runtime counters for:
- feature cache hit rate
- preview generation latency
- average AKAZE computation latency
- import throughput in files/sec and bytes/sec
- initcache throughput in files/sec
- group merge count
- DB write contention or busy-retry count

The rewrite should be tuned from these measurements, not from intuition alone.

## Hash and Grouping Details

### Exact hash
Preferred:
- `blake3` hex string

Compatibility mode:
- `murmur3_x64_128` hex string

### Visual hash
- use `img_hash` DCT `pHash`
- store raw bits and text form
- store `phash_bits` so future bit-width changes are explicit

### AKAZE confirmation
- use `akaze` to extract local features from decoded images or embedded RAW previews
- use descriptor matching plus a simple good-match acceptance rule in v1
- require a minimum number of good matches before accepting a visual duplicate edge
- if AKAZE cannot run for one or both files, do not auto-assign a visual `group_id`

### Candidate threshold
Start conservative:
- exact duplicates: identical exact hash
- visual candidates: prefer high recall, with an initial `pHash` Hamming distance threshold around `12` to `16` for 64-bit hashes, followed by mandatory AKAZE confirmation

The threshold must be configurable, because the right value depends on the media set and whether the target is strict duplicates or broader near-duplicates.

## Error Handling and Logging
Use `tracing` with structured fields.

Required properties:
- log the source path and operation id on scan/import failures
- log group id and request id on web resolve operations
- emit summary lines for scan and import counts
- return actionable CLI errors instead of bare unwrap-style failures

## Testing Strategy

### Unit tests
- timestamp precedence
- exact hash determinism
- `pHash` grouping thresholds
- AKAZE match acceptance thresholds
- canonical file ranking
- visual group merge logic
- pending-group query behavior
- target path conflict resolution
- path-safety checks for web requests

### Integration tests
- scan a fixture tree
- import into a temp destination
- confirm exact duplicates import once
- confirm exact duplicates do not create visual groups
- confirm visual `group_id` values are assigned only when both `pHash` and AKAZE pass
- confirm overlapping candidate edges merge existing groups correctly
- confirm multi-keep resolution updates `keep_state` without dissolving the group
- confirm reviewed groups remain queryable after all members are decided
- confirm rejected files are moved to trash, not deleted
- run `initcache` against a pre-populated destination and verify it reconstructs state without copying files
- resolve a group through the HTTP API
- verify database state and on-disk state after resolution

### Fixture policy
Keep a small committed fixture set:
- JPEG
- PNG
- one or two RAW samples
- optional MP4 sample if later added

## Migration and Rollout
This rewrite should be treated as a new product version, not a line-by-line port.

Recommended rollout:

1. ship the Rust binary with `scan`, `import`, `initcache`, and `serve`
2. document that old Go cache databases are not reused
3. provide a one-time re-scan or target adoption path instead of a complicated DB migration

That is the simpler and safer transition.

## Open Decisions
- whether video metadata parsing beyond filename/filesystem fallback is in scope for v1
- final `pHash` distance threshold inside the chosen high-recall range
- final AKAZE minimum good-match threshold

## Recommendation
Build the Rust rewrite around one database, one visual candidate hash (`pHash`), one required AKAZE confirmation step, and one manual dedup interface.

Do not port the current Go cache-maintenance and derivative-relocation machinery.

Keep `initcache`, but only as in-place adoption of an existing target repository. Do not let it grow back into a separate cache-repair subsystem.

## Sources
Current Rust crate information checked on 2026-04-25:

- `clap 4.5.60`: https://docs.rs/crate/clap/latest
- `tokio 1.51.1`: https://docs.rs/crate/tokio
- `axum 0.8.9`: https://docs.rs/crate/axum/latest
- `tower-http 0.6.8`: https://docs.rs/crate/tower-http/latest
- `rusqlite 0.39.0`: https://docs.rs/crate/rusqlite/latest
- `serde 1.0.228`: https://docs.rs/crate/serde/latest
- `tracing-subscriber 0.3.23`: https://docs.rs/crate/tracing-subscriber/latest
- `walkdir 2.5.0`: https://docs.rs/crate/walkdir/latest
- `rayon 1.12.0`: https://docs.rs/crate/rayon/latest
- `kamadak-exif 0.6.1`: https://docs.rs/crate/kamadak-exif/latest
- `infer 0.19.0`: https://docs.rs/infer
- `image 0.25.10`: https://docs.rs/crate/image/latest
- `img_hash 3.2.0`: https://docs.rs/crate/img_hash/latest
- `akaze 0.7.0`: https://docs.rs/crate/akaze/latest
- `mp4parse 0.17.0`: https://docs.rs/mp4parse
- `matroska 0.30.0`: https://docs.rs/matroska/latest/matroska/
- `blake3 1.8.4`: https://docs.rs/crate/blake3/latest
- `murmur3 0.5.2`: https://docs.rs/murmur3
- `opencv` features2d reference for AKAZE/ORB matcher ecosystem comparison: https://docs.rs/opencv/latest/opencv/features2d/
