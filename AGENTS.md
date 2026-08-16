# Module Guidelines

This is the primary agent context file for the `photo-org` Rust project. It complements the detailed design rationale in `docs/rust-rewrite-design.md`.

## Scope
`photo-org` is the Rust rewrite workspace for the photo organizer.

It owns the new `photo-org` binary, the SQLite schema for `catalog.db` and scan databases, source scanning, target import, target adoption via `initcache`, and the local duplicate-resolution web server.
It also handles RAW preview extraction in pure Rust when possible so camera RAW files can participate in scan/import grouping without shelling out to `exiftool`.
`initcache` should scan target-file facts directly into `catalog.db.target_items`; target adoption must not depend on a persistent target-side `source_items` scan database such as `initcache-scan.db`.
Repeated `initcache` runs should reuse prior `target_items` facts when file `size_bytes` and stored `modified_at` are unchanged, and only fall back to re-reading file content and recomputing hashes when that fingerprint changes.
Long-running `scan`, `import`, and `initcache` paths should emit periodic progress logs with total, processed, and remaining counts so operators can tell that work is advancing.

For the full design rationale, crate selection, and data-model decisions, see `docs/rust-rewrite-design.md`.

Current internal structure also includes:
- `feature_loader`: loads second-stage AKAZE data through a cache-first path and persists it in `scan-db.feature_cache` and `catalog.db.feature_cache`.
- `phash_index`: keeps an in-memory BK-tree over persisted 64-bit pHash values so `import` and `initcache` only run AKAZE on threshold-matching candidates.
- `util`: MIME detection, EXIF date parsing, filename-safe normalization, path-safety checks, and the `ProgressReporter` used for periodic progress logs.
- `interrupt`: shared Ctrl+C signal handling, exposing `check()` for sync bail-out and `wait()` for async graceful shutdown.

## Command Workflows

- **`scan`**: Discovery phase. Walks source directories, extracts metadata (EXIF, hashes, pHash) and AKAZE features in parallel, upserts facts into `scan-db` (`source_items`), and persists AKAZE rows in `scan-db.feature_cache`. Re-scans skip AKAZE when a reusable cache row already exists for the same content hash and size, including renamed or copied files. `discover_file` (used by `initcache`) still computes RAW pHash from embedded previews.
- **`import`**: Transformation phase. Runs `scan` to refresh `scan-db`, selects one canonical per exact-hash group, copies new files to the target directory, and copies scan AKAZE rows into `catalog.db.feature_cache`. Missing AKAZE is backfilled only for hashes that are not already in `target_items`. Visual pHash/AKAZE grouping runs only when `--visual-dedup` is set.
- **`initcache`**: Adoption phase. 
    - **Stage 1 (Ingest)**: Parallel discovery of existing target files, reusing DB facts if file size/mtime match.
    - **Stage 2 (Pre-warm)**: Parallel computation of missing visual features for potential duplicates.
    - **Stage 3 (Grouping)**: Serial grouping and primary-member selection based on resolution and size.
- **`serve`**: Management phase. Provides a web interface for resolving 'undecided' groups and managing the catalog state.
  See `docs/web-server.md` for the current web-server route map, preview path, and state-mutation rules.

## Parallel Design & Safety

- **CPU-Bound Parallelism**: Uses `rayon` for recursive directory walking, file hashing, and expensive visual feature extraction (AKAZE).
- **Database Concurrency**: SQLite is configured with `WAL` mode and `busy_timeout`. To ensure write safety, the application often employs a **worker/consumer pattern** where parallel workers send results through a channel to a single-threaded consumer that performs batch DB writes.
- **Thread Safety**: Visual feature discovery is read-only and thread-safe. High-level grouping logic is typically executed serially to avoid complex locking on the `group_id` state.

## Storage Structure

- **`catalog.db`**: The central repository for the organized collection.
    - `target_items`: Current state of all files in the target directory, including `group_id`, `keep_state`, and `group_status`.
      - `target_path`: Store logical paths rooted at the final `--dest` directory name, not absolute filesystem paths.
        For example, both `--dest repo` and `--dest /root/a/b/repo` should persist `repo/2025/01/02/file.jpg`.
      - `group_status`: Flow-control column used by `initcache`. Starts as `'pending'` on ingest, then serially flipped to `'completed'` after grouping. The pre-warm and grouping passes only process rows where `group_status = 'pending'`.
      - `keep_state`: One of `'undecided'`, `'kept'`, or `'rejected'`. Set by `serve` resolution and used to filter unresolved groups.
      - `is_group_primary`: Marks the preferred representative in a duplicate group.
    - `feature_cache`: Content-based cache (exact hash + size) for AKAZE keypoints and descriptors to avoid redundant re-decoding. Includes `akaze_status`, `akaze_points`, and `feature_version` for schema migration. `import` copies scan rows in a transaction and does not replace a durable catalog row with a retryable scan row.
    - `operations_log`: Audit log of changes (imports, group resolutions).
- **`scan-db`**: A source-specific database (usually `import-scan.db`) used as a staging area during `scan` and `import`.
    - `source_items`: discovered source files and per-file facts (`exact_hash`, `pHash`, dimensions).
    - `feature_cache`: same AKAZE schema as `catalog.db`; written by `scan`, copied into the catalog by `import`.

## Change Rules
Keep the command set limited to `scan`, `import`, `initcache`, and `serve`.
Prefer small, explicit modules over a large monolith, and keep path validation and database mutation visible in code.

The rewrite should stay local-first and avoid reintroducing the removed Go maintenance commands.

**Every commit must keep documentation in sync with the code.** If behavior, CLI flags, workflows, schema, or matching rules change, update the same commit: `AGENTS.md`, `README.md`, `docs/rust-rewrite-design.md`, and `docs/e2e-test-plan.md` as applicable. Do not leave design or user docs describing the old path.

When touching duplicate matching:
- Keep pHash coarse filtering cheap and in-memory.
- Treat `exact_hash`, `pHash`, `phash_bits`, and image dimensions as base item attributes stored on `source_items` or `target_items`, not as second-stage feature-cache payload.
- Do not make callers care whether second-stage AKAZE data came from memory, SQLite, or a fresh decode; `feature_loader` owns that decision.
- Cache keys for persisted AKAZE data must be content-based, not path-based. Use file content hash plus size so renamed or re-adopted files can reuse the expensive second-stage work safely.
- Before editing any `target_items` upsert or regroup/adoption write path, read `docs/target-field-overwrite-risks.md`. `created_at` and `meta_json.fingerprint.modified_at` must be preserved unless the code is explicitly recomputing them; empty incoming values should not silently overwrite the stored value.

## Dup Improvements
When investigating or fixing duplicate-grouping mistakes, first capture a stable local snapshot of the remote group before changing matcher logic.

- Keep repo-maintained Codex skills under `skills/`; `skills/photo-org-group-snapshot/` is the canonical copy of the remote group snapshot workflow.
- Prefer using the Codex skill `$photo-org-group-snapshot` to fetch a `group_id` from `nas-photo`.
- That skill bundles the `fetch_group_snapshot.py` workflow and pulls `target_items`, matching `feature_cache` rows, `operations_log` context, and the related image files into one local directory.
- Keep investigation write-ups and downloaded assets together under a dedicated `docs/<group-or-topic>-investigation/` directory when the issue is important enough to preserve.
- For analysis, explicitly compare which pairs are valid within a subgroup and which borderline pHash-only edges act as bridges across unrelated subgroups.
- Treat transitive group collapse as a first-class failure mode: a small number of weak `no_keypoints` matches can merge otherwise coherent groups.

## RAW Performance
RAW preview extraction itself is cheap in this crate; the expensive work is decoding oversized embedded previews and running visual features on them.

Measured on committed fixtures in `../test_data`:
- `rsraw` open + `extract_thumbs()` for the ARW and CR2 fixtures together took about `0.16s`.
- ARW large embedded JPEG `7008x4672`: decode about `6.29s`, resize to `960` about `2.48s`.
- ARW medium embedded JPEG `1616x1080`: decode about `0.37s`, resize to `960` about `0.23s`.
- CR2 large embedded JPEG `5184x3456`: decode about `5.09s`, resize to `960` about `1.42s`.
- Extra downsize from `960` to `256` was only about `0.05s`, so the main cost is choosing and decoding the wrong preview, not maintaining separate pHash and AKAZE sizes.

When handling RAW files:
- Prefer the smallest embedded preview whose max dimension is at least the AKAZE target size.
- If no preview reaches the AKAZE target, use the largest available preview.
- Do not decode the largest full preview by default when a medium preview is available.
- Do not upscale tiny previews.
- Keep pHash and AKAZE target sizes separate when possible.

Current sizing guidance for this crate:
- `pHash`: target about `256px` max dimension.
- `AKAZE`: target about `640px` max dimension for better speed on current fixtures.
- If a single shared size is required, prefer something around `640px`; `256px` is too small for reliable AKAZE.

## Profiling
Set `PHOTO_ORG_PROFILE=1` to emit per-stage timing breakdowns for `initcache` runs. The profiling summary includes input feature calls, candidate loads, distance checks, AKAZE confirm calls, and DB transaction costs. The current codebase also uses the same env var for `serve` trash-delete profiling logs.

## Testing
Add focused Rust tests next to the implementation or under `tests/` when behavior changes.
Run `cargo test` for every code change in this crate.
Keep `docs/e2e-test-plan.md` in sync with the expected end-to-end regression coverage and use it as the baseline for behavior-sensitive changes.
Any code change that can affect CLI workflows, `serve`, path handling, duplicate grouping, file moves/deletes/restores, or catalog state must preserve this end-to-end suite and leave the `e2e-test-plan` scenarios passing.

For Bookworm-targeted release builds inside a container, use `./container_build.sh`. It prefers Podman and falls back to Docker (`CONTAINER_ENGINE` overrides the choice), uses the `photo-org-build:bookworm` image (building it automatically if missing), and mounts the host Cargo registry and git caches into the container so repeated builds do not re-download Rust dependencies.

The expensive full-tree `initcache` regression lives in `tests/initcache_full_test_data.rs` and is intentionally `#[ignore]`; run it explicitly when changing import/initcache candidate selection, feature caching, or target adoption behavior.

Integration smoke test: `./integration_test.sh`.
