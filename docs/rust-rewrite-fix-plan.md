# Rust Rewrite Fix Plan

Date: 2026-04-25

## Purpose
This document turns the current Rust rewrite review findings into an execution plan for a junior engineer.

The source of truth for expected behavior is:

- [docs/rust-rewrite-design.md](/home/huangyu/workspace/gitrepo/photo_organize/docs/rust-rewrite-design.md)

The current implementation under review is:

- [photo-org-rs/src/import.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/import.rs)
- [photo-org-rs/src/scan.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/scan.rs)
- [photo-org-rs/src/features.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/features.rs)
- [photo-org-rs/src/serve.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/serve.rs)
- [photo-org-rs/src/db.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/db.rs)

## Goals
Fix these four correctness issues:

1. `initcache` must adopt files in place instead of copying them again.
2. visual duplicate grouping must work for scanned/imported images by carrying or recomputing AKAZE data correctly.
3. web group resolution must keep database state and on-disk state consistent.
4. non-image files must stay out of the visual duplicate pipeline.

## Non-Goals
- Do not redesign the CLI.
- Do not add new commands.
- Do not expand scope into preview caching or broad UI redesign.
- Do not touch vendored dependencies.

## Recommended Order
Implement the fixes in this order:

1. Restrict visual grouping to images only.
2. Fix the feature flow so image grouping can actually succeed.
3. Split `initcache` from `import` so adoption happens in place.
4. Make web resolution transactional and path-correct.
5. Add tests that lock the behavior down.

This order matters because steps 1 and 2 define the correct duplicate-matching behavior that `initcache` will reuse.

## Workstream 1: Keep Non-Images Out Of Visual Grouping

### Problem
`scan` currently writes a fallback `phash` even for non-images or failed image decodes. That leaks videos and unsupported files into candidate selection.

### Target Behavior
- Only decodable images should have `phash` and image dimensions.
- Videos and generic files should still get metadata and exact hashes.
- If an image cannot be decoded for visual analysis, leave `phash = ''` and `phash_bits = 0`.
- Candidate matching in `import` and `initcache` should only consider rows with a real image visual hash.

### Files To Edit
- [photo-org-rs/src/scan.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/scan.rs)
- [photo-org-rs/src/import.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/import.rs)
- [photo-org-rs/src/features.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/features.rs) if helper APIs need cleanup

### Implementation Steps
1. Remove or stop using `fallback_phash` in scan for non-images and decode failures.
2. In `discover_file`, keep visual fields empty when:
   - MIME is not image/*
   - image decode fails
3. In candidate loading and matching code, treat empty `phash` as not eligible for visual grouping.
4. Make the grouping path explicitly guard on image MIME type, not only on `phash_bits`.
5. Verify that exact-duplicate import for videos still works.

### Tests To Add
- `scan` test: video or non-image file should produce `exact_hash` but empty `phash`.
- `import` test: a non-image row should import successfully and never get a `group_id`.
- `import` test: an undecodable image-like file should not crash import and should not enter a visual group.

### Acceptance Criteria
- No fallback visual hash is generated for non-images.
- Importing videos or generic files does not attempt AKAZE matching.
- Existing exact-duplicate behavior remains unchanged.

## Workstream 2: Make Visual Grouping Actually Work For Imported Images

### Problem
The current import path reads `phash` from the scan DB but drops AKAZE descriptors. Because `phash` is already present, it does not recompute image features for the newly imported file. As a result, AKAZE confirmation always fails and visual groups are never created.

### Target Behavior
- Imported images must have usable AKAZE descriptors during matching.
- Matching must use:
  - `pHash` to shortlist
  - AKAZE confirmation to accept
- Visual grouping should work for:
  - `import`
  - `initcache`

### Files To Edit
- [photo-org-rs/src/import.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/import.rs)
- [photo-org-rs/src/features.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/features.rs)
- [photo-org-rs/src/db.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/db.rs) if feature-cache reads/writes are added properly

### Implementation Steps
1. Decide the feature source for the imported file.
   Preferred first fix:
   - recompute full visual features from the copied target file before matching

   This is simpler and safer than trying to persist AKAZE from `scan`.
2. Replace the current `feature_from_scan_row` usage for image matching with full visual feature computation for the imported target file.
3. Keep scan DB rows lightweight:
   - `exact_hash`
   - `phash`
   - dimensions
   - no durable AKAZE persistence in scan DB
4. For existing target candidates, avoid unnecessary recomputation when possible.
   Minimum acceptable version:
   - recompute candidate features from disk during matching

   Better follow-up within this same change if manageable:
   - read/write `feature_cache` keyed by `exact_hash`
5. Ensure `save_feature_cache` stores meaningful values.
   Right now it always writes `NULL` for AKAZE fields. That should either:
   - be updated to store serialized data, or
   - be simplified so the table only stores fields actually used today
6. Re-check the group assignment logic after a match:
   - reuse existing `group_id`
   - mint a new one if needed
   - merge multiple groups transactionally
   - choose one primary row

### Design Constraint
Do not create visual groups without AKAZE confirmation. If AKAZE extraction fails for either side, the item must remain ungrouped.

### Tests To Add
- `import` test: two visually similar but not exact-identical images should end up with the same `group_id`.
- `import` test: exact duplicates should still import once and not create a visual group.
- `import` test: two visually different images with nearby dates should remain ungrouped.
- `import` test: if AKAZE extraction fails, import should finish and leave `group_id = NULL`.

### Acceptance Criteria
- At least one test proves visual grouping succeeds for imported images.
- The grouping path still refuses exact duplicates as visual groups.
- AKAZE is required for group creation.

## Workstream 3: Rebuild `initcache` As In-Place Adoption

### Problem
`initcache` currently runs a scan over `--dest` and then feeds the result through the normal import path, which copies files again into the target tree. That is the opposite of adoption.

### Target Behavior
- `initcache` must scan files already under `--dest`.
- It must upsert `target_items` for those exact existing paths.
- It must never copy or move files.
- It must assign visual groups using the same image-only `pHash + AKAZE` rules as import.
- It must not introduce a durable source-side `source_items` database for the target tree.

### Files To Edit
- [photo-org-rs/src/import.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/import.rs)
- [photo-org-rs/src/scan.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/scan.rs) only if shared scan helpers need to be exposed
- [photo-org-rs/src/db.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/db.rs)

### Implementation Strategy
Do not try to keep `initcache` as a thin alias for `import`.

Create a separate in-place adoption path, even if it shares helper functions with import.

### Implementation Steps
1. Introduce a target-side row type or helper that represents scanned files already under `--dest`.
2. Add a dedicated `initcache` pipeline:
   - walk `dest`
   - gather exact hash and image visual data
   - insert or update `target_items` with `target_path` equal to the existing file path
   - write durable state directly to `catalog.db`
3. Do not call:
   - `reserve_target_path`
   - `copy_to_target`
4. If scratch staging is useful for batching, keep it explicitly temporary and delete-safe.
5. Reuse group assignment helpers after the target row exists.
6. Handle existing `target_items` rows carefully:
   - if the exact path already exists in DB, update it
   - if DB rows point to files now missing from disk, mark them stale or otherwise record disappearance according to the chosen schema behavior
7. Make sure traversal still excludes:
   - hidden directories
   - `trash`
   - `.photo-org`
8. Keep write transactions short:
   - do feature extraction outside the write transaction
   - upsert and group assignment inside a short transaction

### Tests To Add
- `initcache` test: given a pre-populated destination tree, row paths in `target_items` must equal the original file paths.
- `initcache` test: file count under `dest` must not increase after running `initcache`.
- `initcache` test: visually similar existing target files should be grouped.
- `initcache` test: `.photo-org/trash` and hidden directories are excluded.

### Acceptance Criteria
- Running `initcache` does not create copied duplicates.
- Existing target paths are adopted as-is.
- Visual grouping during adoption works for images only.

## Workstream 4: Fix Web Resolution Consistency

### Problem
The resolve endpoint moves rejected files before the database transaction and never updates `target_items.target_path` to the new trash location. That leaves the database stale and makes failure recovery unsafe.

### Target Behavior
- All requested paths must be validated under `--dest`.
- Reject actions must move files into `DEST/.photo-org/trash/...`.
- The database must be updated to reflect the new path of moved files.
- Disk mutation and DB mutation must behave as one coherent operation as much as practical.

### Files To Edit
- [photo-org-rs/src/serve.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/serve.rs)
- [photo-org-rs/src/util.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/util.rs) if path helpers need improvement
- [photo-org-rs/src/db.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/db.rs) only if logging helpers need expansion

### Implementation Strategy
Because filesystem renames cannot be rolled back automatically by SQLite, use a staged approach:

1. validate
2. compute intended trash destinations
3. move files
4. update DB in one transaction
5. if DB update fails, log loudly and return an error with enough context

If you want a stronger implementation, add a best-effort rollback of moved files when the DB transaction fails. That is a good improvement, but not required for the first fix if it complicates the patch too much.

### Implementation Steps
1. Validate every path in `kept`, `rejected`, and `primary`:
   - path belongs to the selected group
   - path is under `--dest`
2. Reject malformed requests:
   - same file in both kept and rejected
   - primary path not in the group
3. Change `move_to_trash` so it returns the final trash path it chose.
4. For each rejected member:
   - compute final trash path
   - move file
   - store old path -> new path in memory
5. In the DB transaction:
   - update `keep_state`
   - update `is_group_primary`
   - update `target_path` for moved rows
   - write `operations_log`
6. Ensure one primary row remains only when the request asks for one valid primary.
7. Consider whether rejected rows should ever remain primary. They should not.
8. Return the updated paths in the response payload if useful for the frontend.

### Tests To Add
- `serve` test: rejected file is moved to trash and DB path is updated to the trash path.
- `serve` test: invalid primary path returns `400`.
- `serve` test: path outside `--dest` returns `400` or `500` only if the failure is truly internal.
- `serve` test: same file listed in both kept and rejected is rejected.

### Acceptance Criteria
- After resolution, every `target_items.target_path` points to a real current location.
- Rejected files are no longer left at stale paths in the catalog.
- Invalid resolve requests are rejected before any disk mutation.

## Workstream 5: Strengthen Tests And Review Coverage

### Problem
The existing tests are too narrow. They do not cover the current broken paths.

### Files To Edit
- [photo-org-rs/src/import.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/import.rs)
- [photo-org-rs/src/scan.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/scan.rs)
- [photo-org-rs/src/serve.rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs/src/serve.rs)
- add `tests/` files if that becomes cleaner

### Required Test Matrix
- `scan`
  - present file marked present
  - missing file marked missing
  - non-image has empty visual hash
- `import`
  - exact duplicates import once
  - visually similar images create a group
  - non-images never create a group
  - decode failure does not crash import
- `initcache`
  - adopts existing paths in place
  - does not duplicate files
  - groups similar images already in target
- `serve`
  - resolve updates disk and DB path together
  - invalid request rejected early
  - operations log row written

### Commands To Run
From [photo-org-rs](/home/huangyu/workspace/gitrepo/photo_organize/photo-org-rs):

```bash
cargo test
```

If you add integration-style tests that exercise multiple commands, keep them in Rust and make them part of `cargo test`.

## Suggested Breakdown Into Commits
Use small commits. Recommended split:

1. `fix: keep non-images out of visual grouping`
2. `fix: enable akaze-backed visual grouping during import`
3. `fix: adopt target files in place during initcache`
4. `fix: keep resolve trash moves and catalog paths consistent`
5. `test: cover initcache grouping and resolve edge cases`

## Review Checklist
Before asking for review, confirm all of these:

- `initcache` no longer calls the copy path
- imported image grouping can succeed in tests
- non-image imports do not enter AKAZE matching
- rejected files update `target_items.target_path`
- invalid resolve requests fail before renames
- `cargo test` passes

## Notes For The Engineer
- Prefer small helper extraction over large refactors.
- Keep DB writes explicit and readable.
- Do not hold write transactions open during image decode or hashing.
- If you must choose between performance and correctness in this patch set, choose correctness.
