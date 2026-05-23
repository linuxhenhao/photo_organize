# Group 58 Investigation

## Status

- State: problem documented, fix not implemented yet
- Scope: duplicate grouping behavior in `initcache` / `import`
- Focus group: remote catalog group `58`

## Objective

Document the bad grouping in a stable repo location before changing code, so the problem statement, evidence, constraints, and eventual fix stay together in one directory.

## Local Assets

Downloaded investigation assets live under [`assets/`](./assets/):

- [`assets/DSC02523.ARW`](./assets/DSC02523.ARW)
- [`assets/DSC02523-preview.jpg`](./assets/DSC02523-preview.jpg)
- [`assets/video-000000000000021e_1.jpg`](./assets/video-000000000000021e_1.jpg)
- [`assets/group58-side-by-side.jpg`](./assets/group58-side-by-side.jpg)
- [`assets/2021-11-14 15.48.55.jpg`](./assets/2021-11-14%2015.48.55.jpg)
- [`assets/IMG_0940.JPG`](./assets/IMG_0940.JPG)
- [`assets/2021-11-14 15.48.55.mov.jpg`](./assets/2021-11-14%2015.48.55.mov.jpg)

## Problem Statement

Group `58` is a false duplicate group.

The group currently contains:

- `repo/2025/06/22/DSC02523.ARW`
- `repo/2022/03/26/video-000000000000021e_1.jpg`

These files are not the same scene:

- `DSC02523.ARW` is a sky / contrail image.
- `video-000000000000021e_1.jpg` is a tiny cat thumbnail.

The grouping is therefore incorrect and should be prevented by the matcher.

## Observed Facts

### Group Members

`DSC02523.ARW`

- local copy: [`assets/DSC02523.ARW`](./assets/DSC02523.ARW)
- local preview: [`assets/DSC02523-preview.jpg`](./assets/DSC02523-preview.jpg)
- `id = 21965`
- `size_bytes = 34230272`
- DB dimensions: `1616x1080`
- `exact_hash = b37c031d3c903c2252bcc8ae43cf3125dae421c1f1eaaadc937bbedfda2826ca`
- `phash = VKWKGsBWNqw=`
- `feature_cache.akaze_status = no_keypoints`

`video-000000000000021e_1.jpg`

- local copy: [`assets/video-000000000000021e_1.jpg`](./assets/video-000000000000021e_1.jpg)
- `id = 101`
- `size_bytes = 5249`
- dimensions: `128x96`
- `exact_hash = 064a84adc64b6e225e24abdcab99aed8cfbc1720aaffe769163c6b3179ea1c2c`
- `phash = VI0rGsRmdTw=`
- `feature_cache.akaze_status = no_keypoints`

### Distance

- pHash Hamming distance between the two group members: `13`
- Current default `--phash-threshold`: `14`

### Visual Inspection

Local inspection showed:

- [`assets/DSC02523-preview.jpg`](./assets/DSC02523-preview.jpg) is a blue sky with a contrail
- [`assets/video-000000000000021e_1.jpg`](./assets/video-000000000000021e_1.jpg) is a cat thumbnail
- [`assets/group58-side-by-side.jpg`](./assets/group58-side-by-side.jpg) shows the mismatch directly

This is a confirmed false positive, not a subjective edge case.

## Current Code Path

The relevant matcher behavior is:

1. Candidates are prefiltered by pHash distance.
2. `akaze_confirm()` has a special fallback for the case where both sides are `AkazeStatus::NoKeypoints`.
3. In that fallback, the pair is accepted when pHash distance is within the configured threshold.

Relevant code locations:

- `src/features.rs`: `akaze_confirm()`
- `src/import.rs`: candidate loading and confirmation loop
- `src/main.rs`: default CLI thresholds

## Why Group 58 Forms Today

The bad group forms because both members fell into the `no_keypoints` bucket:

- the tiny `128x96` JPEG has no reusable AKAZE descriptors
- the ARW preview also produced `no_keypoints`

That allows the pair to bypass the normal AKAZE descriptor matching path and use the permissive pHash-only fallback instead.

Because the distance is `13` and the threshold is `14`, the pair is accepted.

## Important Secondary Finding

The tiny cat JPEG appears to have real visual neighbors elsewhere in the catalog:

- `repo/2021/11/14/2021-11-14 15.48.55.jpg`
- `repo/2021/11/14/IMG_0940.JPG`
- `repo/2021/11/14/2021-11-14 15.48.55.mov.jpg`

Local copies:

- [`assets/2021-11-14 15.48.55.jpg`](./assets/2021-11-14%2015.48.55.jpg)
- [`assets/IMG_0940.JPG`](./assets/IMG_0940.JPG)
- [`assets/2021-11-14 15.48.55.mov.jpg`](./assets/2021-11-14%2015.48.55.mov.jpg)

Those cat-related files are much more plausible matches for the tiny JPEG than the sky RAW.

However, they do not currently group through the normal path because their cached AKAZE data is sparse and does not satisfy the normal minimum-descriptor guard.

This means the current logic creates an asymmetry:

- plausible low-detail pairs can fail to group
- implausible low-detail pairs can still group if both land in `no_keypoints`

## Control Case

Group `59` was checked as a control case and is not the same kind of failure.

That group contains:

- a larger photographed document image
- a smaller `default...jpg` derivative of the same page

It grouped through the same broad fallback area, but the visual match itself appears correct.

This matters because the eventual fix should avoid breaking clearly valid derivative grouping while preventing the group `58` false positive.

## Constraints For A Fix

Any solution should preserve the existing architecture constraints in this crate:

- keep pHash coarse filtering cheap and in-memory
- keep `exact_hash`, `phash`, `phash_bits`, and dimensions as base item attributes
- keep `feature_loader` responsible for whether second-stage data comes from memory, SQLite, or a fresh decode
- avoid path-based cache keys for persisted AKAZE data

Behavioral constraints for this specific bug:

- prevent unrelated low-detail images from grouping only because both are `no_keypoints`
- avoid regressing valid derivative cases like document photos and default-sized copies
- keep RAW preview handling on the Rust path already used by the crate
- prefer a fix that is testable with focused Rust tests

## Reproduction Notes

The original investigation used the remote NAS catalog at:

- `/volume3/DocsAndMedia/Multimedia/repo/repo.db`
- downloaded files are now maintained in this directory under [`assets/`](./assets/)
- earlier `/tmp/photo_group58/` ad hoc files are no longer the canonical project record

## Open Questions

- Should the `no_keypoints` fallback use a stricter threshold than the general pHash candidate threshold?
- Should tiny thumbnails be excluded from the `no_keypoints` fallback path?
- Should the fallback require additional geometry checks such as aspect-ratio similarity?
- Should sparse-but-`ready` AKAZE cases have a different low-detail confirmation path so plausible thumbnail/original pairs do not lose to unrelated `no_keypoints` pairs?

## Reserved Solution

### Proposed Change

TBD

### Test Plan

TBD

### Risk Review

TBD

### Follow-up Work

TBD
