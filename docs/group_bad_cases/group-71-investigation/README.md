# Group 71 Investigation

## Status

- State: problem documented, fix not implemented yet
- Scope: duplicate grouping behavior in `initcache` / `import`
- Focus group: remote catalog group `71`

## Objective

Record the bad merged group, keep the relevant image assets in one stable repo directory, and capture the concrete bridge edges that caused the wrong collapse.

## Local Assets

Downloaded investigation assets live under [`assets/`](./assets/):

- [`assets/cluster-0183.jpg`](./assets/cluster-0183.jpg)
- [`assets/cluster-1707.jpg`](./assets/cluster-1707.jpg)
- [`assets/cluster-0758-0759.jpg`](./assets/cluster-0758-0759.jpg)
- [`assets/bridge-1707-vs-0183.jpg`](./assets/bridge-1707-vs-0183.jpg)
- [`assets/bridge-1707-vs-0758.jpg`](./assets/bridge-1707-vs-0758.jpg)

Primary source files copied for this investigation:

- [`assets/1707.jpg`](./assets/1707.jpg)
- [`assets/default1707.jpg`](./assets/default1707.jpg)
- [`assets/IMG_0183.JPG`](./assets/IMG_0183.JPG)
- [`assets/defaultimg_0183-3.jpg`](./assets/defaultimg_0183-3.jpg)
- [`assets/IMG_20180103_125719_0183.JPG`](./assets/IMG_20180103_125719_0183.JPG)
- [`assets/20180103-125719-2.jpg`](./assets/20180103-125719-2.jpg)
- [`assets/20180103-125719-3.jpg`](./assets/20180103-125719-3.jpg)
- [`assets/20180103-125719-4.jpg`](./assets/20180103-125719-4.jpg)
- [`assets/DSC00758.ARW`](./assets/DSC00758.ARW)
- [`assets/DSC00758-preview.jpg`](./assets/DSC00758-preview.jpg)
- [`assets/DSC00758.JPG`](./assets/DSC00758.JPG)
- [`assets/DSC00758-1.JPG`](./assets/DSC00758-1.JPG)
- [`assets/defaultdsc00758.arw.jpg`](./assets/defaultdsc00758.arw.jpg)
- [`assets/DSC00759.ARW`](./assets/DSC00759.ARW)
- [`assets/DSC00759-preview.jpg`](./assets/DSC00759-preview.jpg)
- [`assets/DSC00759.JPG`](./assets/DSC00759.JPG)
- [`assets/DSC00759-1.JPG`](./assets/DSC00759-1.JPG)
- [`assets/defaultdsc00759.arw.jpg`](./assets/defaultdsc00759.arw.jpg)

## Problem Statement

Group `71` is a bad merged group.

It currently contains members from at least three distinct expected groups:

1. `0183 / 125719` should be one group.
2. `1707` should be one group.
3. `0758 / 0759` should be one group.

Instead, the catalog collapsed all of them into a single duplicate group.

## Expected Subgroups

### Subgroup A: `0183 / 125719`

Representative members:

- `repo/2018/01/03/IMG_20180103_125719_0183.JPG`
- `repo/2018/01/03/20180103-125719-2.jpg`
- `repo/2018/01/03/20180103-125719-3.jpg`
- `repo/2018/01/03/20180103-125719-4.jpg`
- `repo/2019/05/23/IMG_0183.JPG`
- `repo/2025/03/17/defaultimg_0183-3.jpg`

Visual summary:

- [`assets/cluster-0183.jpg`](./assets/cluster-0183.jpg)

These are all the same photographed screen/document scene at different sizes / derivatives.

### Subgroup B: `1707`

Representative members:

- `repo/2012/07/19/1707.jpg`
- `repo/2025/03/17/default1707.jpg`

Visual summary:

- [`assets/cluster-1707.jpg`](./assets/cluster-1707.jpg)

This is a foggy horizon / water scene.

### Subgroup C: `0758 / 0759`

Representative members:

- `repo/2023/06/11/DSC00758.ARW`
- `repo/2023/06/11/DSC00758.JPG`
- `repo/2023/06/11/DSC00758-1.JPG`
- `repo/2025/03/17/defaultdsc00758.arw`
- `repo/2023/06/11/DSC00759.ARW`
- `repo/2023/06/11/DSC00759.JPG`
- `repo/2023/06/11/DSC00759-1.JPG`
- `repo/2025/03/17/defaultdsc00759.arw`

Visual summary:

- [`assets/cluster-0758-0759.jpg`](./assets/cluster-0758-0759.jpg)

These are closely related harbor / shoreline shots and their derivatives. Per the user expectation, they belong together.

## Observed Facts

### Group 71 Members Used For Analysis

The investigated members were:

- `1707.jpg`
- `default1707.jpg`
- `IMG_20180103_125719_0183.JPG`
- `20180103-125719-2.jpg`
- `20180103-125719-3.jpg`
- `20180103-125719-4.jpg`
- `IMG_0183.JPG`
- `defaultimg_0183-3.jpg`
- `DSC00758.ARW`
- `DSC00758.JPG`
- `DSC00758-1.JPG`
- `defaultdsc00758.arw`
- `DSC00759.ARW`
- `DSC00759.JPG`
- `DSC00759-1.JPG`
- `defaultdsc00759.arw`

### Feature Cache State

All of the analyzed members in group `71` have:

- `feature_cache.akaze_status = no_keypoints`
- no stored AKAZE descriptors

This means the full merged group was formed entirely by the `NoKeypoints` pHash fallback path.

## Pairwise Distance Findings

### Within-Subgroup Distances

`0183 / 125719`

- distances are all very small: `0`, `1`, `2`, or `3`
- example: `20180103-125719-2.jpg <-> IMG_0183.JPG = 0`
- example: `IMG_20180103_125719_0183.JPG <-> IMG_0183.JPG = 1`

`1707`

- `1707.jpg <-> default1707.jpg = 14`

`0758 / 0759`

- same-file / RAW-JPEG pairs are `0`
- same-scene derivative distances are small: `1`, `2`, `3`, `4`
- cross `0758` / `0759` distances are still within threshold: around `10` to `13`

### Cross-Subgroup Bridge Edges

Only two cross-subgroup edges at or below the default threshold were needed to collapse everything:

1. `default1707.jpg <-> defaultimg_0183-3.jpg = 14`
2. `1707.jpg <-> DSC00758-1.JPG = 13`

These edges link:

- subgroup `1707` to subgroup `0183 / 125719`
- subgroup `1707` to subgroup `0758 / 0759`

Once those matches are accepted, the grouping logic merges the entire connected components into one group.

## Visual Interpretation

The bridge comparisons are clearly wrong:

- [`assets/bridge-1707-vs-0183.jpg`](./assets/bridge-1707-vs-0183.jpg)
- [`assets/bridge-1707-vs-0758.jpg`](./assets/bridge-1707-vs-0758.jpg)

Observed content:

- `1707` is a low-detail fog / water horizon image
- `0183 / 125719` is a photographed screen/document
- `0758 / 0759` is a harbor / sky scene

These scenes are visually distinct, but all are low-texture enough that they produced `no_keypoints`.

## Why Group 71 Forms Today

The current matcher behavior is enough to explain the collapse:

1. candidate search uses pHash distance
2. every analyzed member in this group lands in `AkazeStatus::NoKeypoints`
3. the fallback accepts a pair when both sides are `NoKeypoints` and pHash distance is within the global threshold
4. grouping then merges any touched groups transitively

That combination is the entire reason group `71` exists.

This is a stronger example than group `58` because group `71` shows not just a single false pair, but a false transitive merge of three otherwise coherent subgroups.

## Relevant Code Areas

- `src/features.rs`: `akaze_confirm()`
- `src/import.rs`: candidate confirmation and group merging
- `src/main.rs`: default `--phash-threshold`

## Constraints For A Fix

Any solution here should preserve:

- cheap in-memory pHash coarse filtering
- the current RAW preview path in Rust
- valid low-detail derivative grouping inside real subgroups

And it should prevent:

- low-detail bridge edges from merging unrelated subgroups
- transitive collapse from a handful of borderline pHash-only matches

## Reproduction Notes

The original catalog under investigation is:

- `/volume3/DocsAndMedia/Multimedia/repo/repo.db`

The durable local investigation record is this directory:

- [`docs/group_bad_cases/group-71-investigation/`](./)

## Open Questions

- Should the `NoKeypoints` fallback use a much stricter threshold than the global pHash candidate threshold?
- Should fallback acceptance consider aspect ratio and relative geometry before allowing a match?
- Should very small generated derivatives be treated differently from full-size images in the fallback?
- Should transitive group merging require stronger evidence when groups are joined only through borderline pHash-only edges?

## Reserved Solution

### Proposed Change

TBD

### Test Plan

TBD

### Risk Review

TBD

### Follow-up Work

TBD
