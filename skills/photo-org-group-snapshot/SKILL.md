---
name: photo-org-group-snapshot
description: Use this skill when the user wants to inspect a remote `photo-org` duplicate group by `group_id`, fetch `target_items` metadata plus matching `feature_cache` rows, and download the group's image files locally for further analysis. Best for the `nas-photo` / `repo.db` workflow used by the photo organizer project.
---

# Photo Org Group Snapshot

This skill fetches a remote `photo-org` group snapshot over SSH and stores both metadata and files locally.

It is the right tool when the user asks to:

- inspect a specific `group_id`
- fetch `exact_hash`, `size_bytes`, `meta_json`, pHash, and AKAZE cache data
- download the corresponding image files for local visual analysis
- create a portable local snapshot for later SQL or JSON inspection

## Quick Start

Run the bundled script:

```bash
~/.codex/skills/photo-org-group-snapshot/scripts/fetch_group_snapshot.py <group_id> --out-dir <output-dir>
```

Example:

```bash
~/.codex/skills/photo-org-group-snapshot/scripts/fetch_group_snapshot.py 71 --out-dir downloads
```

This writes:

- `downloads/group-71/group.json`
- `downloads/group-71/group.sqlite`
- `downloads/group-71/manifest.txt`
- `downloads/group-71/files/...`

## Default Behavior

The script defaults to:

- SSH host: `nas-photo`
- remote DB: `/volume3/DocsAndMedia/Multimedia/repo/repo.db`
- remote media root: `/volume3/DocsAndMedia/Multimedia`

It fetches:

- `target_items` rows for the given `group_id`
- matching `feature_cache` rows joined by `(exact_hash, size_bytes)`
- matching `operations_log` rows that mention the same `group_id`
- remote item files into a mirrored local `files/` tree

It also records, per item:

- `resolved_remote_path`
- `remote_file_exists`
- `resolved_remote_size_bytes`
- `downloaded_local_path`

## Common Options

Use these when the defaults are wrong:

```bash
~/.codex/skills/photo-org-group-snapshot/scripts/fetch_group_snapshot.py \
  <group_id> \
  --host <ssh-host> \
  --db-path <remote-db> \
  --media-root <remote-media-root> \
  --out-dir <output-dir>
```

Useful flags:

- `--skip-images`: fetch metadata only
- `--skip-operations-log`: skip `operations_log` lookup

## Workflow

1. Choose a stable output directory.
2. Run the script for the target `group_id`.
3. Inspect `group.json` for quick analysis.
4. Use `group.sqlite` for ad hoc SQL queries.
5. Open files under `files/` for visual confirmation.

For project investigations, prefer an output directory under the repo such as `docs/<investigation>/snapshot/` or `downloads/`.

## Notes

- The script includes a fallback for files moved into `repo/.photo-org/trash/group-<id>/`.
- If the user wants repeatable investigation artifacts checked into the repo, fetch into a repo-local directory instead of `/tmp`.
- If the user also wants a narrative write-up, use the downloaded files and JSON snapshot as the evidence source.

## Script

- `scripts/fetch_group_snapshot.py`: bundled executable used by this skill
