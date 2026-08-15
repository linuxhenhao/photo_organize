# photo-org

`photo-org` is the Rust rewrite of the local-first photo organizer workflow.

It provides one binary with four commands:

- `scan`: discover source files and write source-side facts into a scan database
- `import`: scan sources, copy canonical files into the target library, skip exact duplicates, and optionally create visual duplicate groups in `catalog.db`
- `initcache`: adopt an existing target library directly into `catalog.db` without a target-side scan database
- `serve`: run the local duplicate-resolution web UI

## Current Scope

This crate owns:

- the `photo-org` CLI
- the SQLite schema for `catalog.db` and source scan databases
- parallel source scanning with hashes, pHash, AKAZE, dimensions, and MIME detection
- target import into `DEST/YYYY/MM/DD/...`
- target adoption via `initcache`
- the local web UI for resolving duplicate groups
- RAW preview extraction in Rust so RAW files can participate in scan/import grouping
- persisted feature caching in `catalog.db.feature_cache`

## Highlights

- Rust-only main workflow: no Go binaries, no OpenCV build, no `photo-web-ui`
- Local-first storage: state lives in SQLite plus the target directory
- Parallel scanning and feature extraction with progress logging
- Exact duplicate detection using content hashes
- Cheap in-memory pHash candidate filtering backed by persisted 64-bit pHash values
- Second-stage AKAZE matching with content-based feature caching
- `initcache` reuses prior `target_items` facts when size and stored mtime still match
- Mobile-friendly and desktop-friendly local review UI at `serve`

## Build

Debug build:

```bash
cargo build
```

Release build:

```bash
cargo build --release
```

For the Bookworm-targeted Docker release build used by this repo:

```bash
./container_build.sh
```

That script builds inside the `photo-org-build:bookworm` image and mounts host Cargo caches to avoid repeated dependency downloads.

## Requirements

Core runtime requirements:

- Rust toolchain with Cargo
- a filesystem the process can read from and write to

Not required for the main workflow:

- Go
- OpenCV
- `exiftool`

Notes:

- SQLite is bundled through `rusqlite`, so you do not need a system SQLite development package to build this crate.
- The integration script uses tools such as `sqlite3`, `curl`, and `python3`.

## Command Usage

Top-level help:

```bash
cargo run -- --help
```

### 1. Scan

Scan source directories into a source-side scan database.

```bash
cargo run -- scan \
  --scan-db /path/to/import-scan.db \
  --src /photos/inbox \
  --src /photos/cards
```

This writes file discovery facts into `source_items` and AKAZE features into `feature_cache` in the scan database.

### 2. Import

Import canonical files into the target library. By default `import` only skips exact duplicates (same content hash) and does not create visual near-duplicate groups. It still copies AKAZE features from the scan database into `catalog.db.feature_cache`. If a file that will actually be imported is missing AKAZE in the scan cache, `import` backfills it. It does not recompute AKAZE for a hash already present in the target catalog.

Use an existing scan database:

```bash
cargo run -- import \
  --db /path/to/catalog.db \
  --scan-db /path/to/import-scan.db \
  --dest /path/to/library
```

Or let `import` run `scan` first:

```bash
cargo run -- import \
  --db /path/to/catalog.db \
  --src /photos/inbox \
  --src /photos/cards \
  --dest /path/to/library
```

Enable pHash/AKAZE visual grouping with `--visual-dedup`:

```bash
cargo run -- import \
  --db /path/to/catalog.db \
  --src /photos/inbox \
  --dest /path/to/library \
  --visual-dedup
```

`import` requires `--db`, `--dest`, and at least one of `--src` or `--scan-db`.

When `--src` is provided and `--scan-db` is omitted, `import` uses:

```text
DEST/.photo-org/import-scan.db
```

Tuning flags:

```text
--visual-dedup         opt-in visual near-duplicate grouping (off by default)
--phash-threshold      default 14
--akaze-min-matches    default 10
```

### 3. Initcache

Adopt an existing target library into `catalog.db`.

```bash
cargo run -- initcache \
  --db /path/to/catalog.db \
  --dest /path/to/library
```

`initcache` works in three stages:

1. ingest target files into `target_items`
2. pre-warm missing visual features
3. group pending candidates serially

Important behavior:

- it scans target-file facts directly into `catalog.db.target_items`
- `target_items.target_path` is always stored as a logical path rooted at the final `--dest` directory name
  for example, both `--dest repo` and `--dest /root/a/b/repo` store paths like `repo/2025/01/02/file.jpg`
- it does not depend on a persistent target-side `source_items` database
- repeated runs reuse prior facts when file size and stored mtime are unchanged

Optional profiling summary:

```bash
PHOTO_ORG_PROFILE=1 cargo run -- initcache --db /path/to/catalog.db --dest /path/to/library
```

`PHOTO_ORG_PROFILE` is the current profiling env toggle used by the code. Setting it to any value
enables the `initcache profile summary` log for `initcache`.

### 4. Serve

Run the local review UI for unresolved duplicate groups.

```bash
cargo run -- serve \
  --db /path/to/catalog.db \
  --dest /path/to/library \
  --host 127.0.0.1 \
  --port 8080
```

Then open:

```text
http://127.0.0.1:8080/
```

The UI serves group pages from `catalog.db`, supports page navigation with `page_index` and `page_size`, and lets you confirm keep/reject/primary decisions locally.

For profiling trash-delete batches in `serve`, run with the same env var:

```bash
PHOTO_ORG_PROFILE=1 cargo run -- serve --db /path/to/catalog.db --dest /path/to/library
```

## Databases

### `catalog.db`

Main target-side database. Important tables include:

- `target_items`: current files in the target library plus group and keep state
  `target_path` uses a logical `DEST_BASENAME/...` form instead of an absolute filesystem path
- `feature_cache`: persisted AKAZE cache keyed by content hash and size; filled by `import` from the scan DB and by `initcache`
- `operations_log`: audit log for review actions

### Scan database

Used by `scan` and optionally by `import`.

- stores source-side discovery facts in `source_items`
- stores AKAZE features in `feature_cache` with the same content-hash key as the catalog
- typically named `import-scan.db`, but the path is user-controlled

## Logging

The binary uses `tracing`.

- default behavior enables `warn` globally and `photo_org=info`
- you can override with `RUST_LOG`
- set `PHOTO_ORG_PROFILE=1` to enable additional profiling logs in the implemented paths
  `initcache` logs `initcache profile summary`
  `serve` logs `serve delete_trash profile`

Example:

```bash
RUST_LOG=photo_org=debug cargo run -- scan --scan-db /tmp/scan.db --src /photos/inbox
```

## Development

Run tests:

```bash
cargo test
```

Integration-style smoke test:

```bash
./integration_test.sh
```

The expensive full-tree `initcache` regression is intentionally ignored by default in `tests/initcache_full_test_data.rs`; run it explicitly when changing import/initcache candidate selection or target adoption behavior.

## License

MIT
