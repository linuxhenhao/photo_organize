# Repository Guidelines

## Project Map
`photo_organize` is a Go photo manager built around two SQLite databases:
`photos.db` stores scan results for source media, and `cache.db` stores target
directory cache state plus visual feature cache rows.

Top-level binaries:
`cmd/photo-organizer/` is the main CLI for `scan`, `import`, `initcache`,
`cleangroups`, `precompute`, and `convertdb`.
`cmd/photo-web-ui/` serves the duplicate-resolution UI.
`cmd/xattr-probe/` is a small diagnostic tool for xattrs and thumbnail lookup.

Core packages:
`internal/scanner` walks source trees and populates `photos.db`.
`internal/metadata` extracts timestamps, MIME type, dimensions, and birth time fallbacks.
`internal/exiftool` wraps the shared `exiftool` worker pool.
`internal/hasher` owns exact MMH3 hashing, dHash, legacy pHash aliases, color signatures, and BK-tree candidate search.
`internal/dedupe` classifies directional thumbnail/master relationships.
`internal/vision` contains OpenCV-backed ORB verification and serialization.
`internal/importer` copies grouped files into the target tree.
`internal/target` manages `cache.db`, target indexing, initcache, cleanup, and thumbnail relocation.
`internal/precompute` persists second-stage visual features into `visual_feature_cache`.
`internal/web` serves the duplicate-resolution backend and static UI.
`internal/db` and `internal/migrate` own schema setup, backups, and SQLite migrations.
`internal/fsutil` contains reusable filesystem helpers.
`internal/models` is reserved for shared types if they become necessary.
`photo-org-rs/` is the Rust rewrite workspace that implements the new `photo-org` binary, with its own SQLite schema, scanner, importer, `initcache`, and local web server. In that crate, source-side scan state stays in separate scan DBs, while `initcache` adopts target files directly into `catalog.db` instead of maintaining a persistent target-side scan DB.

Static assets live in `internal/web/static/`. Fixtures and generated samples live in `test_data/`. Helper scripts are under `scripts/`.

Each `internal/<module>/` directory has its own `AGENTS.md`. Read this file first, then the nearest module guide before editing code in that package. When a change spans multiple packages, follow the most specific guide for each edited directory.
The Rust crate under `photo-org-rs/` follows its own `photo-org-rs/AGENTS.md`.

## Data Flow
Scan writes source metadata into `photos.db`, then updates `mmh3_hash`, first-stage `dhash`, and `group_id`.
Import reads grouped rows from `photos.db`, copies the canonical file into `YYYY/MM/DD/`, and uses `cache.db` plus the precompute resolver to keep exact and visual duplicate handling consistent.
`initcache` rebuilds `cache.db` from the target tree, can backfill thumbnail MMH3 values, and can move confirmed derivative files into `thumbnails/`.
`precompute` persists second-stage features keyed by `mmh3_hash` so `cleangroups` and other cleanup paths can avoid repeated decoding and OpenCV work.

Exact duplicates use MMH3 from `internal/hasher.CalculateHash`.
Thumbnail and derivative confirmation uses first-stage dHash in `file_cache.dhash`, then color signature and ORB when needed.
The old `phash` name is a compatibility alias only.

## Build, Test, and Development Commands
Use Go 1.24.1+ and keep `exiftool` on your `PATH`.

```bash
go build -o photo-organizer ./cmd/photo-organizer
go build -o photo-web-ui ./cmd/photo-web-ui
./build.sh
docker build -f Dockerfile.bookworm-gocv -t photo-organize-bookworm-gocv:latest .
go test ./...
./integration_test.sh
./photo-organizer precompute -dest /path/to/repo
./photo-organizer convertdb -dest /path/to/repo
go run ./scripts/gen_test_images/main.go -output-root /tmp/photo-fixtures
```

`go build` produces local binaries. `./build.sh` writes `output/photo_organize`. `go test ./...` covers unit tests across `internal/...`. `./integration_test.sh` builds the app, generates fixtures, and validates scan/import/initcache flows end to end. `precompute` populates `cache.db` with persisted visual features. `convertdb` renames legacy `phash` columns to `dhash` and creates backups.
The Rust rewrite is built and tested from `photo-org-rs/` with `cargo build` and `cargo test`. For Bookworm-targeted Rust builds inside Docker, use `photo-org-rs/container_build.sh` with the `photo-organize-bookworm-gocv:latest` image; it mounts the host Cargo registry and git caches to avoid re-downloading crates.

## Coding Style & Naming Conventions
Follow standard Go formatting: run `gofmt` on every changed file and keep imports organized by `go fmt`. Use tabs for indentation in Go files. Prefer short, package-scoped names that match existing patterns (`cache_manager.go`, `clean_groups.go`, `server_test.go`). Keep new commands under `cmd/<tool-name>/` and new internal packages focused on one responsibility.

For frontend assets in `internal/web/static/`, keep JavaScript and CSS changes small and aligned with the existing plain static-file approach.

## Testing Guidelines
Unit tests sit next to implementation files and use the `*_test.go` naming convention. This repo uses Go’s `testing` package plus `github.com/stretchr/testify/require` and `assert`. Add targeted unit tests for new package behavior and update `test_data/` only when fixture coverage needs to change. Run `go test ./...` before opening a PR; run `./integration_test.sh` for changes affecting scanning, importing, caching, cleanup, or web duplicate handling.

## Commit & Pull Request Guidelines
Recent history favors short, imperative subjects, often with prefixes like `feat:` and `fix:`. Follow that style when practical, for example: `fix: preserve thumbnail links during initcache`. Keep commits focused on one change. PRs should explain user-visible behavior, list validation steps, and call out any fixture, schema, or CLI flag changes. Include screenshots only for web UI updates.

## Configuration & Data Hygiene
SQLite databases, generated tar files, and local outputs appear in the repo root during development; do not treat them as source. Use temporary paths or `output/` for generated artifacts, and avoid editing committed fixture data unless the test intent changes.

## Observability
Keep new code observable by default. Emit logs for long-running work, state transitions, recovery paths, and user-visible failures, and include enough context to identify the affected file, group, request, or destination path. Follow existing patterns such as `resolve_id` in web flows and summary/report logging in cleanup and import paths.

Prefer structured or consistently formatted log lines over ad hoc prints. Return errors with actionable context instead of bare wrapped failures, and preserve identifiers that let operators correlate logs across steps. When adding background workers, batch jobs, or multi-step mutations, make sure success, skip, retry, and rollback outcomes are visible in logs or explicit reports.

## AGENTS.md Maintenance
Keep every `AGENTS.md` aligned with the current codebase at all times. Any change to behavior, module boundaries, commands, tests, or workflow must include the matching guide updates in the same change. Before finishing work, review the repository-level guide and the nearest module guide for each edited directory and update them if they no longer match the code.
