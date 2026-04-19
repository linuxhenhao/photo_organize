# Repository Guidelines

## Project Structure & Module Organization
`cmd/photo-organizer/` contains the main CLI entrypoint. `cmd/xattr-probe/` is a small utility command. Core packages live under `internal/`, grouped by responsibility such as `scanner`, `metadata`, `importer`, `target`, `precompute`, `web`, and `hasher`. Static assets for the web UI are in `internal/web/static/`. Test fixtures and sample outputs live in `test_data/`. Helper scripts, including fixture generation, are under `scripts/`.

Each `internal/<module>/` directory also has its own `AGENTS.md`. Read this repository-level guide first, then read the nearest module guide before changing code in that package. When a task spans multiple modules, follow the most specific guide for each edited directory.

## Build, Test, and Development Commands
Use Go 1.24.1+ and keep `exiftool` on your `PATH`.

```bash
go build -o photo-organizer ./cmd/photo-organizer
./build.sh
go test ./...
./integration_test.sh
./photo-organizer precompute -dest /path/to/repo
go run ./scripts/gen_test_images/main.go -output-root /tmp/photo-fixtures
```

`go build` produces a local binary. `./build.sh` writes the binary to `output/photo_organize`. `go test ./...` runs unit tests across `internal/...`. `./integration_test.sh` builds the app, generates fixtures, and validates `scan`, `import`, and `initcache` end to end. `precompute` populates `cache.db` with persisted visual features for faster cleanup workflows.

## Coding Style & Naming Conventions
Follow standard Go formatting: run `gofmt` on every changed file and keep imports organized by `go fmt`. Use tabs for indentation in Go files. Prefer short, package-scoped names that match existing patterns (`cache_manager.go`, `clean_groups.go`, `server_test.go`). Keep new commands under `cmd/<tool-name>/` and new internal packages focused on one responsibility.

For frontend assets in `internal/web/static/`, keep JavaScript and CSS changes small and aligned with the existing plain static-file approach.

## Testing Guidelines
Unit tests sit next to implementation files and use the `*_test.go` naming convention. This repo uses Go’s `testing` package plus `github.com/stretchr/testify/require` and `assert`. Add targeted unit tests for new package behavior and update `test_data/` only when fixture coverage needs to change. Run `go test ./...` before opening a PR; run `./integration_test.sh` for changes affecting scanning, importing, caching, or web duplicate handling.

## Commit & Pull Request Guidelines
Recent history favors short, imperative subjects, often with prefixes like `feat:` and `fix:`. Follow that style when practical, for example: `fix: preserve thumbnail links during initcache`. Keep commits focused on one change. PRs should explain user-visible behavior, list validation steps, and call out any fixture, schema, or CLI flag changes. Include screenshots only for web UI updates.

## Configuration & Data Hygiene
SQLite databases, generated tar files, and local outputs appear in the repo root during development; do not treat them as source. Use temporary paths or `output/` for generated artifacts, and avoid editing committed fixture data unless the test intent changes.

## Observability
Keep new code observable by default. Emit logs for long-running work, state transitions, recovery paths, and user-visible failures, and include enough context to identify the affected file, group, request, or destination path. Follow existing patterns such as `resolve_id` in web flows and summary/report logging in cleanup and import paths.

Prefer structured or consistently formatted log lines over ad hoc prints. Return errors with actionable context instead of bare wrapped failures, and preserve identifiers that let operators correlate logs across steps. When adding background workers, batch jobs, or multi-step mutations, make sure success, skip, retry, and rollback outcomes are visible in logs or explicit reports.

## Cache Layers
Keep the staged visual-matching model explicit. `file_cache.dhash` is the first-stage `dHash` used broadly for candidate lookup across all cached files. Heavier second-stage features such as full perception hash, color signature, and ORB exist only to confirm thumbnail or derivative relationships and should remain conceptually separate from the base `dhash` field.

## AGENTS.md Maintenance
Keep every `AGENTS.md` aligned with the current codebase at all times. Any change to behavior, module boundaries, commands, tests, or workflow must include the matching guide updates in the same change. Before finishing work, review the repository-level guide and the nearest module guide for each edited directory and update them if they no longer match the code.
