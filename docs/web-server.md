# Web Server Notes

This document describes how `photo-org serve` works today in `src/serve.rs`.

## Purpose

`serve` is the local review surface for unresolved duplicate groups in `catalog.db`.

It is responsible for:

- listing groups that still have at least one `keep_state = 'undecided'` member
- rendering the embedded HTML/CSS/JS review UI
- applying keep/reject/primary decisions back into `target_items`
- moving rejected files into a local trash area under `DEST/.photo-org/trash/`
- serving browser previews for target files, including RAW-backed previews

It is not responsible for:

- duplicate detection or grouping itself
- long-term preview caching
- maintaining a target-side scan DB

## Runtime Shape

`serve` starts an `axum` router on the requested `--host` and `--port`, then shuts down when the shared interrupt future resolves.

Core runtime pieces:

- `AppState`: carries `db_path` and `dest`
- `open_catalog_db()`: opens SQLite with WAL and `busy_timeout`
- embedded frontend: `index()` returns one HTML page with inline CSS and JS
- image delivery: `/image` serves either a direct file, a UGOS thumbnail, or an on-demand resized JPEG preview

The server is intentionally local-first and self-contained: there is no separate frontend build step and no remote service dependency.

## Route Map

Current routes:

- `GET /`
  Returns the embedded review page. The page accepts `page_index` and `page_size` query params and embeds the normalized initial paging values into the frontend bootstrap script.
- `GET /api/groups`
  Returns paged unresolved groups as JSON.
- `POST /api/groups/{id}/resolve`
  Resolves one group.
- `POST /api/groups/resolve_bulk`
  Resolves every group on the current page in one transaction.
- `GET /api/groups/{id}/archive`
  Returns the raw member list for one group. Today this is a read-only JSON view, not a write path.
- `POST /api/groups/{group_id}/members/{member_id}/delete_trash`
  Permanently deletes one member file that is already under `.photo-org/trash/` and removes its row from `target_items`.
- `GET /image`
  Returns a preview for one target file path.

## Group Listing Behavior

`/api/groups` only returns groups that still need operator review.

That means:

- `group_id` must be non-null
- at least one member in the group must still be `keep_state = 'undecided'`
- groups are ordered by `group_id`
- pagination uses `page_index` and `page_size`
- legacy `page` and `limit` query params are still normalized for compatibility

Returned member fields are intentionally lightweight:

- row id
- `target_path`
- MIME type
- keep state
- primary flag
- exact hash
- pHash
- width and height
- size in bytes

The frontend derives temporary UI state from those fields rather than writing partial decisions back to SQLite on each click.

## Frontend Flow

The page at `/` is a single embedded document.

Frontend behavior:

- fetches groups from `/api/groups`
- keeps transient `ui_keep` and `ui_primary` flags in browser memory
- lets the operator change `page_index` and `page_size`
- supports explicit `Keep`, `Reject`, `Primary`, and `Preview` actions per member
- supports single-group confirm and bulk confirm for all visible groups

Current UX expectations:

- image tap/click opens preview rather than toggling keep/reject state
- controls are touch-sized and responsive for both desktop and mobile
- URL query params are kept in sync with the current page state

## Resolve Semantics

Both resolve endpoints mutate `target_items` and append an `operations_log` entry.

### `POST /api/groups/{id}/resolve`

Single-group resolve performs validation before writing:

- the group must exist
- `kept` and `rejected` must not overlap
- every referenced path must belong to the requested group
- `primary`, if present, must belong to the group
- `primary`, if present, must also be included in `kept`

Rejected files are moved before commit into:

```text
DEST/.photo-org/trash/group-<group_id>/
```

If the SQLite transaction commit fails after files were moved, the handler attempts to rename the files back to their original paths.

### `POST /api/groups/resolve_bulk`

Bulk resolve applies multiple group decisions inside one transaction. It also moves rejected files into the trash tree and updates `target_path` rows to the moved location.

Current caveat:

- bulk resolve is less defensive than single-group resolve and assumes the client sent coherent per-group decisions

### `POST /api/groups/{group_id}/members/{member_id}/delete_trash`

Trash-member deletion is intentionally narrower than resolve:

- the row must belong to the requested group
- the stored `target_path` must already point under `.photo-org/trash/`
- the file is permanently removed from disk if it still exists
- the `target_items` row is deleted
- if that leaves only one member in the group, the survivor is de-grouped by clearing `group_id` and `is_group_primary`

This path is for manual cleanup of already-rejected trash files, not for normal duplicate resolution.

## Filesystem Safety Rules

The web server must stay local-first and explicit about mutation.

Important safeguards:

- all preview and mutation paths are checked with `ensure_under_root()` against `--dest`
- rejected files are renamed into a trash subtree instead of being deleted
- DB rows are updated to reflect the new trash path after a move
- database mutations are visible in handler code rather than hidden behind a large abstraction layer

When changing serve behavior, preserve those properties.

## Image Serving Path

`GET /image` has three tiers:

1. If UGOS thumbnail support is available and extended attributes point to a usable thumbnail, serve that file directly.
2. For large-size requests, if the original file is browser-safe and smaller than about 5 MiB, serve the original bytes directly.
3. Otherwise decode on demand and return a resized JPEG preview.

The fallback decode path:

- tries standard image decode first
- if that fails, tries to open the file as RAW with `rsraw`
- extracts an embedded JPEG preview from the RAW file
- resizes to the requested thumbnail bound
- returns a JPEG response

There is no persistent preview cache in the current Rust server.

## Data Dependencies

`serve` depends directly on:

- `catalog.db.target_items`
- `catalog.db.operations_log`

It does not currently need to load second-stage feature rows to render or resolve groups.

## Operational Notes

- Startup log includes the bound address and whether UGOS mode was detected.
- Shutdown is graceful through the shared interrupt path.
- `page_size` is clamped server-side to the configured max.
- Empty pages are normalized so out-of-range `page_index` values fall back to the highest valid page.

## Change Rules For `serve`

When modifying the web server, preserve these constraints:

- keep the command surface limited to `serve` inside the main `photo-org` binary
- keep path validation obvious in handler code
- keep DB writes explicit and auditable through `operations_log`
- do not make target adoption correctness depend on any extra target-side scan DB
- avoid introducing a heavyweight frontend toolchain unless there is a strong reason
- prefer cheap list queries and on-demand preview generation over hidden background maintenance

## Tests

Current tests in `src/serve.rs` cover:

- paging normalization and HTML bootstrap
- resolve behavior and path validation
- graceful shutdown on explicit shutdown future
- graceful shutdown on shared interrupt

If you change route behavior, request validation, or preview semantics, extend those tests nearby.
