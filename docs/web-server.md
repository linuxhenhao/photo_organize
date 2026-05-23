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
  Returns the embedded review page. The page accepts `page_index`, `page_size`, optional `group_id`, and optional `view=trash` query params and embeds the normalized initial paging values into the frontend bootstrap script.
- `GET /api/groups`
  Returns paged groups as JSON for the requested review mode. Default mode is unresolved duplicate review; `view=trash` returns groups that currently have at least one member under `.photo-org/trash/`; `view=filename` returns virtual review groups built from trusted filename-derived matches; `view=filename_trash` returns virtual trash-review groups rebuilt from `.photo-org/trash/filename-group-*` files plus their matched kept-side filename candidates.
- `POST /api/groups/{id}/resolve`
  Resolves one group. This also accepts virtual negative ids for `filename` review groups.
- `POST /api/groups/resolve_bulk`
  Resolves every group on the current page in one transaction, including `filename` virtual groups.
- `POST /api/groups/{id}/delete_trash`
  Permanently deletes every trash member in one group and updates `target_items`.
- `POST /api/groups/delete_trash_bulk`
  Permanently deletes an explicit list of trash members, intended for page-level bulk cleanup in trash review mode.
- `POST /api/groups/{group_id}/members/{member_id}/restore_trash`
  Restores one trash member back into the managed library tree and flips it back to `kept`.
- `GET /api/groups/{id}/archive`
  Returns the raw member list for one group. Today this is a read-only JSON view, not a write path.
- `POST /api/groups/{group_id}/members/{member_id}/delete_trash`
  Permanently deletes one member file that is already under `.photo-org/trash/` and removes its row from `target_items`.
- `GET /image`
  Returns a preview for one target file path.

## Group Listing Behavior

`/api/groups` normally returns groups that still need operator review.

That means:

- `group_id` must be non-null
- at least one member in the group must still be `keep_state = 'undecided'`
- groups are ordered by `group_id`
- pagination uses `page_index` and `page_size`
- legacy `page` and `limit` query params are still normalized for compatibility

When `view=trash` is supplied:

- the listing contains groups with at least one member already moved under `.photo-org/trash/`
- the response marks those groups with status `trash-review`
- this is the review surface for confirming permanent deletion of trash files

When `view=filename` is supplied:

- the listing is built from `target_items` rows where `keep_state = 'undecided'`, `group_id IS NULL`, and the stored path is not already under `.photo-org/trash/`
- the candidate builder uses the repo-documented trusted filename families and merges rows through connected filename keys
- currently that includes the direct-resolvable families from the investigation notes: `default_camera`, `default_embedded`, `default_shotwell`, and `timestamp_rendition`
- examples include `defaultimg_5794-2.cr2`, `defaultimg_5808_cr2_embedded.jpg`, `1-defaultimg_1823_cr2_shotwell.jpg`, and `20191219-215605-3.jpg`
- extensions may differ
- the response uses virtual negative `group_id` values derived from the `default*` row id; these ids are only for the review UI and are not written into `target_items.group_id`
- filename-family and timestamp-rendition patterns are used only to build review groups; primary preselection is then chosen by the normal member ranking rules based on image dimensions and `size_bytes`

When `view=filename_trash` is supplied:

- the listing starts from rows already moved under `.photo-org/trash/filename-group-*`
- the server rebuilds a virtual group by matching those trash rows back to non-trash filename candidates using the same filename-family and timestamp-rendition logic
- the response uses a separate virtual negative `group_id` space for these rebuilt trash groups
- groups are shown only when at least one filename-trash row and at least one matching non-trash candidate are present
- primary highlighting still follows image dimensions plus `size_bytes`; the filename patterns are used only to rebuild the comparison group

When `group_id` is supplied:

- the response contains only that group
- in normal mode the group must still have at least one undecided member
- in trash review mode the group must still have at least one trash member

Returned member fields are intentionally lightweight:

- row id
- `target_path`
  Stored as a logical path rooted at the final `--dest` directory name, such as `repo/2025/01/02/file.jpg`, not as an absolute path.
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
- supports switching between pending review, filename review, filename trash review, and trash review
- exposes that mode switch directly on the root page header so operators do not need to know query params
- supports opening one specific group by `group_id` inside the active review mode
- keeps transient `ui_keep` and `ui_primary` flags in browser memory
- lets the operator change `page_index` and `page_size`
- lets the operator jump directly to one `group_id`
- supports explicit `Keep`, `Reject`, `Primary`, and `Preview` actions per member
- shows `Delete trash file` for members already under `.photo-org/trash/`
- shows `Restore` for trash members so operators can undo a prior reject decision
- supports single-group confirm and bulk confirm for all visible groups in pending mode
- supports single-group trash deletion and page-level trash deletion in trash review mode
- supports page-level trash deletion plus per-file restore/delete inside filename trash review mode

Current UX expectations:

- image tap/click opens preview rather than toggling keep/reject state
- controls are touch-sized and responsive for both desktop and mobile
- URL query params are kept in sync with the current page state

## Resolve Semantics

Both resolve endpoints mutate `target_items` and append an `operations_log` entry.

For `filename` virtual groups:

- the resolve path updates `keep_state` and `is_group_primary` on the participating rows
- rejected files are moved into `DEST/.photo-org/trash/filename-group-<default_row_id>/`
- the participating rows remain outside algorithmic duplicate groups; `group_id` stays null
- those moved rows can later be revisited under `view=filename_trash`, where the server rebuilds a virtual trash-review group from the trashed filename item and the matching kept-side candidates

### `POST /api/groups/{id}/resolve`

Single-group resolve performs validation before writing:

- the group must exist
- `kept` and `rejected` must not overlap
- every referenced path must belong to the requested group
- `primary`, if present, must belong to the group
- `primary`, if present, must also be included in `kept`
- `kept` and `primary` paths must still exist at their original managed-library location; a file that already only exists under `.photo-org/trash/` cannot be re-kept through this endpoint
- `rejected` paths are idempotent: they may still be at the original path or already be under the expected group trash directory

Rejected files are moved before commit into:

```text
DEST/.photo-org/trash/group-<group_id>/
```

If the SQLite transaction commit fails after files were moved, the handler attempts to rename the files back to their original paths.

### `POST /api/groups/resolve_bulk`

Bulk resolve applies multiple group decisions one group at a time. It also moves rejected files into the trash tree and updates `target_path` rows to the moved location.

Current behavior:

- each group is validated and committed independently
- one failing group does not block later groups in the same request
- the response returns HTTP 200 with `status = "partial"` when some groups failed, plus an `errors` array keyed by `group_id`
- `kept` and `primary` use the same strict source-path check as single-group resolve
- `rejected` remains idempotent and may already be in the expected trash location

### `POST /api/groups/{group_id}/members/{member_id}/delete_trash`

Trash-member deletion is intentionally narrower than resolve:

- the row must belong to the requested group
- the stored `target_path` must already point under `.photo-org/trash/`
- the file is permanently removed from disk if it still exists
- the `target_items` row is deleted
- if that leaves only one member in the group, the survivor is de-grouped by clearing `group_id` and `is_group_primary`

This path is for manual cleanup of already-rejected trash files, not for normal duplicate resolution.

### `POST /api/groups/{id}/delete_trash`

Group trash deletion deletes every member in the group whose stored `target_path` already points under `.photo-org/trash/`.

- non-trash members are retained
- if only one survivor remains, the survivor is de-grouped
- if multiple survivors remain and no primary is left, the best remaining member is promoted to primary

### `POST /api/groups/delete_trash_bulk`

Bulk trash deletion accepts a JSON body with explicit `member_ids`.

- every referenced row must already point under `.photo-org/trash/`
- the endpoint validates the full request before mutating files
- this powers the "delete trash on this page" action in trash review mode

### `POST /api/groups/{group_id}/members/{member_id}/restore_trash`

Trash restore is the undo path for a prior reject decision.

- the row must already point under `.photo-org/trash/`
- the file is renamed back into the managed destination tree using the standard `created_at -> YYYY/MM/DD/` layout
- `keep_state` is changed from `rejected` to `kept`
- if the group no longer has a primary, the best remaining member is promoted

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
