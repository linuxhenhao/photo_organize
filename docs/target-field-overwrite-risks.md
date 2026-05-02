# Target Field Overwrite Risks

This note records the class of bugs where a regroup/adoption path accidentally
overwrites preserved `target_items` metadata with empty values.

## Confirmed cases

- `created_at` in `target_items` was previously cleared by `initcache` regrouping
  when the regroup input carried an empty `created_at`.
- `meta_json.fingerprint.modified_at` was previously lost when target metadata
  was rewritten without carrying the existing JSON payload forward.

## Evidence from the NAS catalog

On `/volume3/DocsAndMedia/Multimedia/repo/repo.db`, the catalog already
contained `4836` `target_items` rows with `created_at = ''`, and `3517` of those
rows were still `group_status = 'completed'`. `meta_json.fingerprint.modified_at`
was still present on the same catalog snapshot, which points to a preserved JSON
fingerprint but a damaged `created_at` field.

## Current write rules

- Do not clear `created_at` or `meta_json` during regroup/adoption just because
  the new input is empty.
- Only explicit recomputation paths may replace `created_at`.
- Only explicit recomputation paths may replace the fingerprint JSON payload.

## Fields reviewed and not found to have the same bug

I reviewed the current `target_items` write paths and did not find the same
blank-overwrite bug on these columns:

- `exact_hash`
- `phash`
- `phash_bits`
- `width`
- `height`

These fields are recomputed inputs, not preserved provenance. They are still
worth checking if a future refactor introduces sentinel values or partial writes.

The workflow-state columns are intentionally mutable and are not part of this
overwrite-risk class:

- `group_id`
- `keep_state`
- `is_group_primary`
- `group_status`
- `origin_source_id`

## Review rule

Any change that writes `target_items` should answer one question first:

Does an empty incoming value mean "preserve the old database value" or
"explicitly clear the field"?

If the answer is not explicit in code, the write path is suspect.
