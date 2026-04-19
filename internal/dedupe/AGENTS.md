# Module Guidelines

## Scope
`internal/dedupe` contains rules for deciding whether one file is a derivative of another and which item should be treated as the master.

## Change Rules
Keep this package focused on comparison logic, not file I/O or database writes. Directionality matters here: a thumbnail relationship is not symmetric. If you change thresholds or preference rules, make the rationale obvious in code and avoid hidden behavior changes in callers.

## Testing
Extend `thumbnail_test.go` with representative parent/child and non-match cases. Favor explicit fixtures that show why a relationship should or should not be accepted.
