# Module Guidelines

## Scope
`internal/vision` contains OpenCV-backed checks that are too heavy or specialized for the core hashing packages.

## Change Rules
Keep OpenCV-specific dependencies isolated here so the rest of the codebase stays easier to reason about. Be explicit about failure handling when native libraries or codecs are unavailable, especially in non-container environments.

## Testing
Add narrowly scoped tests only when they can run reliably in the current toolchain. Prefer small, deterministic checks over broad image-processing integration tests.
