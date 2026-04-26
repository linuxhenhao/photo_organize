# Module Guidelines

## Scope
`internal/vision` contains OpenCV-backed checks that are too heavy or specialized for the core hashing packages.

This package owns ORB feature extraction, serialization, deserialization, and derivative verification.
It also provides a no-op fallback when the build does not include OpenCV.

## Change Rules
Keep OpenCV-specific dependencies isolated here so the rest of the codebase stays easier to reason about.
Be explicit about failure handling when native libraries or codecs are unavailable, especially in non-container environments.

The main split to preserve is:
`VerifyDerivativeWithORB` for path-based verification and `VerifyDerivativeWithORBFeatures` for already-loaded feature sets.
Keep ORB serialization compatible with persisted cache rows and close any native resources deterministically.

## Testing
Add narrowly scoped tests only when they can run reliably in the current toolchain. Prefer small, deterministic checks over broad image-processing integration tests.
