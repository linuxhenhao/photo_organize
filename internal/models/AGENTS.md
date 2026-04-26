# Module Guidelines

## Scope
`internal/models` is reserved for shared data structures if cross-package model types become necessary.

The directory is currently empty. Do not move package-specific types here just for convenience.
Add code only when a type is genuinely shared by multiple packages and would otherwise create import cycles or duplicated definitions.

## Change Rules
Keep this package minimal if it ever gets used. Shared types here should stay passive: data only, with no workflow or database behavior.

## Testing
Place tests next to any new shared types if they contain validation, parsing, or helper behavior.
