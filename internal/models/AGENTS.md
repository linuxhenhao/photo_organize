# Module Guidelines

## Scope
`internal/models` is reserved for shared data structures if cross-package model types become necessary.

## Change Rules
This directory is currently empty. Do not move package-specific types here just for convenience. Add code only when a type is genuinely shared by multiple packages and would otherwise create import cycles or duplicated definitions.

## Testing
Place tests next to any new shared types if they contain validation, parsing, or helper behavior.
