# Phase 0 — Remove eager hail import from CLI boot path

## Parent

See `PRD-optional-dependencies.md` in the repository root.

## What to build

The session configuration today computes its `hail_home` default by importing hail at module load time and resolving the hail package directory. Because the CLI imports the session configuration during every invocation, this means hail must be installed for any gentropy command to even reach hydra's step dispatch — including commands for steps that have nothing to do with hail.

Move the default to the lazy code path that the Session object already has. The configuration field becomes a nullable optional that defaults to `None`; when a step that actually starts hail is run, the Session resolves the hail home directory on demand from the live hail import. The CLI then boots without importing hail at all, unblocking every downstream change that needs to treat hail as optional.

This slice is intentionally non-breaking. No extras are introduced, no dependencies are removed, no public behavior changes for users who already have hail installed.

## Acceptance criteria

- [ ] The session-configuration dataclass no longer imports hail at module top.
- [ ] The session-configuration dataclass's hail-home field defaults to `None` and is typed as the existing `str | None` idiom used elsewhere in the same file.
- [ ] The CLI entry point can be loaded and invoked (e.g. for `--help`) in an environment where the hail package is absent — verified by removing hail locally or by importing the CLI entry module in a clean virtualenv where only the rest of the core dependencies are installed.
- [ ] Hail-enabled steps still resolve hail home automatically when `start_hail=True` is set, because the Session's existing lazy fallback handles `None`.
- [ ] The hydra-printed configuration shows `hail_home: null` by default; users who want an explicit path can still pass one through their config.
- [ ] All existing tests pass unchanged — in particular, the configuration-fields test continues to find the `hail_home` field, and the hail-configuration test continues to work via the conftest fixture that resolves hail home independently.
- [ ] No new modules, helpers, or guards are introduced. The change is a deletion of the eager import and a type/default adjustment on a single dataclass field.
- [ ] Commit follows Conventional Commits style and is non-breaking (e.g. `refactor(session): ...` or `feat(session): ...` without the `!` marker).

## Blocked by

None — can start immediately.
