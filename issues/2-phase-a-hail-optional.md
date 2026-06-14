# Phase A — Make hail an optional dependency (`[hail]` extra)

## Parent

See `PRD-optional-dependencies.md` in the repository root.

## What to build

Carve hail out of the core install. After this slice, `pip install gentropy` produces an environment with no hail, and `pip install gentropy[hail]` produces an environment in which the hail-backed steps (gnomAD LD, FinnGen finemapping, PanUKBB LD, susie finemapper) work end-to-end. The slice cuts through every layer the change touches at once — packaging metadata, in-source import guards, test infrastructure, the testing Spark fixture, the Makefile, CI, the Docker image, the Dataproc init script, and the documentation.

The implementation follows the per-module classification from the PRD. Datasource modules that are intrinsically hail-bound get top-of-module guards that re-raise a friendly `ImportError` naming the `[hail]` extra when hail is missing. Modules whose hail use is concentrated in one or two methods keep their existing lazy imports, just wrapped with the same friendly guard. The testing-Spark-conf helper gains a `with_hail` flag so that the test Spark session can be built without the hail Kryo registrator or jar configuration when running the non-hail partition.

A new shared helper module produces the install-hint message used by every guard, so the wording stays consistent across roughly half a dozen guarded sites. Tests requiring hail are annotated with a registered `hail` pytest marker and use `pytest.importorskip("hail")` inside the test body; the existing top-level hail imports in test files are moved inside fixture and test bodies. The session-scoped Spark fixture introspects the marker on collected items and passes `with_hail=True` only when at least one hail-marked test is in the run, so `pytest -m "not hail"` honestly executes the core suite in a hail-free Spark session and `pytest -m hail` runs the hail-marked subset in a hail-enabled one.

The Makefile's existing `test` target syncs all extras together with the test group and then performs two pytest invocations — one filtered to non-hail tests, one filtered to hail tests — each writing to its own coverage file. The combine target gains the new coverage file. The Dockerfile, CI workflow, and Dataproc init script are updated so the operational surfaces continue to install hail by default; for this phase the Dataproc init script uses the `[hail]` form, which will be widened to `[all]` in Phase B. The README and the mkdocs install page gain a row describing what the `[hail]` extra unlocks.

## Acceptance criteria

- [ ] The hail package is removed from the core `dependencies` array and listed under a new `[project.optional-dependencies].hail` entry.
- [ ] A new shared helper module exposes an `install_hint(extra)` function used by every new guard.
- [ ] The four datasource modules that are intrinsically hail-bound (gnomAD LD, gnomAD variants, FinnGen finemapping, PanUKBB LD) raise a friendly `ImportError` naming `[hail]` when imported in an environment without the extra.
- [ ] The three pre-existing in-function hail imports in the common Session and genomic-region modules are wrapped with the same friendly guard.
- [ ] The testing-Spark-configuration helper accepts a `with_hail` flag; when false, the returned `SparkConf` carries no hail-specific keys; when true, hail is imported lazily inside the helper and the hail jar, classpath, serializer, and Kryo registrator entries are added.
- [ ] The session-configuration soft fallback continues to behave correctly — Phase 0's nullable default is preserved.
- [ ] The `hail` pytest marker is registered with a descriptive entry in the pytest configuration.
- [ ] Every test file that today does a top-level `import hail` either moves the import inside the test body (preferred) or skips collection via `pytest.importorskip`; the affected tests are annotated with `@pytest.mark.hail` (or a file-scope `pytestmark` for files where every test needs hail).
- [ ] The root test conftest's top-level hail import is removed; the hail-home fixture imports hail inside its body.
- [ ] The session-scoped Spark fixture inspects `request.session.items` for the hail marker and selects the with-hail-or-not branch of the Spark-conf helper accordingly.
- [ ] The `test` Makefile target syncs all extras and the test group, then performs two pytest invocations (one for non-hail, one for hail) each writing to its own coverage file. The `coverage` target combines the new file with the existing coverage files.
- [ ] The Dockerfile sync invocation includes `--all-extras` (or an equivalent fat-install specifier) so the published image continues to include hail.
- [ ] The Dataproc cluster init script's gentropy install line is updated to specify the `[hail]` extra (interim form; widened to `[all]` in Phase B).
- [ ] The CI workflow's dependency-install step uses `--all-groups --all-extras` so the cache covers what `make test` will need.
- [ ] The README and the mkdocs install page gain an install matrix row for `pip install gentropy[hail]` with a one-line description of what it unlocks.
- [ ] `pip install gentropy` (no extras) succeeds in producing an environment in which (a) the CLI boots, (b) non-hail step modules import, and (c) attempting to instantiate a hail-bound step surfaces a `ImportError` whose message names `[hail]` and includes the `pip install gentropy[hail]` command.
- [ ] `pip install gentropy[hail]` succeeds in producing an environment in which the hail-backed steps run end-to-end.
- [ ] All existing tests pass under `make test`; the new hail/non-hail partition is exhaustive over the test files known to use hail and the two partitions are disjoint.
- [ ] Coverage from the combined invocations matches or exceeds the existing baseline.
- [ ] Commit follows Conventional Commits with `feat(packaging)!:` title and a `BREAKING CHANGE:` footer naming the hail extra and listing the affected functional surface.

## Blocked by

- Blocked by #1 (Phase 0 — CLI must boot without resolving hail at import time before the test partition can validate hail-free behavior).
