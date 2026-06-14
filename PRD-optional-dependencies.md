# PRD: Optional Dependency Scopes for `hail` and `l2g`

## Problem Statement

A `pip install gentropy` today installs the union of every dependency the project has ever needed: Spark, pandas, hail (with its embedded JVM components), an XGBoost runtime, scikit-learn, scikit-ops, SHAP, matplotlib, Weights & Biases, and the Hugging Face Hub SDK. As a user who only wants to run window-based clumping, study validation, or any of the Spark-only pipeline steps, I am forced to:

- Wait through a heavy install of hail, ML, and tracking libraries I will never call.
- Carry transitive constraints from those libraries (Java version, native wheels, CUDA-adjacent footprint) into environments where they make no sense — laptops, lightweight CI jobs, downstream packages that just want to import a gentropy data class.
- Tolerate that every CLI boot eagerly imports hail (because a session-configuration default at module-load time resolves the hail installation directory), so any environment that fails to install hail also fails to launch the CLI for _any_ step.

As a developer, the same fat-install policy means I cannot validate that the gentropy core is honestly decoupled from hail or from the L2G ML stack: every test runs in a Spark session that has already pulled in hail's Kryo registrator and jars, so subtle hail-versus-vanilla-Spark issues go undetected until they hit production.

As an operator deploying gentropy on a Google Dataproc cluster or in a Docker image, I do not have a way to express "give me only what this workload needs." The published artifact is a single fat install, the init script is a single fat install, and there is no documented matrix of "what do I install if I just want X."

## Solution

Introduce two PEP 621 `[project.optional-dependencies]` extras — `hail` and `l2g` — plus an aggregate `all` extra. Carve the heavy dependencies out of the core `dependencies` array into these extras. Make the source code import-resilient so that:

- The CLI boots and the package imports successfully on a `pip install gentropy` with no extras.
- Modules that are intrinsically hail- or L2G-bound fail at import time with an actionable `ImportError` that names the extra to install.
- Modules whose hail or L2G usage is concentrated in a few methods or classes remain importable; the helpful `ImportError` fires only when the heavy code path is actually invoked.
- Test infrastructure partitions cleanly into hail-needing and hail-free runs, with the Spark session built differently in each so the core test suite can validate genuinely hail-free behavior.
- Docker, Dataproc, and CI continue to install everything by default (fat build) so operational behavior is unchanged for current users.
- Downstream users can explicitly opt into the slim install via `pip install gentropy`, `gentropy[hail]`, `gentropy[l2g]`, or `gentropy[all]`.

Roll this out in three phases:

1. **Phase 0** — Non-breaking cleanup that removes the eager hail import from CLI boot, by deleting the hail-derived default of the session-config `hail_home` field and relying on the already-existing lazy fallback inside the Session object.
2. **Phase A** — Carve out the `hail` extra. Add module-level and lazy-in-function guards. Restructure the Spark test fixture to introspect a `hail` pytest marker and choose between a hail-enabled and a hail-free Spark conf. Update Docker, CI, Dataproc init script, and docs.
3. **Phase B** — Carve out the `l2g` extra and the aggregate `all` extra. Add module-level guards on the L2G method and prediction modules. Lazy-refactor the top-level L2G step module so the non-training step classes (feature matrix, train-test split, evidence, associations) remain usable without the extra.

## User Stories

1. As an external user, I want `pip install gentropy` to install a slim package, so that I can adopt gentropy in environments where hail or the L2G stack is not available or not desired.
2. As an external user, I want `pip install gentropy[hail]` to add hail support, so that I can run gnomAD LD, FinnGen finemapping, PanUKBB LD, and susie finemapper steps.
3. As an external user, I want `pip install gentropy[l2g]` to add locus-to-gene model training and prediction, so that I can train, evaluate, and publish L2G models without installing hail.
4. As an external user, I want `pip install gentropy[all]` to give me the historical "everything" experience, so that migrating from the old fat install is a one-character change.
5. As an external user, when I try to run a hail-backed step without the `hail` extra installed, I want the error to name the extra and tell me how to install it, so that I do not have to read a stack trace to figure out what is missing.
6. As an external user, when I try to run an L2G step without the `l2g` extra, I want the same actionable error.
7. As an external user, when I run a feature-matrix-only L2G step (which does not need xgboost or wandb) on a `[hail]`-only install, I want it to succeed without forcing me to install the L2G extra.
8. As an external user, I want to import gentropy data classes (`StudyLocus`, `VariantIndex`, `L2GFeatureMatrix`, `L2GGoldStandard`) without forcing any of the extras, so that downstream code can construct and inspect these objects in a lightweight environment.
9. As a CLI operator, I want `gentropy --help` and any non-hail step to succeed even when hail is not installed, so that a missing extra never blocks unrelated workflows.
10. As a CLI operator, I want the printed hydra config to show `hail_home: null` by default in a non-hail install, so that the absence of hail is reflected honestly in the resolved configuration.
11. As a CLI operator with the `hail` extra installed, I want hail to resolve its own home path lazily inside the Session when `start_hail=True`, so that I can run hail-enabled steps with no extra configuration.
12. As a developer running `make test`, I want all extras to be installed automatically and every test to be exercised across both hail and non-hail Spark sessions, so that I never have to remember separate sync commands.
13. As a developer running `pytest -m "not hail"`, I want to execute only the core suite in a hail-free Spark session, so that I can validate that core behavior does not regress on the hail-free path.
14. As a developer running `pytest -m hail`, I want only the hail-marked tests to execute, with the Spark session built with the hail Kryo registrator and jar configuration, so that hail tests run in their intended environment.
15. As a developer who has only the `hail` extra installed (no L2G), I want L2G-marked tests to skip cleanly, so that I can run a partial test suite without errors.
16. As a developer reviewing diffs, I want the in-source guard pattern to be uniform — one shared helper function for the install-hint message, and a consistent try/except shape — so that the codebase does not accumulate divergent error wording across modules.
17. As a developer adding a new datasource that uses hail, I want to copy the existing module-guard pattern, so that contributing a new hail-backed module does not require inventing a new convention.
18. As a developer adding a new test that uses hail, I want to know to add `@pytest.mark.hail` and `pytest.importorskip("hail")` inside the test body, so that the test integrates with the partition machinery automatically.
19. As a CI maintainer, I want the workflow's dependency-install step to cover both groups and extras, so that subsequent `make test` invocations run against a cache-warm environment.
20. As a CI maintainer, I want coverage data from the hail and non-hail pytest invocations to be combined into a single coverage report, so that the existing Codecov upload continues to reflect total coverage.
21. As a Docker image consumer, I want the published image to behave exactly as before — fully provisioned with hail and L2G — so that current image-based deployments require no migration.
22. As a Dataproc operator, I want the cluster init script to install the `all` extra explicitly, so that the install line is self-documenting about what is being pulled in.
23. As a maintainer cutting a release from `dev` to `main`, I want the breaking-change phases to produce `feat(packaging)!:` commits with `BREAKING CHANGE:` footers, so that semantic-release bumps the major version and the changelog signals the migration step to downstream users.
24. As a documentation reader, I want the install matrix to spell out which extra unlocks which functional surface, so that I can pick the smallest install that fits my workload.
25. As a documentation reader, I want a worked example showing what fails when an extra is missing and exactly which extra to add, so that I can reproduce or recover from a misinstall.
26. As a release manager, I want Phase 0 to land before Phase A, so that the CLI is verified to boot without hail before any user-visible extras-carving lands.
27. As a release manager, I want Phase A to land before Phase B, so that the marker-based Spark-session machinery is validated on the hail axis (where the Kryo registrator coupling is real) before being extended for L2G filtering.
28. As a release manager, I want each breaking-change phase to be a single PR that touches the pyproject, source guards, tests, and consumers atomically, so that no commit between the two leaves a half-broken state on `dev`.
29. As a user inspecting `pytest --markers`, I want the `hail` and `l2g` markers to be registered with clear descriptions that name the required extra, so that the marker conventions are discoverable from the standard pytest help surface.
30. As a contributor running `uv sync --group test`, I want the test group to declare a self-reference to `gentropy[all]` (or, in Phase A, `gentropy[hail]`), so that syncing the test group automatically pulls the extras needed to execute the suite.
31. As a developer running a partial sync (`uv sync` with no extras), I want the package to import successfully at the module level for everything outside the hail and L2G surface, so that I can develop and unit-test core code without setting up heavy dependencies.
32. As a tooling author using `deptry`, I want `xgboost`, `xgboost-cpu`, and the rest of the L2G stack to be declared in `[project.optional-dependencies].l2g` (and hail in the `hail` extra), so that the dependency analyzer recognizes their imports as legitimately declared, even when those imports sit inside `try/except` guards or in-function bodies.
33. As a developer who uses the spark fixture in a non-hail test, I want the fixture to build a vanilla Spark session (no hail jar, no Kryo registrator) so that the test environment matches what a non-hail production install would see.
34. As a developer who uses the spark fixture in a hail test, I want the fixture to build a hail-enabled Spark session, so that hail's mutation of Spark global state is contained to that test session.
35. As a maintainer of `utils/install_dependencies_on_cluster.sh`, I want the install line to use the `all` extra (in Phase B), so that any future addition of new extras flows through that single specifier.
36. As a user of `gentropy.dataset.l2g_feature_matrix` or `gentropy.dataset.l2g_gold_standard`, I want those modules to remain importable with only the core install (no extras), because they contain no L2G heavy dependencies themselves.
37. As a user of `gentropy.external.wandb` or `gentropy.external.hf_hub`, I want those modules to remain importable with only the core install, because they only depend on pydantic for credential models, not on the underlying SDKs.
38. As a `mypy` user, I want the existing override entries that mark hail, xgboost, sklearn, and friends as `ignore_missing_imports = true` to continue working, so that type-checking succeeds even when an extra is not installed in the type-check environment.
39. As a doctest user, I want module-level doctests in hail- and L2G-bound modules to skip cleanly when their extra is missing (or for the contract to be that `make test` always installs extras and the partial-extras invocations are best-effort), so that documentation never silently breaks the suite.
40. As a developer of `LocusToGeneStep`, I want the top-level L2G step module to be lazy-refactored such that only `run_train` and `run_predict` trigger L2G imports, so that the four sibling step classes (`LocusToGeneFeatureMatrixStep`, `LocusToGeneTrainTestSplitStep`, `LocusToGeneEvidenceStep`, `LocusToGeneAssociationsStep`) remain runnable on a core-only or hail-only install.

## Implementation Decisions

- **Dependency mechanism.** Use PEP 621 `[project.optional-dependencies]`, not PEP 735 `[dependency-groups]`. Extras are the only mechanism that makes opt-in installable to all downstream pip-style consumers and that ends up in published wheel metadata.
- **Extras.** Define three extras: `hail`, `l2g`, and `all`. The aggregate `all` is the self-referential `["gentropy[hail,l2g]"]` form and is introduced in Phase B.
- **Bucketing of dependencies.**
  - `hail` extra: the hail package.
  - `l2g` extra: xgboost (all three platform-conditional pins from the current core list), xgboost-cpu, scikit-learn, scikit-ops (`skops`), SHAP, matplotlib, Weights & Biases, and `huggingface-hub`.
  - Core: everything else, including pandas with the gcp and parquet extras. Pandas remains core because seven non-L2G modules use it directly and the existing `pyspark[pandas_on_spark]` declaration already pulls it in.
- **Shared helper.** Introduce a single helper function (`install_hint(extra: str) -> str`) in a new common-imports module. Every guarded `ImportError` re-raises with this helper's message so that wording stays consistent across roughly a dozen guarded sites.
- **Per-module guard strategy.** Classify each module as one of:
  - **Module-guard** — top-of-module `try/except` covering all third-party imports of the relevant extra, raising the helper-generated `ImportError` when the extra is missing. Use this when the module is intrinsically bound to the extra (class definitions reference the heavy types, default values instantiate heavy classes, or the file is saturated with heavy-package calls).
  - **Lazy in-function** — move the heavy `import` into the function or method bodies that use it, each wrapped in a try/except that re-raises with the helper message. Use this when the module is only partially bound and has meaningful surface that should remain importable without the extra.
  - **Soft fallback (no raise)** — used for one specific site in the session configuration default, where the existing eager hail import is replaced by the already-present lazy resolution inside the Session object; the configuration field's default becomes a nullable optional.
- **Hail classification.** Module-guard the four hail-bound datasources. Lazy-guard the existing in-function hail imports inside the common Session and genomic-region modules and the testing-spark-conf utility. Soft-fallback the session-config dataclass's `hail_home` field. Modules that consume these (the gnomAD ingestion step, the FinnGen finemapping ingestion step, the LD matrix interface, the susie finemapper) need no direct guards because the guard at the leaf modules propagates naturally up the import chain and surfaces with the friendly message.
- **L2G classification.** Module-guard the L2G model module, the L2G trainer module, and the L2G prediction dataset module. Lazy-refactor the top-level `gentropy.l2g` step module so that only the training and prediction step classes pull in L2G-heavy imports; the feature-matrix, train-test-split, evidence, and associations step classes remain free of L2G dependencies. Leave the existing in-function L2G imports inside the model module as-is — they are harmless under the new module-guard and removing them would inflate the diff.
- **Testing markers.** Register `hail` and `l2g` pytest markers with descriptive help text. Tests requiring an extra get the corresponding decorator at the function level (or `pytestmark` at file level for files that are wholly extra-bound), and the heavy import is performed inside the test body via `pytest.importorskip(...)` — pytest-native, no collection hook.
- **Test conftest.** Move L2G and hail imports out of module-top in the root tests conftest and into the fixture bodies that need them. Use `if TYPE_CHECKING:` for any annotations that still reference the heavy classes.
- **Spark fixture introspection.** The session-scoped `spark` fixture takes the `request` argument, scans `request.session.items` for items carrying the `hail` marker, and passes `with_hail=True` (or `False`) to the testing Spark-configuration helper. The helper accepts a boolean flag controlling whether hail's jar path, Kryo registrator, and serializer settings are added; when the flag is true, hail is imported lazily inside the helper.
- **`make test` partitioning.** The `test` target syncs all extras and the test group, then invokes pytest twice — once with `-m "not hail"` writing to a non-hail coverage file, once with `-m "hail"` writing to a hail-specific coverage file. The existing `coverage` target combines the new coverage files with the existing ones.
- **CI workflow.** The dependency-install step uses `uv sync --all-groups --all-extras` so the cache is populated for the subsequent `make test` invocation.
- **Docker.** The Dockerfile syncs with `--all-extras` so the published image remains fat. The existing `HAIL_HOME` environment variable continues to work because hail is installed by the fat sync.
- **Dataproc init script.** In Phase A the script temporarily uses `gentropy[hail]` (since the L2G stack is still in core). In Phase B it switches to `gentropy[all]`. The fat-install behavior is preserved end-to-end.
- **Documentation.** Add an install matrix to the README and the mkdocs landing/installation page: core, `[hail]`, `[l2g]`, `[all]`, with a one-line description of what each unlocks.
- **Phasing.** Three sequential PRs. Phase 0 is the session-config cleanup; it is non-breaking and lands first. Phase A introduces the `hail` extra and the full guard/test/consumer wiring for hail. Phase B introduces the `l2g` extra, the `all` aggregate, the L2G guards and lazy refactor, and finalizes the consumer surface.
- **Commit hygiene.** Phase 0 is a `refactor` or `feat`-non-breaking commit. Phases A and B use Conventional Commits with `feat(packaging)!:` titles and `BREAKING CHANGE:` footers naming the extra and listing the affected functional surface, so that `semantic-release` interprets them as major-version bumps when `dev` is merged to `main`.

## Testing Decisions

- **Definition of a good test in this codebase.** Exercise the externally observable behavior of a public module: an import succeeds or fails with the documented error message; a step runs and produces the expected data shape; a fixture returns a Spark session with the expected configuration. Avoid asserting on private attributes, internal call counts, or specific exception-chain shapes beyond "an `ImportError` is raised that mentions the extra name."
- **Imports & guards.** Verify behaviorally, not by introspection. A small test that imports a hail-guarded module in an environment where hail is absent and asserts that the resulting `ImportError` carries the expected install-hint substring. The repo's existing `tests/gentropy/no_spark` package — which already exercises lightweight no-Spark behavior — is the right place for these.
- **Session configuration.** The existing `tests/gentropy/test_config.py` test that asserts the set of `SessionConfig` fields is the natural place to extend; it must continue to pass with `hail_home` typed as nullable. The existing `tests/gentropy/no_spark/test_no_spark.py` hail-configuration test already exercises the lazy fallback inside the Session and is unchanged in shape, only re-marked as a hail test.
- **Spark fixture branching.** A unit-style assertion that the testing Spark-conf helper returns a configuration with the hail keys when called with `with_hail=True` and without them when called with `with_hail=False`. A behavioral assertion that, under a hail-only pytest invocation, the resolved fixture session's `SparkConf` contains the hail Kryo registrator; under a non-hail invocation, it does not.
- **Marker partitioning.** A behavioral test or CI step that runs `pytest -m "not hail" --collect-only` and `pytest -m "hail" --collect-only` and verifies the partition is disjoint and exhaustive across the test files known to use hail.
- **L2G surface.** The existing trainer and model tests gate on the `l2g` extra via marker and in-body `importorskip`. Behavioral tests for `LocusToGeneFeatureMatrixStep`, `LocusToGeneTrainTestSplitStep`, `LocusToGeneEvidenceStep`, and `LocusToGeneAssociationsStep` are valuable here because those four classes are the proof that the lazy refactor of the top-level L2G step module worked: each can be instantiated and a representative method exercised under a `[hail]`-only or core-only install.
- **Doctest.** `make test` always installs extras, so doctest collection is exercised in the fat environment. Partial-extras pytest invocations are documented as best-effort with respect to doctests.
- **Coverage combination.** The combine step adds the new coverage files; verify locally that combined `coverage xml` includes both partitions, and that the existing Codecov upload step still works against the combined file.
- **Prior art.** The codebase already runs three separate pytest invocations (default, no-shared-spark, web-deps) and combines their coverage. This PRD's hail partition slots into the same pattern. The `tests/gentropy/no_spark` package is prior art for tests that intentionally bypass the Spark session fixture; new "no-hail" tests follow the same shape.

## Out of Scope

- A `gpu` extra for CUDA-enabled xgboost or a GPU-accelerated SHAP install. Not requested and not part of any current workflow.
- A `dev` aggregate extra. Existing `[dependency-groups].dev` already covers development tooling and remains the right home for that.
- Splitting the published Docker image into multiple tags (slim, hail, all). The fat image is retained.
- Splitting the Dataproc init script into per-extra variants. The init script installs the `all` extra unconditionally after Phase B.
- Migrating any other dependency out of core (numpy, scipy, scikit-learn-adjacent transitives via pyspark, etc.). Only the hail and L2G surface is being carved out.
- Changing the published wheel layout (`[tool.hatch.build.targets.wheel].packages`). The wheel continues to package `src/gentropy` as today.
- Changing the configured doctest behavior under partial-extras invocations. Doctests in guarded modules will not run cleanly in a `pip install gentropy` (no extras) environment; the documented contract is that `make test` is the supported way to run the suite.
- Adjusting the existing platform-conditional pinning of xgboost (three pins covering arm64, linux amd64/x86_64, and macOS x86_64). The pins move from the core array into the `l2g` extra unchanged.
- Removing the historical mypy `ignore_missing_imports` overrides for the now-optional packages. They remain useful for type-check environments that do not install the extras.

## Further Notes

- The session configuration is currently the only site in the package where a heavy import (hail) leaks into the CLI's boot path. The Phase 0 cleanup is a one-place change made possible because the Session object already had the lazy resolution code; the eager `os.path.dirname(hail.__file__)` default was redundant pre-computation. It is worth scanning the rest of the codebase periodically for similar patterns — module-level defaults whose values are derived from heavy imports that the runtime already resolves lazily elsewhere.
- The `LocusToGeneModel` dataclass currently uses an `XGBClassifier(...)` instance as the default for its `model` field. This is evaluated at class-definition time and is the single most decisive signal that the L2G model module must be module-guarded rather than lazy. If, in a future refactor, that default were replaced with a `field(default_factory=...)`, the module could potentially be downgraded to a lazy guard — but that is not in scope here.
- The fat-image default for Docker and Dataproc preserves backwards-compatibility for operational consumers; the breaking-change footer signals the install contract for external Python users who do `pip install gentropy`. This split — operational surface unchanged, declarative surface tightened — is the intentional outcome.
- The `pytest.mark.hail` marker drives two coupled behaviors: per-test filtering (so `make test` can run two pytest processes against disjoint subsets) and Spark-session shape (the session-scoped fixture introspects `request.session.items` at first use). Because pytest finalises collection before any session-scoped fixture runs, the introspection is safe and requires no CLI flag or environment variable.
- Future contributors adding hail- or L2G-using code should treat module-guard as the default for new modules that are intrinsically bound to an extra, and lazy in-function guards as the default for modules whose binding is concentrated in a small fraction of their methods. The shared `install_hint` helper keeps the wording consistent.
