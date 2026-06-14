# Phase B — Make L2G dependencies optional and introduce the `all` aggregate extra

## Parent

See `PRD-optional-dependencies.md` in the repository root.

## What to build

Carve the locus-to-gene machine learning dependencies out of the core install and introduce an aggregate `all` extra that pulls both `[hail]` and `[l2g]` together. After this slice, `pip install gentropy` produces an environment with neither hail nor the L2G stack; `pip install gentropy[l2g]` enables L2G model training, prediction, evaluation, persistence, and Hugging Face Hub publishing; `pip install gentropy[all]` reproduces the historical "everything" install. The slice cuts through every layer the change touches at once — packaging metadata, in-source guards, lazy refactor of the top-level L2G step module, test marker registration and test-file updates, the Dataproc init script's switch to `[all]`, and the documentation install matrix.

The implementation follows the per-module classification from the PRD. The L2G model module and L2G trainer module are intrinsically bound to the L2G stack (the model class field default instantiates `XGBClassifier` at class-definition time; the trainer is saturated with wandb, scikit-learn, SHAP, and matplotlib usage). Both get top-of-module guards that re-raise the shared install-hint message. The L2G prediction dataset module imports the now-guarded model class at top and uses SHAP in its explainability methods, so it also receives a top-of-module guard.

The top-level L2G step module is lazy-refactored. Its top-level imports of `XGBClassifier`, `wandb_login`, the L2G model, the L2G trainer, and the L2G prediction dataset are moved inside the bodies of the methods that use them — `LocusToGeneStep.run_train`, `LocusToGeneStep.run_predict`, and the associations step's prediction load site. The four sibling step classes (`LocusToGeneFeatureMatrixStep`, `LocusToGeneTrainTestSplitStep`, `LocusToGeneEvidenceStep`, `LocusToGeneAssociationsStep`) remain free of L2G heavy dependencies and continue to work on a core-only or `[hail]`-only install. The L2G feature-matrix and gold-standard dataset modules, the feature factory, and the `gentropy.external.wandb` / `gentropy.external.hf_hub` credential models stay in core because they have no L2G heavy imports.

The `[dependency-groups].test` self-reference is widened from `gentropy[hail]` to `gentropy[all]`. The `l2g` pytest marker is registered alongside the existing `hail` marker. Tests requiring the L2G stack use the same in-body `pytest.importorskip` pattern Phase A established for hail. The Dataproc init script switches from `[hail]` to `[all]` so the operational install line is fully expressive of what the cluster needs. The Dockerfile and CI workflow already use `--all-extras` from Phase A and require no further change. The README and mkdocs install page gain rows for `[l2g]` and `[all]`.

## Acceptance criteria

- [ ] The L2G heavy packages (xgboost — all three platform-conditional pins — xgboost-cpu, scikit-learn, scikit-ops/skops, SHAP, matplotlib, Weights & Biases, huggingface-hub) are removed from the core `dependencies` array and listed under a new `[project.optional-dependencies].l2g` entry.
- [ ] A new `[project.optional-dependencies].all` entry is added with the self-referential `["gentropy[hail,l2g]"]` form.
- [ ] The L2G model module, L2G trainer module, and L2G prediction dataset module are top-of-module guarded with the shared `install_hint("l2g")` message.
- [ ] The top-level L2G step module is lazy-refactored: its `wandb_login`, `XGBClassifier`, L2G prediction dataset, L2G model, and L2G trainer imports are moved into the methods that use them; type annotations that referenced these names use TYPE_CHECKING / forward references where needed.
- [ ] The four non-training L2G step classes (`LocusToGeneFeatureMatrixStep`, `LocusToGeneTrainTestSplitStep`, `LocusToGeneEvidenceStep`, `LocusToGeneAssociationsStep`) can be instantiated and exercised in an environment without the `[l2g]` extra installed; the L2G feature-matrix, gold-standard, feature-factory, and credential modules also remain importable without the extra.
- [ ] The `l2g` pytest marker is registered with a descriptive entry in the pytest configuration.
- [ ] Test files that today do a top-level import of any L2G heavy package use the in-body `pytest.importorskip` pattern and carry the `@pytest.mark.l2g` decoration (or a file-scope `pytestmark`).
- [ ] The root test conftest's top-level L2G prediction import is moved inside the fixture body; the L2G feature-matrix and gold-standard imports remain at module top because those datasets are not L2G-extra-bound.
- [ ] The `[dependency-groups].test` self-reference is widened from `gentropy[hail]` to `gentropy[all]` so `uv sync --group test` pulls every extra needed by the suite.
- [ ] The Dataproc cluster init script is updated from `gentropy[hail]` to `gentropy[all]`.
- [ ] The Dockerfile and CI workflow continue to work unchanged from Phase A (already at `--all-extras`); confirm by exercising both pipelines.
- [ ] The README and the mkdocs install page gain install-matrix rows for `[l2g]` and `[all]`.
- [ ] `pip install gentropy` (no extras) succeeds in producing an environment in which (a) the CLI boots, (b) feature-matrix / train-test-split / evidence / associations steps run end-to-end without the L2G stack, (c) attempting to run training or prediction surfaces an `ImportError` whose message names `[l2g]` and includes the `pip install gentropy[l2g]` command.
- [ ] `pip install gentropy[l2g]` succeeds in producing an environment in which L2G training, prediction, model load/save, and Hugging Face Hub publishing work end-to-end.
- [ ] `pip install gentropy[all]` produces the same end-to-end coverage as a pre-PRD install.
- [ ] All existing tests pass under `make test`; the hail / non-hail partition from Phase A continues to be exhaustive and disjoint; coverage from the combined invocations matches or exceeds the existing baseline.
- [ ] Commit follows Conventional Commits with `feat(packaging)!:` title and a `BREAKING CHANGE:` footer naming the L2G extras and listing the affected functional surface.

## Blocked by

- Blocked by #2 (Phase A — the shared `install_hint` helper, the test-marker registration pattern, and the lazy-Spark-session machinery introduced for hail are reused here for L2G; the consumer surfaces — Dockerfile, CI, Makefile — also depend on Phase A's setup).
