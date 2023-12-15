# CHANGELOG



## v0.1.0-rc.1 (2023-12-15)

### Build

* build(deps-dev): bump ruff from 0.1.7 to 0.1.8 (#341)

Bumps [ruff](https://github.com/astral-sh/ruff) from 0.1.7 to 0.1.8.
- [Release notes](https://github.com/astral-sh/ruff/releases)
- [Changelog](https://github.com/astral-sh/ruff/blob/main/CHANGELOG.md)
- [Commits](https://github.com/astral-sh/ruff/compare/v0.1.7...v0.1.8)

---
updated-dependencies:
- dependency-name: ruff
  dependency-type: direct:development
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`3643f64`](https://github.com/opentargets/genetics_etl_python/commit/3643f649c5b1afc555249caa086fd26853daa4f1))

* build(deps-dev): bump isort from 5.13.1 to 5.13.2 (#342)

Bumps [isort](https://github.com/pycqa/isort) from 5.13.1 to 5.13.2.
- [Release notes](https://github.com/pycqa/isort/releases)
- [Changelog](https://github.com/PyCQA/isort/blob/main/CHANGELOG.md)
- [Commits](https://github.com/pycqa/isort/compare/5.13.1...5.13.2)

---
updated-dependencies:
- dependency-name: isort
  dependency-type: direct:development
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`a96793a`](https://github.com/opentargets/genetics_etl_python/commit/a96793af5d0d91209b12113a2097f92fe705efba))

* build(deps-dev): bump python-semantic-release from 8.3.0 to 8.5.1 (#343)

Bumps [python-semantic-release](https://github.com/python-semantic-release/python-semantic-release) from 8.3.0 to 8.5.1.
- [Release notes](https://github.com/python-semantic-release/python-semantic-release/releases)
- [Changelog](https://github.com/python-semantic-release/python-semantic-release/blob/master/CHANGELOG.md)
- [Commits](https://github.com/python-semantic-release/python-semantic-release/compare/v8.3.0...v8.5.1)

---
updated-dependencies:
- dependency-name: python-semantic-release
  dependency-type: direct:development
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;
Co-authored-by: David Ochoa &lt;ochoa@ebi.ac.uk&gt; ([`817d8f0`](https://github.com/opentargets/genetics_etl_python/commit/817d8f07cd621f13db7933bdb14369f846ba076c))

### Feature

* feat: semantic release gh action (#354) ([`c7b0dca`](https://github.com/opentargets/genetics_etl_python/commit/c7b0dca11aef068451d57883ef5f1fa295122444))

* feat: upload release (#353) ([`0c00e50`](https://github.com/opentargets/genetics_etl_python/commit/0c00e50328a64c77c4eaf7e99ea6c180451102b1))

* feat: activate release process (#352)

* feat: serious release

* revert: no tests within release workflow ([`2a40dca`](https://github.com/opentargets/genetics_etl_python/commit/2a40dca883f7d7bdb3a3ea2b0bf25b1c9cddc86b))

### Unknown

* revert: dispatch (#355) ([`721a8b1`](https://github.com/opentargets/genetics_etl_python/commit/721a8b1cab48d74e63fbd0476ba7b84eddce86a0))


## v0.0.0-rc.1 (2023-12-14)

### Breaking

* feat: build and submit process redesigned

- New build based on `poetry build`
- Project renamed - beware consequences with `poetry update` etc.
- Using wheels accross the board
- Package is installed in dataproc using intialisation actions
- Dependencies are installed in the remote cluster for remote work
- Minor bugs on config paths in variant variant_annotation
- BUG: variant_annotation produces an error not yet diagnosed

BREAKING CHANGE: src/etl now contains the modules instead of src/ ([`16b56fa`](https://github.com/opentargets/genetics_etl_python/commit/16b56fa2c21bb73c603a59e1d5403ecbe860affa))

* feat: modular hydra configs and other enhancements

- Dynamic configs using hydra (see README)
- ETLSession initialises spark session
- Moving to Spark logging instead of python logging
- Missing dependencies added (e.g. mypy, flake8 extensions)
- Missing dependencies added (e.g. pandas)
- Unnecesary logic removed from intervals
- Mypy added to precommit
- VScode workspace settings include lintrs

BREAKING CHANGE: ETLSession class to initialise Spark/logging ([`bcfb28e`](https://github.com/opentargets/genetics_etl_python/commit/bcfb28e5952aa78afeb4d18cb1e99b9d0c87078a))

### Build

* build(deps-dev): bump ipykernel from 6.26.0 to 6.27.1 (#332) ([`e275a83`](https://github.com/opentargets/genetics_etl_python/commit/e275a839a9afb9a5a94dcaf872fbcb09cc53811a))

* build(deps-dev): bump pytest-xdist from 3.4.0 to 3.5.0 (#333) ([`42b366c`](https://github.com/opentargets/genetics_etl_python/commit/42b366cb1e0aba87822309679af3727b605a2890))

* build(deps-dev): bump ruff from 0.1.6 to 0.1.7 (#331)

Bumps [ruff](https://github.com/astral-sh/ruff) from 0.1.6 to 0.1.7.
- [Release notes](https://github.com/astral-sh/ruff/releases)
- [Changelog](https://github.com/astral-sh/ruff/blob/main/CHANGELOG.md)
- [Commits](https://github.com/astral-sh/ruff/compare/v0.1.6...v0.1.7)

---
updated-dependencies:
- dependency-name: ruff
  dependency-type: direct:development
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`ef5a498`](https://github.com/opentargets/genetics_etl_python/commit/ef5a4982f72187b89ae4da1284735550c91e7c7c))

* build(deps-dev): bump google-cloud-dataproc from 5.7.0 to 5.8.0 (#330)

Bumps [google-cloud-dataproc](https://github.com/googleapis/google-cloud-python) from 5.7.0 to 5.8.0.
- [Release notes](https://github.com/googleapis/google-cloud-python/releases)
- [Changelog](https://github.com/googleapis/google-cloud-python/blob/main/packages/google-cloud-documentai/CHANGELOG.md)
- [Commits](https://github.com/googleapis/google-cloud-python/compare/google-cloud-dataproc-v5.7.0...google-cloud-dataproc-v5.8.0)

---
updated-dependencies:
- dependency-name: google-cloud-dataproc
  dependency-type: direct:development
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`8738462`](https://github.com/opentargets/genetics_etl_python/commit/87384622efdf1f9a7cfbfa8d842c1774c63b2426))

* build(deps): bump typing-extensions from 4.8.0 to 4.9.0 (#317)

Bumps [typing-extensions](https://github.com/python/typing_extensions) from 4.8.0 to 4.9.0.
- [Release notes](https://github.com/python/typing_extensions/releases)
- [Changelog](https://github.com/python/typing_extensions/blob/main/CHANGELOG.md)
- [Commits](https://github.com/python/typing_extensions/compare/4.8.0...4.9.0)

---
updated-dependencies:
- dependency-name: typing-extensions
  dependency-type: direct:production
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`6d092ae`](https://github.com/opentargets/genetics_etl_python/commit/6d092ae39424868111e7b98a488ea625744ec9ea))

* build(deps): bump numpy from 1.26.1 to 1.26.2 (#314)

Bumps [numpy](https://github.com/numpy/numpy) from 1.26.1 to 1.26.2.
- [Release notes](https://github.com/numpy/numpy/releases)
- [Changelog](https://github.com/numpy/numpy/blob/main/doc/RELEASE_WALKTHROUGH.rst)
- [Commits](https://github.com/numpy/numpy/compare/v1.26.1...v1.26.2)

---
updated-dependencies:
- dependency-name: numpy
  dependency-type: direct:production
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`c98ae80`](https://github.com/opentargets/genetics_etl_python/commit/c98ae80c30134b84a8f66e6f0342549a191e7662))

* build(deps-dev): bump pre-commit from 3.5.0 to 3.6.0 (#316)

Bumps [pre-commit](https://github.com/pre-commit/pre-commit) from 3.5.0 to 3.6.0.
- [Release notes](https://github.com/pre-commit/pre-commit/releases)
- [Changelog](https://github.com/pre-commit/pre-commit/blob/main/CHANGELOG.md)
- [Commits](https://github.com/pre-commit/pre-commit/compare/v3.5.0...v3.6.0)

---
updated-dependencies:
- dependency-name: pre-commit
  dependency-type: direct:development
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`3f25441`](https://github.com/opentargets/genetics_etl_python/commit/3f2544126bdc32976de11e250ae22bc0ccf9396c))

* build(deps-dev): bump mkdocs-material from 9.4.14 to 9.5.2 (#324)

Bumps [mkdocs-material](https://github.com/squidfunk/mkdocs-material) from 9.4.14 to 9.5.2.
- [Release notes](https://github.com/squidfunk/mkdocs-material/releases)
- [Changelog](https://github.com/squidfunk/mkdocs-material/blob/master/CHANGELOG)
- [Commits](https://github.com/squidfunk/mkdocs-material/compare/9.4.14...9.5.2)

---
updated-dependencies:
- dependency-name: mkdocs-material
  dependency-type: direct:development
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;
Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt; ([`a2e4e4d`](https://github.com/opentargets/genetics_etl_python/commit/a2e4e4de6bf45439224d8036bfdf9ad59463a45c))

* build(deps): bump wandb from 0.16.0 to 0.16.1 (#315)

Bumps [wandb](https://github.com/wandb/wandb) from 0.16.0 to 0.16.1.
- [Release notes](https://github.com/wandb/wandb/releases)
- [Changelog](https://github.com/wandb/wandb/blob/main/CHANGELOG.md)
- [Commits](https://github.com/wandb/wandb/compare/v0.16.0...v0.16.1)

---
updated-dependencies:
- dependency-name: wandb
  dependency-type: direct:production
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;
Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`883a498`](https://github.com/opentargets/genetics_etl_python/commit/883a4989b58f4f423f83d90703c605e41ba2ee44))

* build(deps-dev): bump apache-airflow-providers-google (#302)

Bumps [apache-airflow-providers-google](https://github.com/apache/airflow) from 10.11.1 to 10.12.0.
- [Release notes](https://github.com/apache/airflow/releases)
- [Changelog](https://github.com/apache/airflow/blob/main/RELEASE_NOTES.rst)
- [Commits](https://github.com/apache/airflow/compare/providers-google/10.11.1...providers-google/10.12.0)

---
updated-dependencies:
- dependency-name: apache-airflow-providers-google
  dependency-type: direct:development
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`a603aad`](https://github.com/opentargets/genetics_etl_python/commit/a603aadb2f85601b4f493b4c6a8c3cf3eb4b954a))

* build(deps-dev): bump mkdocs-git-committers-plugin-2 from 1.2.0 to 2.2.2 (#303)

Bumps [mkdocs-git-committers-plugin-2](https://github.com/ojacques/mkdocs-git-committers-plugin-2) from 1.2.0 to 2.2.2.
- [Release notes](https://github.com/ojacques/mkdocs-git-committers-plugin-2/releases)
- [Commits](https://github.com/ojacques/mkdocs-git-committers-plugin-2/compare/1.2.0...2.2.2)

---
updated-dependencies:
- dependency-name: mkdocs-git-committers-plugin-2
  dependency-type: direct:development
  update-type: version-update:semver-major
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`b887f6e`](https://github.com/opentargets/genetics_etl_python/commit/b887f6e90b526042de731f18bed60d8cc5d297f3))

* build(deps-dev): bump pymdown-extensions from 10.3.1 to 10.5 (#301)

Bumps [pymdown-extensions](https://github.com/facelessuser/pymdown-extensions) from 10.3.1 to 10.5.
- [Release notes](https://github.com/facelessuser/pymdown-extensions/releases)
- [Commits](https://github.com/facelessuser/pymdown-extensions/compare/10.3.1...10.5)

---
updated-dependencies:
- dependency-name: pymdown-extensions
  dependency-type: direct:development
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`624a7fb`](https://github.com/opentargets/genetics_etl_python/commit/624a7fb111c1735c2bc754e755df3a8c27972500))

* build(deps): bump scipy from 1.11.3 to 1.11.4 (#299)

Bumps [scipy](https://github.com/scipy/scipy) from 1.11.3 to 1.11.4.
- [Release notes](https://github.com/scipy/scipy/releases)
- [Commits](https://github.com/scipy/scipy/compare/v1.11.3...v1.11.4)

---
updated-dependencies:
- dependency-name: scipy
  dependency-type: direct:production
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`98bb2e9`](https://github.com/opentargets/genetics_etl_python/commit/98bb2e9f60e01d087e6fd104b08d8491d75cc036))

* build(deps-dev): bump mkdocs-material from 9.4.10 to 9.4.14 (#300)

Bumps [mkdocs-material](https://github.com/squidfunk/mkdocs-material) from 9.4.10 to 9.4.14.
- [Release notes](https://github.com/squidfunk/mkdocs-material/releases)
- [Changelog](https://github.com/squidfunk/mkdocs-material/blob/master/CHANGELOG)
- [Commits](https://github.com/squidfunk/mkdocs-material/compare/9.4.10...9.4.14)

---
updated-dependencies:
- dependency-name: mkdocs-material
  dependency-type: direct:development
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`a684d12`](https://github.com/opentargets/genetics_etl_python/commit/a684d12cb582d62a3ae8a05d193856de7496e182))

* build(deps-dev): bump mypy from 1.7.0 to 1.7.1 (#278)

Bumps [mypy](https://github.com/python/mypy) from 1.7.0 to 1.7.1.
- [Changelog](https://github.com/python/mypy/blob/master/CHANGELOG.md)
- [Commits](https://github.com/python/mypy/compare/v1.7.0...v1.7.1)

---
updated-dependencies:
- dependency-name: mypy
  dependency-type: direct:development
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`a4a44da`](https://github.com/opentargets/genetics_etl_python/commit/a4a44da2be20e907e5f6f4bc4967afb01edc2e90))

* build(deps-dev): bump ruff from 0.1.3 to 0.1.6 (#276)

Bumps [ruff](https://github.com/astral-sh/ruff) from 0.1.3 to 0.1.6.
- [Release notes](https://github.com/astral-sh/ruff/releases)
- [Changelog](https://github.com/astral-sh/ruff/blob/main/CHANGELOG.md)
- [Commits](https://github.com/astral-sh/ruff/compare/v0.1.3...v0.1.6)

---
updated-dependencies:
- dependency-name: ruff
  dependency-type: direct:development
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`90c9ad3`](https://github.com/opentargets/genetics_etl_python/commit/90c9ad3b6b100681a5840695655f351da1b5e5a2))

* build(deps-dev): bump mkdocstrings-python from 1.7.4 to 1.7.5 (#279)

Bumps [mkdocstrings-python](https://github.com/mkdocstrings/python) from 1.7.4 to 1.7.5.
- [Release notes](https://github.com/mkdocstrings/python/releases)
- [Changelog](https://github.com/mkdocstrings/python/blob/main/CHANGELOG.md)
- [Commits](https://github.com/mkdocstrings/python/compare/1.7.4...1.7.5)

---
updated-dependencies:
- dependency-name: mkdocstrings-python
  dependency-type: direct:development
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`feaf1af`](https://github.com/opentargets/genetics_etl_python/commit/feaf1afd1604f6bbd7d76d9187fb3a36fc07c928))

* build(deps-dev): bump ipython from 8.17.2 to 8.18.1 (#280)

Bumps [ipython](https://github.com/ipython/ipython) from 8.17.2 to 8.18.1.
- [Release notes](https://github.com/ipython/ipython/releases)
- [Commits](https://github.com/ipython/ipython/compare/8.17.2...8.18.1)

---
updated-dependencies:
- dependency-name: ipython
  dependency-type: direct:development
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt; ([`aa05aa5`](https://github.com/opentargets/genetics_etl_python/commit/aa05aa51375fe1c22842e07da0a7565b2cc3a1c7))

* build(deps): bump pyarrow from 11.0.0 to 14.0.1

Bumps [pyarrow](https://github.com/apache/arrow) from 11.0.0 to 14.0.1.
- [Commits](https://github.com/apache/arrow/compare/go/v11.0.0...go/v14.0.1)

---
updated-dependencies:
- dependency-name: pyarrow
  dependency-type: direct:production
  update-type: version-update:semver-major
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`5f7d928`](https://github.com/opentargets/genetics_etl_python/commit/5f7d928451beaf8734442ac882df752fa40e8749))

* build(deps-dev): bump mkdocstrings-python from 1.7.3 to 1.7.4

Bumps [mkdocstrings-python](https://github.com/mkdocstrings/python) from 1.7.3 to 1.7.4.
- [Release notes](https://github.com/mkdocstrings/python/releases)
- [Changelog](https://github.com/mkdocstrings/python/blob/main/CHANGELOG.md)
- [Commits](https://github.com/mkdocstrings/python/compare/1.7.3...1.7.4)

---
updated-dependencies:
- dependency-name: mkdocstrings-python
  dependency-type: direct:development
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`d407968`](https://github.com/opentargets/genetics_etl_python/commit/d407968c836d091ef48e31949d921a4127c6705e))

* build(deps-dev): bump mkdocs-material from 9.4.8 to 9.4.10

Bumps [mkdocs-material](https://github.com/squidfunk/mkdocs-material) from 9.4.8 to 9.4.10.
- [Release notes](https://github.com/squidfunk/mkdocs-material/releases)
- [Changelog](https://github.com/squidfunk/mkdocs-material/blob/master/CHANGELOG)
- [Commits](https://github.com/squidfunk/mkdocs-material/compare/9.4.8...9.4.10)

---
updated-dependencies:
- dependency-name: mkdocs-material
  dependency-type: direct:development
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`ad2762b`](https://github.com/opentargets/genetics_etl_python/commit/ad2762bed7b0a786ecca957436ce4a8cff133d53))

* build(deps-dev): bump mypy from 1.6.1 to 1.7.0

Bumps [mypy](https://github.com/python/mypy) from 1.6.1 to 1.7.0.
- [Changelog](https://github.com/python/mypy/blob/master/CHANGELOG.md)
- [Commits](https://github.com/python/mypy/compare/v1.6.1...v1.7.0)

---
updated-dependencies:
- dependency-name: mypy
  dependency-type: direct:development
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`d780406`](https://github.com/opentargets/genetics_etl_python/commit/d78040626f8b46f8029061a7cb86c1727f019650))

* build(deps-dev): bump apache-airflow-providers-google

Bumps [apache-airflow-providers-google](https://github.com/apache/airflow) from 10.11.0 to 10.11.1.
- [Release notes](https://github.com/apache/airflow/releases)
- [Changelog](https://github.com/apache/airflow/blob/main/RELEASE_NOTES.rst)
- [Commits](https://github.com/apache/airflow/compare/providers-google/10.11.0...providers-google/10.11.1)

---
updated-dependencies:
- dependency-name: apache-airflow-providers-google
  dependency-type: direct:development
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`456a9fb`](https://github.com/opentargets/genetics_etl_python/commit/456a9fbb96e13937d0d3c29cc1f058272b9095e8))

* build(deps-dev): bump google-cloud-dataproc from 5.6.0 to 5.7.0

Bumps [google-cloud-dataproc](https://github.com/googleapis/google-cloud-python) from 5.6.0 to 5.7.0.
- [Release notes](https://github.com/googleapis/google-cloud-python/releases)
- [Changelog](https://github.com/googleapis/google-cloud-python/blob/main/CHANGELOG.md)
- [Commits](https://github.com/googleapis/google-cloud-python/compare/google-cloud-dataproc-v5.6.0...google-cloud-dataproc-v5.7.0)

---
updated-dependencies:
- dependency-name: google-cloud-dataproc
  dependency-type: direct:development
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`766e02f`](https://github.com/opentargets/genetics_etl_python/commit/766e02f0ecde56a8f0e7915892e4d0410e5b1636))

* build(deps): bump wandb from 0.13.11 to 0.16.0

Bumps [wandb](https://github.com/wandb/wandb) from 0.13.11 to 0.16.0.
- [Release notes](https://github.com/wandb/wandb/releases)
- [Changelog](https://github.com/wandb/wandb/blob/main/CHANGELOG.md)
- [Commits](https://github.com/wandb/wandb/compare/v0.13.11...v0.16.0)

---
updated-dependencies:
- dependency-name: wandb
  dependency-type: direct:production
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`a2ba62c`](https://github.com/opentargets/genetics_etl_python/commit/a2ba62c480d543e037bd5436c962ca799c4773c6))

* build(deps-dev): bump pytest-xdist from 3.3.1 to 3.4.0

Bumps [pytest-xdist](https://github.com/pytest-dev/pytest-xdist) from 3.3.1 to 3.4.0.
- [Changelog](https://github.com/pytest-dev/pytest-xdist/blob/master/CHANGELOG.rst)
- [Commits](https://github.com/pytest-dev/pytest-xdist/compare/v3.3.1...v3.4.0)

---
updated-dependencies:
- dependency-name: pytest-xdist
  dependency-type: direct:development
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`09c4b32`](https://github.com/opentargets/genetics_etl_python/commit/09c4b322004b028218c06cf2da8ca395e5b4c3a8))

* build(deps-dev): bump mkdocs-material from 9.4.7 to 9.4.8

Bumps [mkdocs-material](https://github.com/squidfunk/mkdocs-material) from 9.4.7 to 9.4.8.
- [Release notes](https://github.com/squidfunk/mkdocs-material/releases)
- [Changelog](https://github.com/squidfunk/mkdocs-material/blob/master/CHANGELOG)
- [Commits](https://github.com/squidfunk/mkdocs-material/compare/9.4.7...9.4.8)

---
updated-dependencies:
- dependency-name: mkdocs-material
  dependency-type: direct:development
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`eb82c88`](https://github.com/opentargets/genetics_etl_python/commit/eb82c88e5ae89cb8654eecf03a68919f720f8182))

* build(deps): bump hail from 0.2.122 to 0.2.126

Bumps [hail](https://github.com/hail-is/hail) from 0.2.122 to 0.2.126.
- [Release notes](https://github.com/hail-is/hail/releases)
- [Commits](https://github.com/hail-is/hail/compare/0.2.122...0.2.126)

---
updated-dependencies:
- dependency-name: hail
  dependency-type: direct:production
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`b83a68d`](https://github.com/opentargets/genetics_etl_python/commit/b83a68d2fe29418acd124d5607b73ecfcdcd1409))

* build(deps-dev): bump apache-airflow from 2.7.2 to 2.7.3

Bumps [apache-airflow](https://github.com/apache/airflow) from 2.7.2 to 2.7.3.
- [Release notes](https://github.com/apache/airflow/releases)
- [Changelog](https://github.com/apache/airflow/blob/2.7.3/RELEASE_NOTES.rst)
- [Commits](https://github.com/apache/airflow/compare/2.7.2...2.7.3)

---
updated-dependencies:
- dependency-name: apache-airflow
  dependency-type: direct:development
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`d3a919f`](https://github.com/opentargets/genetics_etl_python/commit/d3a919f19b0fff24cf2afed11fc6dee60011c0a6))

* build(deps-dev): bump pre-commit from 2.21.0 to 3.5.0

Bumps [pre-commit](https://github.com/pre-commit/pre-commit) from 2.21.0 to 3.5.0.
- [Release notes](https://github.com/pre-commit/pre-commit/releases)
- [Changelog](https://github.com/pre-commit/pre-commit/blob/main/CHANGELOG.md)
- [Commits](https://github.com/pre-commit/pre-commit/compare/v2.21.0...v3.5.0)

---
updated-dependencies:
- dependency-name: pre-commit
  dependency-type: direct:development
  update-type: version-update:semver-major
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`43cd825`](https://github.com/opentargets/genetics_etl_python/commit/43cd825ec5f8572ad8327e15b2923d057bd45b50))

* build(deps-dev): bump mkdocs-minify-plugin from 0.5.0 to 0.7.1

Bumps [mkdocs-minify-plugin](https://github.com/byrnereese/mkdocs-minify-plugin) from 0.5.0 to 0.7.1.
- [Release notes](https://github.com/byrnereese/mkdocs-minify-plugin/releases)
- [Commits](https://github.com/byrnereese/mkdocs-minify-plugin/compare/0.5.0...0.7.1)

---
updated-dependencies:
- dependency-name: mkdocs-minify-plugin
  dependency-type: direct:development
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`3761ca2`](https://github.com/opentargets/genetics_etl_python/commit/3761ca2b30ff782dd5a02f37b8785782b03eae95))

* build(deps-dev): bump mkdocs-material from 9.4.6 to 9.4.7

Bumps [mkdocs-material](https://github.com/squidfunk/mkdocs-material) from 9.4.6 to 9.4.7.
- [Release notes](https://github.com/squidfunk/mkdocs-material/releases)
- [Changelog](https://github.com/squidfunk/mkdocs-material/blob/master/CHANGELOG)
- [Commits](https://github.com/squidfunk/mkdocs-material/compare/9.4.6...9.4.7)

---
updated-dependencies:
- dependency-name: mkdocs-material
  dependency-type: direct:development
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`71b7621`](https://github.com/opentargets/genetics_etl_python/commit/71b7621fd1a0cb44f7dc4e998bfb00891fac50cf))

* build(deps): bump apache/airflow in /src/airflow

Bumps apache/airflow from 2.7.1-python3.10 to 2.7.2-python3.10.

---
updated-dependencies:
- dependency-name: apache/airflow
  dependency-type: direct:production
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`57612c8`](https://github.com/opentargets/genetics_etl_python/commit/57612c807ae4b30d218c201034b577a1c5262c6d))

* build(deps-dev): bump ruff from 0.0.287 to 0.1.3

Bumps [ruff](https://github.com/astral-sh/ruff) from 0.0.287 to 0.1.3.
- [Release notes](https://github.com/astral-sh/ruff/releases)
- [Changelog](https://github.com/astral-sh/ruff/blob/main/CHANGELOG.md)
- [Commits](https://github.com/astral-sh/ruff/compare/v0.0.287...v0.1.3)

---
updated-dependencies:
- dependency-name: ruff
  dependency-type: direct:development
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`94faa5b`](https://github.com/opentargets/genetics_etl_python/commit/94faa5b74be820416916ff7849e1360640913f6c))

* build: add pydoclint to the project ([`1f9d830`](https://github.com/opentargets/genetics_etl_python/commit/1f9d830af22dbc4ddc3f24b5fe959957744f0a0c))

* build(deps-dev): bump apache-airflow-providers-google

Bumps [apache-airflow-providers-google](https://github.com/apache/airflow) from 10.10.0 to 10.10.1.
- [Release notes](https://github.com/apache/airflow/releases)
- [Changelog](https://github.com/apache/airflow/blob/main/RELEASE_NOTES.rst)
- [Commits](https://github.com/apache/airflow/compare/providers-google/10.10.0...providers-google/10.10.1)

---
updated-dependencies:
- dependency-name: apache-airflow-providers-google
  dependency-type: direct:development
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`d310e6a`](https://github.com/opentargets/genetics_etl_python/commit/d310e6aabaaab325b3fec88d4cd1fbfdf79e3150))

* build(deps-dev): bump mkdocs-autolinks-plugin from 0.6.0 to 0.7.1

Bumps [mkdocs-autolinks-plugin](https://github.com/midnightprioriem/mkdocs-autolinks-plugin) from 0.6.0 to 0.7.1.
- [Release notes](https://github.com/midnightprioriem/mkdocs-autolinks-plugin/releases)
- [Commits](https://github.com/midnightprioriem/mkdocs-autolinks-plugin/commits)

---
updated-dependencies:
- dependency-name: mkdocs-autolinks-plugin
  dependency-type: direct:development
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`a2f0189`](https://github.com/opentargets/genetics_etl_python/commit/a2f018922cefe1e5b9da06832ff2a2b0181c0c81))

* build(deps-dev): bump mypy from 0.971 to 1.6.1

Bumps [mypy](https://github.com/python/mypy) from 0.971 to 1.6.1.
- [Changelog](https://github.com/python/mypy/blob/master/CHANGELOG.md)
- [Commits](https://github.com/python/mypy/compare/v0.971...v1.6.1)

---
updated-dependencies:
- dependency-name: mypy
  dependency-type: direct:development
  update-type: version-update:semver-major
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`389ad4d`](https://github.com/opentargets/genetics_etl_python/commit/389ad4d61a17b310ca5fd2df067e637cda5b2efc))

* build(deps): bump pyspark from 3.3.0 to 3.3.3

Bumps [pyspark](https://github.com/apache/spark) from 3.3.0 to 3.3.3.
- [Commits](https://github.com/apache/spark/compare/v3.3.0...v3.3.3)

---
updated-dependencies:
- dependency-name: pyspark
  dependency-type: direct:production
  update-type: version-update:semver-patch
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`9eda90b`](https://github.com/opentargets/genetics_etl_python/commit/9eda90b071fdeb92b7840a02d5b8d8b12e137f39))

* build: lock hail version to 0.2.122 to fix #3088 ([`4846668`](https://github.com/opentargets/genetics_etl_python/commit/484666872d3f9f605628eeeb11f53f16df6eb1fd))

* build: lock hail version to 0.2.122 to fix #3088 ([`9327e53`](https://github.com/opentargets/genetics_etl_python/commit/9327e53c772119bb02135205ae90dbf8d6b2e5e2))

* build(deps-dev): bump apache-airflow-providers-google

Bumps [apache-airflow-providers-google](https://github.com/apache/airflow) from 10.6.0 to 10.10.0.
- [Release notes](https://github.com/apache/airflow/releases)
- [Changelog](https://github.com/apache/airflow/blob/main/RELEASE_NOTES.rst)
- [Commits](https://github.com/apache/airflow/compare/providers-google/10.6.0...providers-google/10.10.0)

---
updated-dependencies:
- dependency-name: apache-airflow-providers-google
  dependency-type: direct:development
  update-type: version-update:semver-minor
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`2a1a168`](https://github.com/opentargets/genetics_etl_python/commit/2a1a16807c77391cf0377108a846378a35b9ad26))

* build: upgrade mkdocstrings-python from 0.7.1 to 1.7.3 ([`5f74151`](https://github.com/opentargets/genetics_etl_python/commit/5f74151062c78dbeba842427bb2ea5243a6085ae))

* build: add scikit-learn ([`20da3ba`](https://github.com/opentargets/genetics_etl_python/commit/20da3ba64d51aad1b387bf82a98ea0cde442be05))

* build: add `extensions.json` to vscode conf ([`3bf9b2f`](https://github.com/opentargets/genetics_etl_python/commit/3bf9b2f3cf3ac2e5c064a6941ddf2827fc24345c))

* build(session): assign driver and exec memory based on resources ([`9c8bc12`](https://github.com/opentargets/genetics_etl_python/commit/9c8bc1280da69f4c3814a6051a869ee04c46e8a9))

* build(ldindex): apply to all populations ([`18c18f2`](https://github.com/opentargets/genetics_etl_python/commit/18c18f2d42112bff0f478bb5ec2c2cfe14502cef))

* build: update configuration for new ldindex ([`7df214b`](https://github.com/opentargets/genetics_etl_python/commit/7df214bbe4889efd0dc8868378575bafc18f7fd4))

* build: downgrade project version to 0.1.4 ([`93406d9`](https://github.com/opentargets/genetics_etl_python/commit/93406d9343466dc0b773ec5939bf3e6e823d263c))

* build: downgrade from 3.10.9 to 3.10.8 for compatibility w/ dataproc ([`d2f96bb`](https://github.com/opentargets/genetics_etl_python/commit/d2f96bb7d44a396ae459d57c6b70853667515cc0))

* build: update gitignore ([`3ed4482`](https://github.com/opentargets/genetics_etl_python/commit/3ed4482b1c5a852646f50de541f36d5e2ea8d3df))

* build: update mypy to 1.2.0 ([`0d5c6e9`](https://github.com/opentargets/genetics_etl_python/commit/0d5c6e9e9f6592f4b005e5feef43c550c3535f2d))

* build: add scikit-learn ([`2fc9032`](https://github.com/opentargets/genetics_etl_python/commit/2fc9032bb77c115b63407e93b4410bec083ea6e2))

* build: add wandb and xgboost to the project ([`fca2231`](https://github.com/opentargets/genetics_etl_python/commit/fca223122e161802ad3bacad8285d1e4a708b13e))

* build: update python to 3.8.15 ([`6c4713b`](https://github.com/opentargets/genetics_etl_python/commit/6c4713b46a4ff292e9e96efd95f2c78601e49e64))

* build: update python to 3.8.15 ([`c794692`](https://github.com/opentargets/genetics_etl_python/commit/c7946925076d57cf98464a1ff77b49fe95cd3692))

* build: new target schema with the new `canonicalTranscript` ([`80d79cb`](https://github.com/opentargets/genetics_etl_python/commit/80d79cbc325c5b2e6716b7c030b2fbfc70929f4c))

* build: pipeline run with most up to date gene index ([`f98a987`](https://github.com/opentargets/genetics_etl_python/commit/f98a9874fb409d223b48aa2b06c8989c1003e875))

* build: add pandas as a test dependency ([`9a75890`](https://github.com/opentargets/genetics_etl_python/commit/9a7589031662c50c2910b78b9023d5e69dd256e1))

* build: add pandas as a test dependency ([`5ff7ccd`](https://github.com/opentargets/genetics_etl_python/commit/5ff7ccd3608069dd07bbe0fcf35f60f781d41157))

### Chore

* chore: upgrade checkout (#346) ([`e92e1e5`](https://github.com/opentargets/genetics_etl_python/commit/e92e1e5af62828170752c241b594f2f67d78a7e9))

* chore: delete makefile_deprecated (#329)

Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt; ([`0bd02aa`](https://github.com/opentargets/genetics_etl_python/commit/0bd02aa7653382625d5c019fa124593a4cc29963))

* chore: review study locus and study index configs (#326)

* chore: make studylocus and study indices configs clearer

* chore: temporarily turn off removal of redundancies due to perf

* refactor: read studyindex and studylocus recursively ([`7dfce61`](https://github.com/opentargets/genetics_etl_python/commit/7dfce618a32eb4e09936f1f4abc656ced3098a26))

* chore: create code of conduct (#327)

* Create CODE_OF_CONDUCT.md

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci

---------

Co-authored-by: pre-commit-ci[bot] &lt;66853113+pre-commit-ci[bot]@users.noreply.github.com&gt; ([`923684c`](https://github.com/opentargets/genetics_etl_python/commit/923684c9437f47d95ddac948c0a6850b791cecac))

* chore: add `l2g_benchmark` notebook to compare with production results (#323) ([`7a076ad`](https://github.com/opentargets/genetics_etl_python/commit/7a076ad702a94cb188fb7381e67bb64b164dea52))

* chore(deps): bump actions/setup-python from 4 to 5 (#319)

Bumps [actions/setup-python](https://github.com/actions/setup-python) from 4 to 5.
- [Release notes](https://github.com/actions/setup-python/releases)
- [Commits](https://github.com/actions/setup-python/compare/v4...v5)

---
updated-dependencies:
- dependency-name: actions/setup-python
  dependency-type: direct:production
  update-type: version-update:semver-major
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
Co-authored-by: dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;
Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt; ([`8f58ec6`](https://github.com/opentargets/genetics_etl_python/commit/8f58ec6edb8575e88e5fa3c7d790ae03e9144612))

* chore(airflow): schedule_interval deprecation warning (#293)

Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt; ([`f2f8399`](https://github.com/opentargets/genetics_etl_python/commit/f2f83998ccc95ca6a57be398b933651a8d341a02))

* chore(l2ggoldstandard): add studyId to schema (#305)

* chore(l2ggoldstandard): add studyId to schema

* fix: add `studyId` to gold standards testing fixtures

---------

Co-authored-by: David Ochoa &lt;ochoa@ebi.ac.uk&gt; ([`52784dc`](https://github.com/opentargets/genetics_etl_python/commit/52784dc7647e9d05fec4681456b476efde940795))

* chore: rename study_locus to credible_set for l2g ([`4e4e4f5`](https://github.com/opentargets/genetics_etl_python/commit/4e4e4f5d45af2574d4e4af38d0924896166182da))

* chore: remove reference to confidence intervals ([`4635b99`](https://github.com/opentargets/genetics_etl_python/commit/4635b99a6d91bc09ae38456f62b53389e2b7be79))

* chore: remove beta value interval calculation ([`13de75f`](https://github.com/opentargets/genetics_etl_python/commit/13de75fd239e2b445928cbbf8ffa32d692f8bbdc))

* chore: pre-commit autoupdate ([`ebf80e8`](https://github.com/opentargets/genetics_etl_python/commit/ebf80e8f209f7499d18706e8983f2138f016890a))

* chore(gold_standards): define gs labels as `L2GGoldStandard` attributes ([`2f13b3b`](https://github.com/opentargets/genetics_etl_python/commit/2f13b3b76015f610a2a6d2b6cf5b5bf715da6dfd))

* chore(overlaps): chromosome and statistics are not mandatory fields in the schema ([`dc7c423`](https://github.com/opentargets/genetics_etl_python/commit/dc7c423d6f668f7c8b8d4a072a85058beaf21a76))

* chore: change `sources` in gold standards schema to a nullable ([`c75a663`](https://github.com/opentargets/genetics_etl_python/commit/c75a6634bf55113c3bff15fbab2d169778c07eb7))

* chore: add `variantId` to gold standards schema ([`6a33976`](https://github.com/opentargets/genetics_etl_python/commit/6a339761e80361ee3aa5a8ae1fb40bf785b665cf))

* chore: make local SSDs a default ([`293b6ba`](https://github.com/opentargets/genetics_etl_python/commit/293b6ba8e6bbfce8d1d83b9791f8e19db795c3c4))

* chore: align default values with docstring ([`d499839`](https://github.com/opentargets/genetics_etl_python/commit/d4998395e20c6bb838c2f9b38283a9f056eecb9e))

* chore: remove the num_local_ssds arg which has no effect ([`bb85654`](https://github.com/opentargets/genetics_etl_python/commit/bb85654b85f9cc33b4a1a87aa0e0d552798a4b80))

* chore: repartition data before processing ([`f81f617`](https://github.com/opentargets/genetics_etl_python/commit/f81f617ca6391aa5d6656f06e8c77bdd6e2f2ac7))

* chore: read input data from Google Storage ([`d467f79`](https://github.com/opentargets/genetics_etl_python/commit/d467f797f8b7e31aca60219ce11983c5a5ce4f6f))

* chore: partition output data by chromosome ([`f741006`](https://github.com/opentargets/genetics_etl_python/commit/f741006964f623f9bf2c67ae74ce59514e2f8049))

* chore: replace attributes with static methods for eQTL summary stats ([`01878b8`](https://github.com/opentargets/genetics_etl_python/commit/01878b87c05db7f89a0ff70414819c3d7099e106))

* chore: replace attributes with static methods for eQTL study index ([`f3b87b3`](https://github.com/opentargets/genetics_etl_python/commit/f3b87b3d452f6af31dff7593fd1b7962332ef871))

* chore: add __init__.py for eQTL Catalogue ([`72e8e50`](https://github.com/opentargets/genetics_etl_python/commit/72e8e50637d7a17240703d01470f44ef56c6bc97))

* chore: unify FinnGen config with eQTL Catalogue ([`9e312e6`](https://github.com/opentargets/genetics_etl_python/commit/9e312e6fe2e41f2c835c7c97eabe980be22d1d66))

* chore: add configuration ([`f7e7e3d`](https://github.com/opentargets/genetics_etl_python/commit/f7e7e3df90783114e02c1901884e75a8b2257ce6))

* chore: always error if the output data exists ([`6ae28bf`](https://github.com/opentargets/genetics_etl_python/commit/6ae28bfdeb8945f872ad5cd11f9415ea3a7b020d))

* chore: use more partitions for FinnGen ([`871f568`](https://github.com/opentargets/genetics_etl_python/commit/871f5684a9738a4ec36ca5f0ff9e1f1ec84ef298))

* chore: use gs:// prefix for FinnGen input data ([`8d5ab8a`](https://github.com/opentargets/genetics_etl_python/commit/8d5ab8a861a62b95a0e4bb32c770ec8e7503a84b))

* chore: use a higher RAM master machine ([`66c54c8`](https://github.com/opentargets/genetics_etl_python/commit/66c54c8d6ecf206655c20636cd4cb90a9ed8bea1))

* chore: changes in config ([`cf2be5b`](https://github.com/opentargets/genetics_etl_python/commit/cf2be5b7568926de8bcbdc2737a22d650e633a7c))

* chore: delete local chain file

bug reported here https://github.com/hail-is/hail/issues/13993 ([`dfbd943`](https://github.com/opentargets/genetics_etl_python/commit/dfbd9439c9431202eb4e62c8af12ac44e21b6daf))

* chore(config): remove gnomad datasets from `gcp.yaml` ([`b7e5c55`](https://github.com/opentargets/genetics_etl_python/commit/b7e5c55a598441fb88b0beb70a3bb188a316dd25))

* chore: do not set start_hail explicitly ([`54444dd`](https://github.com/opentargets/genetics_etl_python/commit/54444ddfdc34a9d0741b6a203c5e8b7100129a4f))

* chore: merge main ([`646ee08`](https://github.com/opentargets/genetics_etl_python/commit/646ee08b9d2ce9f073a447d48e0df148e01bff36))

* chore: unnecessary test ([`e0904e3`](https://github.com/opentargets/genetics_etl_python/commit/e0904e35be2870af1b8c76760cd2a1c80a66d7f5))

* chore: set default retries to 1 in dev setting ([`ea2ae2d`](https://github.com/opentargets/genetics_etl_python/commit/ea2ae2d92ef3bade7983d6cec4e1aef07ccd86d8))

* chore: set the number of workers to 2 (min) ([`557c275`](https://github.com/opentargets/genetics_etl_python/commit/557c2757b4b75964023fb04e382e48a89d54c43f))

* chore: remove empty AIRFLOW_ID= from .env ([`d569c54`](https://github.com/opentargets/genetics_etl_python/commit/d569c5480b1056854f1d56bd88508243fd6c4326))

* chore: leave AIRFLOW_UID empty by default ([`6014e99`](https://github.com/opentargets/genetics_etl_python/commit/6014e99f199dac5ecbb0260b59fbf89d2e619757))

* chore: update base Airflow version in Dockerfile ([`6acaae9`](https://github.com/opentargets/genetics_etl_python/commit/6acaae9b43028878367b4ddf7a307b835081b118))

* chore: remove the __init__.py which is no longer needed ([`cc3a1e5`](https://github.com/opentargets/genetics_etl_python/commit/cc3a1e58ed4080d8d7db89d8d8c04bdab49fc950))

* chore(dag): add l2g and overlaps steps to `dag.yaml` ([`3687b66`](https://github.com/opentargets/genetics_etl_python/commit/3687b663fe0d566dfb13b56910f78bfa83ad52c2))

* chore: move L2GFeature to datasets ([`27be82b`](https://github.com/opentargets/genetics_etl_python/commit/27be82bd195ed60a6de25fe1ddeccf01197e60f2))

* chore: merge main ([`a3579f3`](https://github.com/opentargets/genetics_etl_python/commit/a3579f3e3ccf606cbb0ce610220b134cd39af5e9))

* chore: remove commented code ([`615af04`](https://github.com/opentargets/genetics_etl_python/commit/615af04d958a3dd7a1bde41f792feef355b5547b))

* chore: merge main ([`df3e982`](https://github.com/opentargets/genetics_etl_python/commit/df3e982da5f760a583c6edc3524756cf705e9259))

* chore: extend docker image with `psycopg2-binary` for postgres set up ([`d860f21`](https://github.com/opentargets/genetics_etl_python/commit/d860f2157632e9ed06ecec08966027eca92e4100))

* chore: pin `apache-airflow-providers-google` version ([`87d163d`](https://github.com/opentargets/genetics_etl_python/commit/87d163dca579e07de766591f590499db49839fe1))

* chore: merge main ([`e9d7d7b`](https://github.com/opentargets/genetics_etl_python/commit/e9d7d7b833f833d4305ed7c9619bcffd5c622ca5))

* chore: comment out failing airflow dag ([`c2ffd01`](https://github.com/opentargets/genetics_etl_python/commit/c2ffd015293833c699ae32ac4ac42800c2742b35))

* chore: add `pydoclint` to pre-commit ([`aad98a4`](https://github.com/opentargets/genetics_etl_python/commit/aad98a46133f7d7eb1891e43970d14224b1d389d))

* chore(l2g): set to train by default ([`e44872e`](https://github.com/opentargets/genetics_etl_python/commit/e44872e1577d34d7df1c471cbc57c74ab4bd7277))

* chore: delete l2g_benchmark ([`f13f401`](https://github.com/opentargets/genetics_etl_python/commit/f13f401fa93c197f214aab1cd0de56d37e5ad0c4))

* chore: remove unnecessary paths in gitignore ([`9894b50`](https://github.com/opentargets/genetics_etl_python/commit/9894b50ce8a68a167d9aa2d2ff63d7f7a4b07557))

* chore: rename l2g config ([`a8b48fc`](https://github.com/opentargets/genetics_etl_python/commit/a8b48fc5f00c22706b3347d9a2ebaafacb09f243))

* chore: merge main ([`ea9f3b2`](https://github.com/opentargets/genetics_etl_python/commit/ea9f3b25e79c8f324d79bf2a3509fd05ea6ac7df))

* chore(env): update lock file ([`0b4653d`](https://github.com/opentargets/genetics_etl_python/commit/0b4653d3b392c97332cf07ee5641235ac16b2457))

* chore(env): update lock file ([`0c7e6bc`](https://github.com/opentargets/genetics_etl_python/commit/0c7e6bca258805ee2bc2b8e74e96860610e6ec04))

* chore(feature): rename `feature_matrix.py` to `feature.py` ([`0139dd5`](https://github.com/opentargets/genetics_etl_python/commit/0139dd58230cc6f8e0d55a2acac4341a60af03f0))

* chore: rename workflow DAG following review ([`a893cf7`](https://github.com/opentargets/genetics_etl_python/commit/a893cf7699ef2ff62816821df538a87c245dc508))

* chore: address review comments ([`3692d7a`](https://github.com/opentargets/genetics_etl_python/commit/3692d7a120519b53e40e32c126d3b75a43123793))

* chore: rename DAG related modules ([`701c0e6`](https://github.com/opentargets/genetics_etl_python/commit/701c0e65c391adfb86623b775b2de49fc8554b30))

* chore: update Python version to sync with the rest of the project ([`e5e3201`](https://github.com/opentargets/genetics_etl_python/commit/e5e3201d84e888bb1379f8398a79bf4b4906ab2e))

* chore: address review comments ([`4df9576`](https://github.com/opentargets/genetics_etl_python/commit/4df957627ed0734e40578cccf4528c65b0f701dd))

* chore: make Dockerfile build more quiet ([`781afdd`](https://github.com/opentargets/genetics_etl_python/commit/781afddee2a93d90689906dea63d5afd5e8a8368))

* chore: remove docker-compose - to be populated during installation ([`32217c5`](https://github.com/opentargets/genetics_etl_python/commit/32217c5b4100e1db61a649a56053f4e833af1efb))

* chore: update Dockerfile ([`4ddc250`](https://github.com/opentargets/genetics_etl_python/commit/4ddc250eaf67c861c4e040fd36a867e395f14ea1))

* chore: update requirements.txt ([`bc168f0`](https://github.com/opentargets/genetics_etl_python/commit/bc168f040ebde6e44f92204edd947148f6ffa03d))

* chore: remove .env - to be populated during installation ([`b2377b3`](https://github.com/opentargets/genetics_etl_python/commit/b2377b31a8ae7291e4eac98c33c84da6ac1ee134))

* chore: fix typo for `gnomad_genomes` ([`0109a1c`](https://github.com/opentargets/genetics_etl_python/commit/0109a1c91775f6085ddd5d1bc37a5a8fec8b781c))

* chore(deps): bump actions/setup-python from 2 to 4

Bumps [actions/setup-python](https://github.com/actions/setup-python) from 2 to 4.
- [Release notes](https://github.com/actions/setup-python/releases)
- [Commits](https://github.com/actions/setup-python/compare/v2...v4)

---
updated-dependencies:
- dependency-name: actions/setup-python
  dependency-type: direct:production
  update-type: version-update:semver-major
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`ec15e96`](https://github.com/opentargets/genetics_etl_python/commit/ec15e96361a7c7ee623b9902fca70aef699292f0))

* chore(deps): bump actions/cache from 2 to 3

Bumps [actions/cache](https://github.com/actions/cache) from 2 to 3.
- [Release notes](https://github.com/actions/cache/releases)
- [Changelog](https://github.com/actions/cache/blob/main/RELEASES.md)
- [Commits](https://github.com/actions/cache/compare/v2...v3)

---
updated-dependencies:
- dependency-name: actions/cache
  dependency-type: direct:production
  update-type: version-update:semver-major
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`337962a`](https://github.com/opentargets/genetics_etl_python/commit/337962a26ec0974bb0f7ba45ec793948eb64df3f))

* chore(deps): bump actions/checkout from 2 to 4

Bumps [actions/checkout](https://github.com/actions/checkout) from 2 to 4.
- [Release notes](https://github.com/actions/checkout/releases)
- [Changelog](https://github.com/actions/checkout/blob/main/CHANGELOG.md)
- [Commits](https://github.com/actions/checkout/compare/v2...v4)

---
updated-dependencies:
- dependency-name: actions/checkout
  dependency-type: direct:production
  update-type: version-update:semver-major
...

Signed-off-by: dependabot[bot] &lt;support@github.com&gt; ([`b829c6f`](https://github.com/opentargets/genetics_etl_python/commit/b829c6f17a643defdc140d16b181cbc4dbd3f0b9))

* chore: library updates in toml ([`2f2705b`](https://github.com/opentargets/genetics_etl_python/commit/2f2705b11cf1280aa93f47be11f4b59357dfa991))

* chore: fix conflicts with main ([`7085e4e`](https://github.com/opentargets/genetics_etl_python/commit/7085e4ed635f3902cf9b88aec2617ae699e8ae8e))

* chore: squash commit with main ([`9ac5bad`](https://github.com/opentargets/genetics_etl_python/commit/9ac5bad497a739602226f0b07dbab06489965baf))

* chore: remove exploration notebooks ([`eda0be7`](https://github.com/opentargets/genetics_etl_python/commit/eda0be78d7de0d8197d1a4390b4fd8dc420b4074))

* chore: comment line to compile config in makefile build ([`eb40eef`](https://github.com/opentargets/genetics_etl_python/commit/eb40eef365ce72cbce83aafca28d9c49797f71ee))

* chore: rebase branch with main ([`45e444e`](https://github.com/opentargets/genetics_etl_python/commit/45e444eec2f75f44bc2bb10c84db672727628cf3))

* chore: rebase branch with main ([`43ae890`](https://github.com/opentargets/genetics_etl_python/commit/43ae8907d40e364632893ebcecd5aeb37f8a331b))

* chore: squash commit with main ([`f90b1e2`](https://github.com/opentargets/genetics_etl_python/commit/f90b1e2a7ac8a01e140d93a961dff6be9370023c))

* chore: rename function ([`bc7b9e4`](https://github.com/opentargets/genetics_etl_python/commit/bc7b9e482ccff3dc660574570d39e98d4dd88b79))

* chore: duplicated logic ([`92ae933`](https://github.com/opentargets/genetics_etl_python/commit/92ae93359b3626b175a299fbfb011ec19035d635))

* chore: merge main ([`8b1442d`](https://github.com/opentargets/genetics_etl_python/commit/8b1442d23ee9bc8abda99836f14b379af5e9ec71))

* chore: unused dependency ([`21fb7cf`](https://github.com/opentargets/genetics_etl_python/commit/21fb7cf63f26b0e4ca3aa36b7c6d0ab63ef3fce0))

* chore: remove unused function ([`8978255`](https://github.com/opentargets/genetics_etl_python/commit/89782552dab3dc6b48e1611518a66ed91e08a1f3))

* chore: align column names with main ([`45f1e76`](https://github.com/opentargets/genetics_etl_python/commit/45f1e76c13b0f9e070267622411306500c000347))

* chore: remove print message ([`46aa092`](https://github.com/opentargets/genetics_etl_python/commit/46aa092fa9943244201b41ccbf2a9e0e98ddf378))

* chore: work in progress ([`720fff5`](https://github.com/opentargets/genetics_etl_python/commit/720fff566f6485fea5b97af8117165984e489b7c))

* chore: deprecate `utils/configure` ([`8e3680d`](https://github.com/opentargets/genetics_etl_python/commit/8e3680de01c19eeaa2aa0ff2c9e73126d25a9851))

* chore: remove coverage xml in `test` rule ([`06b3aed`](https://github.com/opentargets/genetics_etl_python/commit/06b3aedca37da09d971d111570359806a2d015f7))

* chore: merge conflicts ([`b6588fd`](https://github.com/opentargets/genetics_etl_python/commit/b6588fd49b0bd6b65851ef691f25c22e0f07c531))

* chore: _get_schema renamed to get_schema ([`12100f2`](https://github.com/opentargets/genetics_etl_python/commit/12100f2e752e56530e51e8df498939c6ae25cbb6))

* chore: merge conflicts ([`7007b80`](https://github.com/opentargets/genetics_etl_python/commit/7007b8074ebf74f4b7d01278c01adc4b4feff816))

* chore: hooks are now a main functionality of mkdocs ([`ae06da5`](https://github.com/opentargets/genetics_etl_python/commit/ae06da57dedd15b785ffd1e2c428b4a6886b24f1))

* chore: resolve conflicts with main branch ([`7d6c3cb`](https://github.com/opentargets/genetics_etl_python/commit/7d6c3cbac3688d587aed66b2a77f8aa2af067c63))

* chore: newline fix ([`43fdc62`](https://github.com/opentargets/genetics_etl_python/commit/43fdc627729b060dbc5828016b5e1dd50959d615))

* chore: restructuring the project ([`bd8a6a7`](https://github.com/opentargets/genetics_etl_python/commit/bd8a6a76840f7bae11d5e49c51bbbe19e5d8e294))

* chore: blacklist flake8 extension ([`8db0c77`](https://github.com/opentargets/genetics_etl_python/commit/8db0c775b01c2aebb93d6ea5eb2135c2977ccfa9))

* chore: dag renamed ([`85c0373`](https://github.com/opentargets/genetics_etl_python/commit/85c03734606af489002270e29abbe44aefbbc411))

* chore: minor comment ([`ee33fd2`](https://github.com/opentargets/genetics_etl_python/commit/ee33fd2df30a01c872b8184946ff040f30ee4fa8))

* chore: remove `my_ld_matrix.yaml` ([`a073946`](https://github.com/opentargets/genetics_etl_python/commit/a073946be383ed6c18869c3c61b0caf9ca80bbe2))

* chore: update ld_index schema ([`d7d2f40`](https://github.com/opentargets/genetics_etl_python/commit/d7d2f40eb1471ec9e5279d7873f338b4cf623547))

* chore: remove unnecessary file ([`8394c96`](https://github.com/opentargets/genetics_etl_python/commit/8394c96ca628c8d4d6553f22e950d382276c23da))

* chore: suggestions on structure (#99)

Looks good to me. ([`465306d`](https://github.com/opentargets/genetics_etl_python/commit/465306d79490057d41a3a102dec7c92f4c2b93d5))

* chore: logic cleaned up based on PR review ([`660c9ec`](https://github.com/opentargets/genetics_etl_python/commit/660c9ecff3642338458108a4f4b874c8649cb4cd))

* chore: typing error ([`d6ce0d7`](https://github.com/opentargets/genetics_etl_python/commit/d6ce0d785f1593c17fdf490ec5bf87736a18ccae))

* chore: remove exploration notebooks ([`e854c50`](https://github.com/opentargets/genetics_etl_python/commit/e854c5026acf8054bcbd74d519b853ddd0b2743c))

* chore: merge do-hydra branch ([`dbc2072`](https://github.com/opentargets/genetics_etl_python/commit/dbc20726aa422ebcaf51574cc0f980b95114f112))

* chore: pull main branch ([`b1d843b`](https://github.com/opentargets/genetics_etl_python/commit/b1d843b6a7e7c6d7f4a4feb5b48f51bfd333ad3b))

* chore: comment line to compile config in makefile build ([`0d747b7`](https://github.com/opentargets/genetics_etl_python/commit/0d747b79658508012afff0b6d4f1890df4818430))

* chore: wip gwas splitter ([`c5eb279`](https://github.com/opentargets/genetics_etl_python/commit/c5eb2792e34db5eaf092bc7c627127bf5b26323f))

* chore: gitignore schemas markdown ([`67accc6`](https://github.com/opentargets/genetics_etl_python/commit/67accc6c6e6babc5b59b26915114837478e7a5ae))

* chore: cleanup deprecated ([`439e847`](https://github.com/opentargets/genetics_etl_python/commit/439e8476ba5fd7415689068fb185bb1d5039427f))

* chore: update v2g schema to include distance features ([`8b7dc2c`](https://github.com/opentargets/genetics_etl_python/commit/8b7dc2c5c1c4985c2b3eeb5279919ed1450c4e62))

* chore: delete commented lines ([`3444817`](https://github.com/opentargets/genetics_etl_python/commit/34448173ca1677195530872906655a2ae12ef655))

* chore: separate script for dependency installation (#47) ([`2553be9`](https://github.com/opentargets/genetics_etl_python/commit/2553be9d49d89108a867baa5764059a977965c50))

* chore: updated config and makefile ([`8d906f1`](https://github.com/opentargets/genetics_etl_python/commit/8d906f15e141d264135718bb31dc43ada82ac01e))

* chore: adding function to calculate z-score ([`d82f270`](https://github.com/opentargets/genetics_etl_python/commit/d82f270e6d7a8531ccdc3d7d372af23cf51667ec))

* chore: exploring p-value conversion ([`890038f`](https://github.com/opentargets/genetics_etl_python/commit/890038f2974c7e0ff0ac9835eeeb3f7a47b7cd23))

### Ci

* ci: remove pyupgrade

it didnt work as expected, updating syntax that was causing typing errors ([`8021f2f`](https://github.com/opentargets/genetics_etl_python/commit/8021f2f3a198ab1f726bb0edc1ffa5c2371cc93c))

### Documentation

* docs: finngen description v1 (#345) ([`7e5e127`](https://github.com/opentargets/genetics_etl_python/commit/7e5e127f2c84f383406204ae03a0c75cdc184c49))

* docs: minify plugin removed to prevent clash in local development (#284) ([`74a1967`](https://github.com/opentargets/genetics_etl_python/commit/74a1967611fd0a52ea02699f21431ba9a3582692))

* docs: pics step ([`e73732e`](https://github.com/opentargets/genetics_etl_python/commit/e73732e322583253d36e5a97c01f6ffccb8e1fd6))

* docs: add automatically generated docs ([`49b8c0e`](https://github.com/opentargets/genetics_etl_python/commit/49b8c0e3a305fc09d40fc023811b7b2d270ceba3))

* docs: update contributing checklist ([`3690516`](https://github.com/opentargets/genetics_etl_python/commit/369051621844b01b9d8e2d3e3e24f65d6466a24c))

* docs: update running instructions ([`a9d961a`](https://github.com/opentargets/genetics_etl_python/commit/a9d961af1e872d127e66d7eeb84050cbc3ead835))

* docs: generalizing GnomAD class documentation ([`4c617c6`](https://github.com/opentargets/genetics_etl_python/commit/4c617c68a58465a04674d64abebee287ee9cd52a))

* docs: add rudimentary documentation on DAGs and autoscaling ([`53af017`](https://github.com/opentargets/genetics_etl_python/commit/53af01763cb2eccca9284b196c5fb0f8e289db1e))

* docs: rewrite section to add user ID ([`31da693`](https://github.com/opentargets/genetics_etl_python/commit/31da69302bd5415a9b1410cdd87059586ba4a5c3))

* docs: remove unnecessary comment which is replaced by a note ([`9e05db1`](https://github.com/opentargets/genetics_etl_python/commit/9e05db1cc0fd98293de08e1f221c94813f67b0f4))

* docs: set correct user ID to fix file access issues ([`ca18ab1`](https://github.com/opentargets/genetics_etl_python/commit/ca18ab15ef7d248aab54b04709826a36277f518b))

* docs: clarify parameter specific for CeleryExecutor ([`c17fbd2`](https://github.com/opentargets/genetics_etl_python/commit/c17fbd239b88bf8515ed629ba88605a315566537))

* docs: do not hardcode name of project ([`d42fd26`](https://github.com/opentargets/genetics_etl_python/commit/d42fd268f781ef31aaf56911349995d7c64615d1))

* docs: fix list and code block formatting ([`26b499d`](https://github.com/opentargets/genetics_etl_python/commit/26b499dad63a264882ec617bd4ad27f67eb46f94))

* docs: fix contributing checklist formatting ([`757c37b`](https://github.com/opentargets/genetics_etl_python/commit/757c37b1bf5ef1d8a2d7b4671bca528954ad2985))

* docs: streamline Airflow documentation style and tone ([`11c66cb`](https://github.com/opentargets/genetics_etl_python/commit/11c66cb534cda5294afb7a860aca2eecc90ce579))

* docs: l2g step title ([`dc59502`](https://github.com/opentargets/genetics_etl_python/commit/dc59502592f32d5fff154dc223792804962fcdc3))

* docs: add session to attributes list for all steps ([`5c80cea`](https://github.com/opentargets/genetics_etl_python/commit/5c80cea33acf9d7914454214cc8193562e689131))

* docs(l2g): add step documentation ([`b8c10c1`](https://github.com/opentargets/genetics_etl_python/commit/b8c10c1de9fee46b6d809827e7cefeb2f11b8b72))

* docs(l2g): add titles to files ([`494f213`](https://github.com/opentargets/genetics_etl_python/commit/494f213eb606cd3d65750c1b9f7a5b3e9baf0fb2))

* docs: study_locus schema at the end ([`23511fe`](https://github.com/opentargets/genetics_etl_python/commit/23511fe032a78e56fea24986f0016dbb07407bf7))

* docs: create and organise development section ([`edaeffc`](https://github.com/opentargets/genetics_etl_python/commit/edaeffc7472a29a5aba8d8547f514b70288543dd))

* docs: update index to match docs update ([`c97bad9`](https://github.com/opentargets/genetics_etl_python/commit/c97bad937596fb59771bd49006401b781485bafe))

* docs: fix command to start docs server ([`753bfe3`](https://github.com/opentargets/genetics_etl_python/commit/753bfe3fea44fddb252de60640be956edf9fb1ce))

* docs: make development a separate section ([`5146c9b`](https://github.com/opentargets/genetics_etl_python/commit/5146c9b40b8cb604888d7ca0dcc5d79faa10c28c))

* docs: add instructions for running Airflow ([`18aa5b5`](https://github.com/opentargets/genetics_etl_python/commit/18aa5b5222fdad3b49e8832250ec2136acb207f0))

* docs: create and organise development section ([`9f317ac`](https://github.com/opentargets/genetics_etl_python/commit/9f317acf65f902f89a07162e23c7251676745ba0))

* docs: rename all instances of Thurnman to Thurman ([`172f10a`](https://github.com/opentargets/genetics_etl_python/commit/172f10acbea809c1eea84bbfdc6e2232354576b0))

* docs: structural changes and data source images ([`fde9d3a`](https://github.com/opentargets/genetics_etl_python/commit/fde9d3a1dc674dfaf27d875a4f3a8a948c09dd7f))

* docs: several fixes in docstrings ([`f4f955d`](https://github.com/opentargets/genetics_etl_python/commit/f4f955d21c7a26fab3c54577c0cb29ee0308064f))

* docs: window_length no longer defines the collect locus ([`381030c`](https://github.com/opentargets/genetics_etl_python/commit/381030c5d7a40cc811bf7db3aba098a957e06eed))

* docs: enhance locus_collect column ([`a5babcc`](https://github.com/opentargets/genetics_etl_python/commit/a5babcc7079bae2b54166c509107cd612a9243b7))

* docs: more meaningful message ([`9708dca`](https://github.com/opentargets/genetics_etl_python/commit/9708dcabfd1caf2479e14708bd76acfc6e464541))

* docs: added descriptions ([`a903f03`](https://github.com/opentargets/genetics_etl_python/commit/a903f032caa73decddf8be3af2d8e13988fdc762))

* docs: remove links pointing nowhere to fix mkdocs ([`f5792ee`](https://github.com/opentargets/genetics_etl_python/commit/f5792ee9ed95b9ce1aef687918d4631a75d6f9b2))

* docs: fix incorrect reference ([`591fe15`](https://github.com/opentargets/genetics_etl_python/commit/591fe1555fda8eae45e2a35210d82b8b30278402))

* docs: fix relative link in `contributing.md` ([`819929c`](https://github.com/opentargets/genetics_etl_python/commit/819929ca6321eca51689bc7e57adfa00820abad8))

* docs: improve overlaps docs ([`afc02bd`](https://github.com/opentargets/genetics_etl_python/commit/afc02bd4830d4aaf23eacb2d54d1fbc7ff26a927))

* docs: improve documentation on the overall process ([`b6a758a`](https://github.com/opentargets/genetics_etl_python/commit/b6a758af00e5d3910f510acfd2c222725dfa402e))

* docs: page for window based clumping ([`bd7a23e`](https://github.com/opentargets/genetics_etl_python/commit/bd7a23e9617669d5f04e1ee81dfd1bee3a68203c))

* docs: remove the now unneeded file from utils ([`6b1f319`](https://github.com/opentargets/genetics_etl_python/commit/6b1f31917a5c437107d1e1d4c087b82147fb287d))

* docs: reorganise troubleshooting section ([`a9819e7`](https://github.com/opentargets/genetics_etl_python/commit/a9819e72c365bcf2b6070cca01b79fdc50857aa8))

* docs: update index with a separate troubleshooting section ([`79b69d9`](https://github.com/opentargets/genetics_etl_python/commit/79b69d9a28d5226a6aa4061212deccc503a2ab76))

* docs: move troubleshooting information from the contributing guide ([`ef8a7f7`](https://github.com/opentargets/genetics_etl_python/commit/ef8a7f766d91cd6ce71bb74228764ceadaa9aee6))

* docs: update documentation on building and testing ([`6f4e0c2`](https://github.com/opentargets/genetics_etl_python/commit/6f4e0c2473d2638fa699f88d26461b512c43de5a))

* docs: improve `_annotate_discovery_sample_sizes` docs ([`eff02da`](https://github.com/opentargets/genetics_etl_python/commit/eff02da6269373693398182c6cbd09a021cc6d2e))

* docs: updated java version text ([`93de7c3`](https://github.com/opentargets/genetics_etl_python/commit/93de7c3ff110f66c507c1ce8f4c254801145297f))

* docs: finngen study index dataset duplicated in docs ([`6f2aa01`](https://github.com/opentargets/genetics_etl_python/commit/6f2aa01c50a2ad61573dc3fb90138757f34fa8e1))

* docs: finngen study index dataset duplicated in docs ([`eeb85b0`](https://github.com/opentargets/genetics_etl_python/commit/eeb85b08e0a52ef4aec0963c60f3cb990b5446ea))

* docs: Update ukbiobank.py docstring

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`a6c1ccb`](https://github.com/opentargets/genetics_etl_python/commit/a6c1ccb44f31185b73e78aee0a90d3304c9e5493))

* docs: compile the new contributing section ([`42776f9`](https://github.com/opentargets/genetics_etl_python/commit/42776f9ee7fa82474ff2e479c3aa36ccf1da77e6))

* docs: add contributing information to index ([`70edea7`](https://github.com/opentargets/genetics_etl_python/commit/70edea74810bc9abbceab3daa8afae7dd8fcc9dc))

* docs: move technical information out of README ([`a11f042`](https://github.com/opentargets/genetics_etl_python/commit/a11f042fa7c67860ae170f95811693ce6b2e9638))

* docs: add need to install java version 8 ([`42325d4`](https://github.com/opentargets/genetics_etl_python/commit/42325d448b8d334c51efaf6a3b86734bde95e05d))

* docs: fix link ([`c3bee56`](https://github.com/opentargets/genetics_etl_python/commit/c3bee5691f854c8cd4a664ef69264e2fd48b4983))

* docs: fix fixes ([`0148299`](https://github.com/opentargets/genetics_etl_python/commit/0148299e90421f8dda95318e7fa5d12cfbb1e94b))

* docs: update index.md ([`3ffdd4f`](https://github.com/opentargets/genetics_etl_python/commit/3ffdd4fb2d12838e398f343044adf486c3d1a4ea))

* docs: reference renamed to components based on buniellos request ([`a6458ef`](https://github.com/opentargets/genetics_etl_python/commit/a6458ef97ab1fd250a245288e1e0092cdbd8c2a7))

* docs: correct version naming example ([`18c4e6d`](https://github.com/opentargets/genetics_etl_python/commit/18c4e6dcf9c849fb3d8d529c5f6ae35f1b56e00f))

* docs: amend documentation to make clear how versioning works ([`8b564af`](https://github.com/opentargets/genetics_etl_python/commit/8b564af0c373bc0238d64f405a1f80b4aef88edc))

* docs: missing docs for finger step added ([`e8f1f67`](https://github.com/opentargets/genetics_etl_python/commit/e8f1f6736f095a9cb6e431aba2a3eeb8fe2dad59))

* docs: rewrite README with running instructions ([`c41fd38`](https://github.com/opentargets/genetics_etl_python/commit/c41fd38bd412c05f4048fe786a43b9f03b36bcc6))

* docs: add documentation on StudyIndexFinnGen ([`9c65be4`](https://github.com/opentargets/genetics_etl_python/commit/9c65be4f137d900733207a14856e0b9498da7a5f))

* docs: add troubleshooting instructions for Pyenv/Poetry ([`2848643`](https://github.com/opentargets/genetics_etl_python/commit/2848643f3a0ca0015fd2d507b10b556e526c378c))

* docs: simplify instructions for development environment ([`597fc7a`](https://github.com/opentargets/genetics_etl_python/commit/597fc7ad98c46c691ad070321088f11444e981a4))

* docs: docs badge added

Docs badge added ([`f93bc57`](https://github.com/opentargets/genetics_etl_python/commit/f93bc5752c322e3824c2694f3ba3623b89716c8d))

* docs: fixing actions ([`a9a22ab`](https://github.com/opentargets/genetics_etl_python/commit/a9a22ab6741267871fa8e5c52af2e0d507cce3cb))

* docs: fixing actions ([`25d35a0`](https://github.com/opentargets/genetics_etl_python/commit/25d35a0ce7aebd8137a6bd2a8bc3c902b99a9327))

* docs: fixing actions ([`70f1295`](https://github.com/opentargets/genetics_etl_python/commit/70f12951f451335ef3e1c7c020bc6b3ee99bce26))

* docs: fix to prevent docs crashing ([`74b3833`](https://github.com/opentargets/genetics_etl_python/commit/74b3833bf4a40521e3973ba3557e491aefaaf9b3))

* docs: fix doc ([`61d53de`](https://github.com/opentargets/genetics_etl_python/commit/61d53deab49fc1196fba36f1fbccf70e6d77e929))

* docs: explanation improved ([`7ff8fc9`](https://github.com/opentargets/genetics_etl_python/commit/7ff8fc962ffeca267dd39a74a09fd6e6862eba83))

* docs: missing docstring ([`4343ea9`](https://github.com/opentargets/genetics_etl_python/commit/4343ea92058dc204ddac9dab36a8ea5a06337563))

* docs: gwas catalog step ([`e3fc4d5`](https://github.com/opentargets/genetics_etl_python/commit/e3fc4d54753b04d7c25939f7d89033813bd0e41f))

* docs: missing ld clumped ([`caa3453`](https://github.com/opentargets/genetics_etl_python/commit/caa3453a7505e6f3a09bab3e649f9a17143dc62c))

* docs: typo ([`52c9092`](https://github.com/opentargets/genetics_etl_python/commit/52c9092e1d61adc8e45662206021527325055ae5))

* docs: including titles in pages ([`37282e1`](https://github.com/opentargets/genetics_etl_python/commit/37282e18ea5a29f69fce8e33fb58cc5b939e3f32))

* docs: improved description ([`01a4723`](https://github.com/opentargets/genetics_etl_python/commit/01a47233ac39713f78471ea52877d26f418b2fac))

* docs: extra help to understand current configs ([`705adf7`](https://github.com/opentargets/genetics_etl_python/commit/705adf7c0a6b142351ecc426d7e2f9f1464b6b7f))

* docs: address david comment on verbose docs ([`a7210a7`](https://github.com/opentargets/genetics_etl_python/commit/a7210a79cea7d864083eba576b05aca581d97603))

* docs: correct reference to coloc modules ([`df10181`](https://github.com/opentargets/genetics_etl_python/commit/df10181b6441849435cf54ceea2d6aa9d7a967f9))

* docs: update summary of logic ([`ebbf5a4`](https://github.com/opentargets/genetics_etl_python/commit/ebbf5a44fec45ac3f9287a552eca0d62ff81248c))

* docs: added docs ([`a271ab5`](https://github.com/opentargets/genetics_etl_python/commit/a271ab5e36b6f75e345c1c6299dda4bac959fd2d))

* docs: added v2g docs page ([`6b8c983`](https://github.com/opentargets/genetics_etl_python/commit/6b8c9835f8c4546fcd9449f97ad61a1e7cd24ee7))

* docs: scafolding required for variant docs ([`a00fa0a`](https://github.com/opentargets/genetics_etl_python/commit/a00fa0a864ef0cde563357a69c306155f8177a17))

* docs: generate_variant_annotation documentation ([`84c7a3a`](https://github.com/opentargets/genetics_etl_python/commit/84c7a3a3815ff2f5888c94cc234d76373ff13c91))

* docs: removing old docsc (#36)

I don&#39;t think there is anything that hasn&#39;t been migrated yet ([`a71c367`](https://github.com/opentargets/genetics_etl_python/commit/a71c3673930393e94b848827f0d0194adcc21931))

* docs: rounding numbers for safety ([`09d09f2`](https://github.com/opentargets/genetics_etl_python/commit/09d09f2a886b4b2d3baaa2b765a1021940282580))

* docs: first pyspark function example ([`4845f6e`](https://github.com/opentargets/genetics_etl_python/commit/4845f6e7309139ad27c6ba79ba97b8f19f98866c))

* docs: non-spark function doctest ([`e750a8b`](https://github.com/opentargets/genetics_etl_python/commit/e750a8b7fcb1a61f3cf2f9f8a13824c728c870da))

* docs: doctest functionality added ([`55b5d3e`](https://github.com/opentargets/genetics_etl_python/commit/55b5d3e3da97d065f3f9401bd635aaa7a70b6399))

* docs: openblas and lapack libraries required for scipy ([`5fe9d10`](https://github.com/opentargets/genetics_etl_python/commit/5fe9d10bdd47271f50f92ecddd52167f8d26cb61))

* docs: improvements on index page and roadmap ([`a93abf5`](https://github.com/opentargets/genetics_etl_python/commit/a93abf50ce70066f676ca18c0ee4056853209142))

* docs: license added ([`c4fc195`](https://github.com/opentargets/genetics_etl_python/commit/c4fc19571bed95450060d0ced172e26cc83fff09))

* docs: better docs for mkdocs ([`d6eafae`](https://github.com/opentargets/genetics_etl_python/commit/d6eafae86f83288fd670466db927f10a46dc0066))

* docs: function docstring ([`3c1da91`](https://github.com/opentargets/genetics_etl_python/commit/3c1da915f13fd7f7977183148b32d42c0bbfefbe))

* docs: schema functions description ([`f49d913`](https://github.com/opentargets/genetics_etl_python/commit/f49d91387490823549928d072ba825d2c6c2d141))

* docs: adding badges to README ([`215e19f`](https://github.com/opentargets/genetics_etl_python/commit/215e19f3960682709c9ac180fcedc8877cea425c))

* docs: adding badges to README ([`42bb711`](https://github.com/opentargets/genetics_etl_python/commit/42bb7118122d465a64b8d9c0aacbe0662a2474fb))

* docs: adding documenation for gwas ingestion ([`6ce1d53`](https://github.com/opentargets/genetics_etl_python/commit/6ce1d53a02c29640dbb910c5bb63c520b0d93f5b))

* docs: not necessary ([`d59a96e`](https://github.com/opentargets/genetics_etl_python/commit/d59a96e269ada404d462d65daeee21ae2aa1e9b6))

* docs: adding documentation for Intervals and Variant annotation ([`645a824`](https://github.com/opentargets/genetics_etl_python/commit/645a824f37f2b6cfcda15bfb183d0688ae15bf9e))

### Feature

* feat: release branch (#350) ([`e51294b`](https://github.com/opentargets/genetics_etl_python/commit/e51294be134207a3264abefb189b80d4596fb7c8))

* feat: yamllint to ensure yaml linting (#338)

* feat: yamllint support

* feat: updates yamllint rules ([`1b35c99`](https://github.com/opentargets/genetics_etl_python/commit/1b35c9920118b9be73a17326005f546ac5869073))

* feat: trigger on push (#337) ([`31cb305`](https://github.com/opentargets/genetics_etl_python/commit/31cb30588d5d62e485d485d8aeea2febe3ec5a30))

* feat: track feature missingness rates (#335)

* feat(L2GFeatureMatrix): add `features_list` as attribute

* fix: log wandb table

* feat(L2GFeatureMatrix): track missingness rate for each feature

* feat(L2GFeatureMatrix): track missingness rate for each feature

* chore(LocusToGeneModel): remove evaluation outside experiment tracking ([`5c2db1a`](https://github.com/opentargets/genetics_etl_python/commit/5c2db1a4aa748d294ca9b49b88381f619ddb8dc5))

* feat: semantic release automation (#294)

Feature needs to be fully tested ([`645bb46`](https://github.com/opentargets/genetics_etl_python/commit/645bb46327aece03d7665af719f57ae7e678ca7d))

* feat: ruff as formatter (#322)

* ruff formatter instead of black

* refactor: ruff reformatted files

* feat: more complete ruff adjustments

* refactor: all codebase to comply with ruff rules

* chore: update lock

* feat: more stringent docstring rules

* revert: remove isort and black from Makefile ([`6798153`](https://github.com/opentargets/genetics_etl_python/commit/6798153268d6ce807b7f9e36e7ab64f240b7f82b))

* feat: track training data and feature importance (#325) ([`243d8f6`](https://github.com/opentargets/genetics_etl_python/commit/243d8f671ff36759d3af51f7845f0dc427494a41))

* feat: finngen preprocess prototype (#272)

All steps associated with the preprocessing of Finngen studies (PICS-road) included in a DAG:
- summary stats harmonisation
- window-based clumping
- LD-based clumping
- PICS

Several enhancements might follow in different PRs.

---------
Co-authored-by: Irene López &lt;irene.lopezs@protonmail.com&gt; ([`47fb71f`](https://github.com/opentargets/genetics_etl_python/commit/47fb71ff923736b5f8fe490eaa60722d0683feee))

* feat: add gwas_catalog_preprocess dag (#291)

* feat: gwas_catalog step stops at ingestion

* feat: gwas_catalog step stops at ingestion

* feat: add gwas_catalog_preprocess dag

* fix: change step_id to task_id as task_id

* feat: group gwas_catalog_preprocess tasks into sumstats and curation groups

* fix: add all dependencies when ld clumping

* fix: update gwas catalog docs

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci

* refactor(dag): extract releasebucket and sumstats paths as constants

* refactor: streamline study locus paths ([`56ea0a9`](https://github.com/opentargets/genetics_etl_python/commit/56ea0a93a42d272a70471154f33a165c236ae99a))

* feat: Gnomad v4 based variant annotation (#311)

* feat: gnomad4 parser and changes in schema

* fix: variant annotation schema

* feat: required changes in variant index

* test: fix schema

* test: adjust testing to absence of sift and polyphen

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci

* fix: typo in schema

* fix: preventing skewed partitions

* fix: remove sift and polyphen predictions from v2g

* feat: rename gnomad3VariantId to gnomadVariantId name ([`eea2a4c`](https://github.com/opentargets/genetics_etl_python/commit/eea2a4c81238807de5157bdbcb63844ac1399b69))

* feat: GWAS Catalog harmonisation prototype (#270)

* chore: merge main

* feat: partitioning GWAS Catalog dataset to 20 equally-sized partitions

* fix: missing config

* feat: dag with gwas catalog harmonisation

* feat: adding more primary workers to help with the task

* refactor: changed autoscaling policy

* feat: allow to specify number of preeptible workers

* fix: bugs on to_do_list

* revert: version number

* fix: gwas_catalog_sumstat_preprocess no longer needs study_id

* fix: unnecessary config causes issues

* fix: improved regexp

* refactor: generalising the config

* fix: rename cluster to prevent clashes with other dags

* Update src/airflow/dags/gwas_catalog_harmonisation.py

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt;

---------

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt;
Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt; ([`e0297cb`](https://github.com/opentargets/genetics_etl_python/commit/e0297cbfadd1797d741d1474d6b37ab0f433be70))

* feat: add &#39;coalesce&#39; and &#39;repartition&#39; wrappers to &#39;Dataset&#39; (#307)

* feat(dataset): add `coalesce`

* feat(clump): coalesce summary stats to 1000 partitions

* feat(dataset): change coalesce to setPartitions

* test(dataset): added `TestSetPartitions`

* refactor(dataset): split set_partitions into repartition and coalesce

* chore(clump): decrease number of sumstats partitions to 400

* chore: change number of partitions to window clumping to 4000

Number estimate on the basis of 60 workers x 16 cores x 4 partitions

Co-authored-by: David Ochoa &lt;ochoa@ebi.ac.uk&gt;

---------

Co-authored-by: David Ochoa &lt;ochoa@ebi.ac.uk&gt; ([`30d163f`](https://github.com/opentargets/genetics_etl_python/commit/30d163f9950307284c3bd4038899a2d23d0e0351))

* feat: Adding cohorts field to study index (#309)

* feat: ingesting cohort information for GWAS Catalog

* feat: adding cohort to finngen study index ([`28d7479`](https://github.com/opentargets/genetics_etl_python/commit/28d747956ac92b0af689e3c81e6c9e166c7934cd))

* feat: add prettier as formatter (yaml, json, md, etc.) (#298)

* feat: prettier added to the project and precommit

* feat: recommend vscode extension

* fix: adjusted prettier

* fix: all files reformated by prettier

* fix: unnecessary comment ([`ae68d84`](https://github.com/opentargets/genetics_etl_python/commit/ae68d84c138d6409ddcf0b98ac77f5892c9ba30f))

* feat: adding unpublished studies (#290)

* feat: adding unpublished studies

* feat: updating all gwas catalog sources

* feat: generalizing study ingestion to accept list of files

* fix: removing unused configuration

* feat: adding unpublished ancestries as well

* fix: updating ancestry config name

* Apply suggestions from code review

Co-authored-by: Kirill Tsukanov &lt;tskir@users.noreply.github.com&gt;

---------

Co-authored-by: Kirill Tsukanov &lt;tskir@users.noreply.github.com&gt; ([`6118bb9`](https://github.com/opentargets/genetics_etl_python/commit/6118bb972b749033ca904e1473af9b7319a3ec2a))

* feat: deptry added to handle unused, missing and transitive dependencies (#304)

* feat: add deptry to the project

* feat: deptry as pre-commit and associated changes

* feat: deptry as pre-commit and associated changes

* fix: exclude deptry from pre-commit.ci

* ci: check deptry when testing

* fix: poetry lock with no update ([`bc5f2b3`](https://github.com/opentargets/genetics_etl_python/commit/bc5f2b39cefa97e7ec309de948e324a0a60d2611))

* feat: add `clump` step (#288)

* feat: join clump methods into step canonical

* refactor: keep clump step minimal

* docs: add clump step docs page

* fix: add clump config

* revert(pre-commit): downgrade mypy version to 1.7.0

* feat(clump): add attribute to collect locus in window-based clumping

* refactor(clump): make study and ld indices non mandatory

* test: add test for clumpstep when input is ss

* test: rename test_clump_py to avoid clashes

* refactor: remove data variable in clumpstep ([`8b654ac`](https://github.com/opentargets/genetics_etl_python/commit/8b654acf9f0ee7e8ebdfd4328bad4a1ddd0cb2e1))

* feat: ingestion supported for both new and old format of the harmonized GWAS Catalog Summary stats. (#274)

* feat: converting gwas catalog ingestion to the new format

* feat: generalizing GWAS catalog sumstas ingestion both format supported

* fix: sample file must follow the convention of the real files

* fix: woopsie, the sample file is not added

* test: adding trst for old gwas format

* feat: updating gwas summary stats ingestion step to the new way of getting data

* fix: casting types to make schema explicit ([`024d084`](https://github.com/opentargets/genetics_etl_python/commit/024d0848b0ddb65635d2d75bb57565dec9e43fbd))

* feat: gitignore .venv file ([`8f92ea9`](https://github.com/opentargets/genetics_etl_python/commit/8f92ea999c59f92cc803f6ab3bd9ed31dad5fe6c))

* feat: updating summary stats schema and ingtesion ([`55ac5ad`](https://github.com/opentargets/genetics_etl_python/commit/55ac5ad818b3e201115cf3720a8fe7f58f5dae90))

* feat(l2g_gold_standard): change `filter_unique_associations` logic ([`28031b8`](https://github.com/opentargets/genetics_etl_python/commit/28031b8a14dfad0ef91d7160d138567a7ead0e6d))

* feat(overlaps): add and test method to transform the overlaps as a square matrix ([`9c0a042`](https://github.com/opentargets/genetics_etl_python/commit/9c0a042306f0e56ac42d1c67f354e65c6c252500))

* feat: raise error in `from_parquet` when df is empty ([`a1c41e6`](https://github.com/opentargets/genetics_etl_python/commit/a1c41e69031feff3a086e961997594ad55608f6a))

* feat: gcp dataset config ([`e9be8a6`](https://github.com/opentargets/genetics_etl_python/commit/e9be8a602cd9a8ae1870f5339ce4502bf5713454))

* feat: datasets config ([`a07c7c4`](https://github.com/opentargets/genetics_etl_python/commit/a07c7c4bf16df61fbcc4285b7e4dec946667beab))

* feat: configuration for PICS step ([`0c5878c`](https://github.com/opentargets/genetics_etl_python/commit/0c5878c0546814d02ac076ea15031e3af3b7ff72))

* feat: new PICS step ([`c9d8dd9`](https://github.com/opentargets/genetics_etl_python/commit/c9d8dd9d1dfc5e987080d8fecc43cbc8abda4a7a))

* feat: finetune spark job: taskid, trigger rule, other args ([`c24debb`](https://github.com/opentargets/genetics_etl_python/commit/c24debb98b521d9e0e8739fba98323a356b864ba))

* feat: parametrise autoscaling policy ([`670ebf9`](https://github.com/opentargets/genetics_etl_python/commit/670ebf9b3df4771da92122143646cabe6e438890))

* feat: add ability to attach local SSDs ([`d23f18e`](https://github.com/opentargets/genetics_etl_python/commit/d23f18eb7569654883dc24c0ddc168a860253931))

* feat: populate geneId column ([`fb8f6f8`](https://github.com/opentargets/genetics_etl_python/commit/fb8f6f83889734e3eaccee4780ecda90385d6ac9))

* feat: join dataframes to add the full study ID information ([`53a4532`](https://github.com/opentargets/genetics_etl_python/commit/53a4532b7ceb5445b841011be8f538edd4a273e8))

* feat: map partial and full study IDs ([`24e9be3`](https://github.com/opentargets/genetics_etl_python/commit/24e9be3b1ffff801a2ff317a9d33146cfce618e1))

* feat: construct study ID based on all appropriate columns ([`071e375`](https://github.com/opentargets/genetics_etl_python/commit/071e375768894473a52a136a323e8e585d44ab0c))

* feat: implement summary stats ingestion ([`d75b1a2`](https://github.com/opentargets/genetics_etl_python/commit/d75b1a2226d801aed72c7a764b84df9c883c96a0))

* feat: implement study index ingestion ([`f3f5c3a`](https://github.com/opentargets/genetics_etl_python/commit/f3f5c3a8967e8f575247ea91bce14070fd6b8737))

* feat: eQTL Catalogue main ingestion script ([`eeacd0d`](https://github.com/opentargets/genetics_etl_python/commit/eeacd0dc9e56bbe7ba7ddf4312180f96f72a788e))

* feat: add eQTL ingestion to the list of steps in DAG ([`c160fff`](https://github.com/opentargets/genetics_etl_python/commit/c160fff9b323c61cf4aeb3569c797f8e9a87c6b6))

* feat: hydra full error on dataproc ([`75af590`](https://github.com/opentargets/genetics_etl_python/commit/75af5908788d343fcd94274ca36e1df03ea0f849))

* feat: function to retrieve melted ld matrix with resolved variantids ([`eafda09`](https://github.com/opentargets/genetics_etl_python/commit/eafda094519ee96f9a669c1072df3bd6f9b28c90))

* feat: type enhancements ([`b44ac35`](https://github.com/opentargets/genetics_etl_python/commit/b44ac35967ec9badd37e3d332f835936b6031326))

* feat: extract gnomad ld matrix slice ([`2d4df12`](https://github.com/opentargets/genetics_etl_python/commit/2d4df12b79057d02af19c4a7c797aa6af7fe788f))

* feat: local chain file ([`9103905`](https://github.com/opentargets/genetics_etl_python/commit/91039058f55b647fe58a8bee9c2d70427a4706cf))

* feat: variant annotation within class ([`a140ad0`](https://github.com/opentargets/genetics_etl_python/commit/a140ad059334fc41dd60acd7dc7e9aae5ec535cf))

* feat: gnomAD LD datasource contains path ([`c5f8937`](https://github.com/opentargets/genetics_etl_python/commit/c5f8937099ae347b22851a9fcf59c634795d313e))

* feat: enable autoscaling ([`c0b2db6`](https://github.com/opentargets/genetics_etl_python/commit/c0b2db672383672c67e455fc2bc5674f043fcdc4))

* feat(variant_annotation): include variant_annotation step as part of the preprocessing dag ([`689cf28`](https://github.com/opentargets/genetics_etl_python/commit/689cf28a112c591419a7150b2ee9f9617227015c))

* feat(ldindex): include ldindex step as part of the preprocessing dag ([`602d55b`](https://github.com/opentargets/genetics_etl_python/commit/602d55bc696d2124573f2d08edb03d9b60fa4796))

* feat: implement Preprocess DAG ([`2950941`](https://github.com/opentargets/genetics_etl_python/commit/29509416aaded7d580aa38ce2f835141db986d88))

* feat(airflow): remove cluster autodeletion ([`1fb6c6a`](https://github.com/opentargets/genetics_etl_python/commit/1fb6c6acd6d12b1a4fe35050f6a6d48bb36e7d82))

* feat: update gcp sdk version to 452 and airflow image to airflow:slim-2.7.2-python3.10 ([`5acc280`](https://github.com/opentargets/genetics_etl_python/commit/5acc2801e990ba5b0545974b825fd7a1a6db63fd))

* feat: update gcp sdk version to 452 and airflow image to airflow:slim-2.7.2-python3.10 ([`adaac6c`](https://github.com/opentargets/genetics_etl_python/commit/adaac6c2884c3d8422451a4131540745f1c262f4))

* feat: switch back to non root after gcp installation ([`637266f`](https://github.com/opentargets/genetics_etl_python/commit/637266fabb4256b437ea49632eab37bfbc7059bf))

* feat: change airflow image to `airflow:slim-2.7.2-python3.9` ([`5c7f0de`](https://github.com/opentargets/genetics_etl_python/commit/5c7f0dee8d6a9e374f5b997a8ea0cd8c824b7476))

* feat: display class name in docs and cleanup docs ([`4a0f0d4`](https://github.com/opentargets/genetics_etl_python/commit/4a0f0d4cc8acddea26da2def455f9db9e9903182))

* feat: apply pydoclint pre commit to `src` only ([`06f4a72`](https://github.com/opentargets/genetics_etl_python/commit/06f4a72c0a2ecd1c7be3c157e7e1cd5cfbb3c11d))

* feat: `make check` has two tiers of docstrings linting ([`5fb4022`](https://github.com/opentargets/genetics_etl_python/commit/5fb40220f7df980faa06c77991775644faa8b423))

* feat: configure pydoclint to require return docstring when returning nothing ([`116ff4f`](https://github.com/opentargets/genetics_etl_python/commit/116ff4fa451e36c17caeec7bad0006e662bf92db))

* feat: configure pydoclint to check all functions have args in their docstrings ([`05f46e4`](https://github.com/opentargets/genetics_etl_python/commit/05f46e47e88c79eda82241c204853ed1c6f01692))

* feat: configure pydoclint to allow docstrings in init methods ([`da5344a`](https://github.com/opentargets/genetics_etl_python/commit/da5344ab7a74bb07323385d7b4e5e82a31da75ca))

* feat: configure pydoclint to require type signatures for all functions ([`fd7cc17`](https://github.com/opentargets/genetics_etl_python/commit/fd7cc17019a3a27819833fe32ae8a526ee5fefed))

* feat: add `pydoclint` to make check rule ([`e0287ee`](https://github.com/opentargets/genetics_etl_python/commit/e0287ee47cc9de83e987dc0139980bc66fa5cf03))

* feat: add docs and reorganise modules ([`8ea4ad5`](https://github.com/opentargets/genetics_etl_python/commit/8ea4ad59a7a3c38b82454c665cdef569082f064c))

* feat(session): start session inside step instance ([`16d1ff2`](https://github.com/opentargets/genetics_etl_python/commit/16d1ff2da796adf996703f01286712943124cfcc))

* feat: adding notebooks for finngen and ld matrix ([`1ed273d`](https://github.com/opentargets/genetics_etl_python/commit/1ed273d8e3f526f309a04122f3a11a1362afdf88))

* feat(l2g): working session interpolation with yaml ([`421d7e0`](https://github.com/opentargets/genetics_etl_python/commit/421d7e0e605469b00dd6bd50dd3f35241664b9cc))

* feat(session): pass extended_spark_conf as dict, not sparkconf object ([`2ecf362`](https://github.com/opentargets/genetics_etl_python/commit/2ecf362bca5d5eac97f18485fcde732d6dc4a326))

* feat: start l2g step with custom spark config ([`06392ee`](https://github.com/opentargets/genetics_etl_python/commit/06392ee1a1825ec52f621d99a412a61528fdad79))

* feat: migrate to LocalExecutor ([`d31d35b`](https://github.com/opentargets/genetics_etl_python/commit/d31d35b727d0bcd7bdc0f685334da43f7ee400f8))

* feat: gitkeep config directory ([`6b8ee1f`](https://github.com/opentargets/genetics_etl_python/commit/6b8ee1fd4ed870c51541c7d4ae01ed20b63c4e6c))

* feat: dependabot to monitor airflow docker ([`fdf97a1`](https://github.com/opentargets/genetics_etl_python/commit/fdf97a17b7e0556dd97d3d245fa7df2fdf41418a))

* feat: gitkeep dags folder as well ([`66a98c2`](https://github.com/opentargets/genetics_etl_python/commit/66a98c25fa8702078de8716a21a7197431d30ea4))

* feat: streamline docker configuration ([`2ed6da6`](https://github.com/opentargets/genetics_etl_python/commit/2ed6da6d449557cf9a6dd2eb3308e4cc4dc04f08))

* feat(study_index): harmonise all study indices under common path ([`cd81547`](https://github.com/opentargets/genetics_etl_python/commit/cd81547c7abc5e6e73303f15ae66840c4189410d))

* feat(l2g): l2gprediction.from_study_locus generates features instead of reading them ([`057e94f`](https://github.com/opentargets/genetics_etl_python/commit/057e94f7181e44ef314d7002d639cd1fced81ed8))

* feat: implement submit_pig_job in common_airflow ([`39cfdac`](https://github.com/opentargets/genetics_etl_python/commit/39cfdac2d14d3b1383828869b036c962b35b8869))

* feat: additional changes for dag_genetics_etl_gcp ([`49938b2`](https://github.com/opentargets/genetics_etl_python/commit/49938b22bd4f2d2039e19fb011e15e41be989800))

* feat: additional changes for airflow_common ([`aae80f2`](https://github.com/opentargets/genetics_etl_python/commit/aae80f24b7ca72eceb1c8f8b01dc9b8405d29b8e))

* feat: add the common module for Airflow dags ([`d9ae407`](https://github.com/opentargets/genetics_etl_python/commit/d9ae407f5ce1babf609b51d7aef57025558210d7))

* feat(l2g): add sources to gold standard schema ([`09788d4`](https://github.com/opentargets/genetics_etl_python/commit/09788d4a857d8e49b37279d7c2121bdf28bcc4e8))

* feat(l2g): new module to capture gold standards parsing logic ([`c9be821`](https://github.com/opentargets/genetics_etl_python/commit/c9be821a4a6e8552c4a95bc2658e288ab9602f36))

* feat(l2g): add evaluate flag to trainer ([`6da604c`](https://github.com/opentargets/genetics_etl_python/commit/6da604c8f8ea89548a0482270fe04a7ae883c372))

* feat(intervals): create Interval.from_source ([`7fe52a3`](https://github.com/opentargets/genetics_etl_python/commit/7fe52a3b3df8c4baec997e535d3d3905c05c807a))

* feat(intervals): abstract logic from sources wip ([`ee62ed1`](https://github.com/opentargets/genetics_etl_python/commit/ee62ed108594c8f8658bdcec7b05e7e74a1194d0))

* feat(intervals): prototype of collect_interval_data ([`e2cb15f`](https://github.com/opentargets/genetics_etl_python/commit/e2cb15fe78ce83e3dc1af245ad6db834069817d4))

* feat: added data bio ([`12d5b63`](https://github.com/opentargets/genetics_etl_python/commit/12d5b63284ff8a3a31b8ce8ca4e02bcd9da19e29))

* feat(v2g): remove  from schema - no longer used to write dataset ([`a2fbdce`](https://github.com/opentargets/genetics_etl_python/commit/a2fbdce0a84b8df69ac33e1ac7d1ca6fb28304e9))

* feat(session): read files recursively ([`59f2358`](https://github.com/opentargets/genetics_etl_python/commit/59f2358ebf5d7e8f4a51ff8d7d8d3ece2399194f))

* feat(studylocus): change `unique_lead_tag_variants` to `unique_variants_in_locus` + test ([`a0f63c4`](https://github.com/opentargets/genetics_etl_python/commit/a0f63c4094876d85371675ccad506e40d16621bd))

* feat(github): check toml validity on cicd ([`1920c9d`](https://github.com/opentargets/genetics_etl_python/commit/1920c9d2dc314578814b906d002b583b7b649186))

* feat(dependabot): preformat commit messages ([`24be6de`](https://github.com/opentargets/genetics_etl_python/commit/24be6de4c9d5263841b02068f65e3a38f5e96484))

* feat(github): check toml validity on cicd ([`d9c1bf9`](https://github.com/opentargets/genetics_etl_python/commit/d9c1bf925ae9da57cfaf2a0f888a83382ff3b5cf))

* feat: dependabot for poetry/pip and github actions ([`d25076d`](https://github.com/opentargets/genetics_etl_python/commit/d25076d58a5151c0862f7edbcfff0ccc13e2195b))

* feat: changing the action ([`ffc6435`](https://github.com/opentargets/genetics_etl_python/commit/ffc643569d0d251c592b6815beab3c80c4dcf71d))

* feat: bump poetry packages ([`e20b611`](https://github.com/opentargets/genetics_etl_python/commit/e20b61104cfe37afecc7e96be0e753dae83c8cff))

* feat: change catalog_study_locus path under common study_locus ([`c2b1e59`](https://github.com/opentargets/genetics_etl_python/commit/c2b1e5966039fab08dff05c81b01827f7e66899f))

* feat: add l2g_benchmark.ipynb ([`621a7cc`](https://github.com/opentargets/genetics_etl_python/commit/621a7cc129bd9bfb051b70c8a16f6be0d296fb65))

* feat: install jupyter in dev cluster ([`3ffdbd0`](https://github.com/opentargets/genetics_etl_python/commit/3ffdbd0fbaf0be6be94c8dcaed722445edade9f8))

* feat: finalise summary stats ingestion logic ([`2dd6a49`](https://github.com/opentargets/genetics_etl_python/commit/2dd6a49896ec969c7be6ed8b0edf708ffeb24da1))

* feat: stub for running summary stats ingestion ([`f99d6b2`](https://github.com/opentargets/genetics_etl_python/commit/f99d6b2ab62c3f2d513bd6a8bc0fb15a0b943935))

* feat: implement FinnGen summary stats ingestion ([`202e788`](https://github.com/opentargets/genetics_etl_python/commit/202e7889c1e9340305c6117074b658514cb35e4c))

* feat: port FinnGen study index ingestion ([`580552d`](https://github.com/opentargets/genetics_etl_python/commit/580552dad8f06eb8fd72b10563e2a8f27a0fedad))

* feat: separate Preprocess code ([`e777a7b`](https://github.com/opentargets/genetics_etl_python/commit/e777a7b3f4b8945ac2b7661884d0584424550860))

* feat: update Makefile to upload Preprocess files ([`b320fde`](https://github.com/opentargets/genetics_etl_python/commit/b320fde261710f0ca2691411526f8605d72ae539))

* feat: add logic to predict step ([`856ce3d`](https://github.com/opentargets/genetics_etl_python/commit/856ce3dc6fda968946aec68d91c5efd3a3a15a93))

* feat: add fm schema ([`6f2a89d`](https://github.com/opentargets/genetics_etl_python/commit/6f2a89daa5573da577cff2a6dc2411956d667e70))

* feat: add feature list as configuration ([`be0e4df`](https://github.com/opentargets/genetics_etl_python/commit/be0e4dfbcc02df3879e679d32076888487ed6e49))

* feat: improve LocusToGeneModel definition and usage ([`18082db`](https://github.com/opentargets/genetics_etl_python/commit/18082dbf29e39d4657b2f67580a3c3e568b322b7))

* feat: wip work with real data ([`425c051`](https://github.com/opentargets/genetics_etl_python/commit/425c05116746fcf734ebc865001b37d16580cd4f))

* feat: etl runs, tests fail ([`af6a1b1`](https://github.com/opentargets/genetics_etl_python/commit/af6a1b113cc9a6282d7a175ecc658d4fde41873e))

* feat: _convert_from_wide_to_long with spark ([`88f1c2b`](https://github.com/opentargets/genetics_etl_python/commit/88f1c2b502c26083dc49f61e730c31ec53b733d3))

* feat: convert the distance features to long format ([`2e51cf6`](https://github.com/opentargets/genetics_etl_python/commit/2e51cf6763f9e1a787b614aa796360ba77fc4629))

* feat: add utils to parse schema from pandas ([`8ff2c2e`](https://github.com/opentargets/genetics_etl_python/commit/8ff2c2ea7bc6c21f73000b5c9766fef788d7e3cb))

* feat: add common functions to melt and pivot dfs ([`db76ada`](https://github.com/opentargets/genetics_etl_python/commit/db76adafa56b596da6e1967e1204285e3654bea9))

* feat: checkpoint, cli works ([`62d8c34`](https://github.com/opentargets/genetics_etl_python/commit/62d8c34507e56ce48d9ee5fdfba8aa8bfcc0b0cb))

* feat: remove dataset configs, will be handled with struct configs ([`75b740a`](https://github.com/opentargets/genetics_etl_python/commit/75b740a99ec511aeb4bf69de99a5b6082f6f16ff))

* feat: l2g step configuration step accommodated in the general cli ([`55a9735`](https://github.com/opentargets/genetics_etl_python/commit/55a973539ae50bb549bcbae581287100c7dca3a7))

* feat: rename datasets schema to _schema, organise config classes, and progress with cli ([`b5563b1`](https://github.com/opentargets/genetics_etl_python/commit/b5563b1ca729c0966b752309015e67ee4af42abf))

* feat: implement config for L2GStep ([`a0cf9db`](https://github.com/opentargets/genetics_etl_python/commit/a0cf9db2fb55b50a47c20d8d5981ab2a3535936f))

* feat: checkpoint ([`388bc30`](https://github.com/opentargets/genetics_etl_python/commit/388bc30c675b2bf61ab433958b25bd89f054832d))

* feat: create mock coloc, study locus and other minor fixes ([`6688fe3`](https://github.com/opentargets/genetics_etl_python/commit/6688fe32134ff2648e9fcc88ab05f3299369ed2b))

* feat: add geneId to studies schema ([`0488588`](https://github.com/opentargets/genetics_etl_python/commit/048858849099a63c7846b434492a3fe4492196a3))

* feat: calculate coloc, naive distance features as class methods ([`50f12d5`](https://github.com/opentargets/genetics_etl_python/commit/50f12d558e2656e3b2625234075b7b6f8a906b35))

* feat: l2g targets skeleton ([`10c76e5`](https://github.com/opentargets/genetics_etl_python/commit/10c76e57a4a955cd32fd5296dd41c2f384e3ab32))

* feat: dont filter credible set by default ([`85f9f75`](https://github.com/opentargets/genetics_etl_python/commit/85f9f750db64de312632940a1e7e31e112285560))

* feat: enhancements around pics and annotate credible set ([`1ae7774`](https://github.com/opentargets/genetics_etl_python/commit/1ae7774043238d3bc131fab51947ee85bf29c75b))

* feat: prevent precommit issues with imports ([`fc671be`](https://github.com/opentargets/genetics_etl_python/commit/fc671be123e2da8c21613c89be5416891b8547e0))

* feat: flexible window set up for locus collection ([`e79284f`](https://github.com/opentargets/genetics_etl_python/commit/e79284f46ec5e8e73a61f8b4fe623705812d0fe1))

* feat: handling studyLocus with existing ldSet ([`5ac38a8`](https://github.com/opentargets/genetics_etl_python/commit/5ac38a8f17e597f21f63d5e5b31bd64fe8c6aabd))

* feat: ld annotation ([`4ff00a5`](https://github.com/opentargets/genetics_etl_python/commit/4ff00a5a8f4b968c5f7c3b9f7eabb222efa4b0ee))

* feat: deprecate function ([`9c706be`](https://github.com/opentargets/genetics_etl_python/commit/9c706beffcd94e5ee094b513664fa84b1d097ef1))

* feat: various pics fixes ([`2b89385`](https://github.com/opentargets/genetics_etl_python/commit/2b89385e2732c6afce4856ed38ac76c5a29acff6))

* feat: generalizing ancestry mapping ([`f075fcf`](https://github.com/opentargets/genetics_etl_python/commit/f075fcf1a6476797bdc5e6bc1d4904efe9e07bd1))

* feat(gcsc): functionality for the google cloud storage connector ([`8fc535c`](https://github.com/opentargets/genetics_etl_python/commit/8fc535c9b1d675a258b33db2cabd51e91023532b))

* feat: intervals tests with several bugfixes ([`320a5da`](https://github.com/opentargets/genetics_etl_python/commit/320a5daa77d171657a892e39ea26527ffc7dce71))

* feat: moving update logic to a shell file ([`1f2773f`](https://github.com/opentargets/genetics_etl_python/commit/1f2773ff0d4f474c0d29b135b155a840a1110527))

* feat: moving update logic to a shell file ([`feb550a`](https://github.com/opentargets/genetics_etl_python/commit/feb550aa1b989f034b706d1afe1def968b6f0d4d))

* feat: adding notebook to update GWAS Catalog data on GCP ([`ad97802`](https://github.com/opentargets/genetics_etl_python/commit/ad97802b2494bd127211f727000f51455d44ef27))

* feat: externalise data sources ([`ee80e87`](https://github.com/opentargets/genetics_etl_python/commit/ee80e87a89f618021c967ad4588136196619a5c2))

* feat: add common spark testing config ([`8deb291`](https://github.com/opentargets/genetics_etl_python/commit/8deb29152a29a87f92426f108cae7f48ce07dbd6))

* feat(session): add `extended_conf` parameter ([`3fbfe24`](https://github.com/opentargets/genetics_etl_python/commit/3fbfe242160cb7e5fc0ea9ac703469f890df28d0))

* feat: ensure schema columns are camelcase ([`1570150`](https://github.com/opentargets/genetics_etl_python/commit/1570150dca3171d1a881e60fe1f2a98d827d10be))

* feat: incomplete renaming of tags ([`d5bd474`](https://github.com/opentargets/genetics_etl_python/commit/d5bd474156ddcac8c53dae2f78ca7cf9a3363ce6))

* feat: move pytest instructions to project level ([`73a8479`](https://github.com/opentargets/genetics_etl_python/commit/73a84799fd530cf97890c835bc8c53bfa7d8c747))

* feat: adding xdist function in github action ([`694595d`](https://github.com/opentargets/genetics_etl_python/commit/694595d566c125eec2a58c710b90d0ee4c39361f))

* feat: implementation of xdist ([`e9e90a4`](https://github.com/opentargets/genetics_etl_python/commit/e9e90a412687301c7e60de6e1c5ce92d1d6c5790))

* feat: rename credibleSet to locus ([`2dae1a5`](https://github.com/opentargets/genetics_etl_python/commit/2dae1a591d9a71b742adb2c8a7c5cd7adcbdbff0))

* feat: docs build is now tested! ([`4e08c2d`](https://github.com/opentargets/genetics_etl_python/commit/4e08c2d6d5121cee01d6539cc255f4782c99093a))

* feat: convert assign_study_locus_id to a class method ([`6bf9932`](https://github.com/opentargets/genetics_etl_python/commit/6bf99328d7ea15408fd404eadf6278566b67c9ca))

* feat: remove redundant assign_study_locus_id ([`650c77a`](https://github.com/opentargets/genetics_etl_python/commit/650c77ab6197cc1dcfd6225d205e71c4ac1c03b2))

* feat: move `get_study_locus_id` inside studylocus ([`0502ad8`](https://github.com/opentargets/genetics_etl_python/commit/0502ad89449d14e14ddfccd7b1031a2c5e01ef26))

* feat: move `get_study_locus_id` inside studylocus ([`f53940f`](https://github.com/opentargets/genetics_etl_python/commit/f53940fcd33ee434ba553c35da9cb37cf2eb04b5))

* feat: reducing data by clustering ([`2a3d1d2`](https://github.com/opentargets/genetics_etl_python/commit/2a3d1d2fd0f21260052027bbc82e15b49f3eecdf))

* feat: purely window based clustering ([`f8f1e18`](https://github.com/opentargets/genetics_etl_python/commit/f8f1e18805f54e918a0195b398b206a3b268e31c))

* feat: testing window based clumping in real data ([`bd7ecdb`](https://github.com/opentargets/genetics_etl_python/commit/bd7ecdb2a2aa260e1e2e7c7150114bdc8eb74488))

* feat: adding exclusion filter to sumstats ([`0d794bf`](https://github.com/opentargets/genetics_etl_python/commit/0d794bf57675f209601b83da7ea7dd9f7093d69d))

* feat(makefile): create rule to format, test, and build docs ([`76e3501`](https://github.com/opentargets/genetics_etl_python/commit/76e35011ff3ad7f97425502f677841de41562fbc))

* feat(pre-commit-hooks): deprecate flake8 and add ruff ([`f72dc1b`](https://github.com/opentargets/genetics_etl_python/commit/f72dc1b3e234ea1e31d13b59ec52be300726ff41))

* feat(linter): deprecate flake8 and set up ruff in project and vscode settings ([`b7e7a68`](https://github.com/opentargets/genetics_etl_python/commit/b7e7a689f82eb43bfe0a066567f4ee7f40deb557))

* feat: wokflow is now triggered manually (rename required) ([`291eb66`](https://github.com/opentargets/genetics_etl_python/commit/291eb664eb7e1578ba2dd0aa8d11a108fd7f13eb))

* feat(validate_schema): only compare field names when looking for unexpected extra fields ([`2519ac9`](https://github.com/opentargets/genetics_etl_python/commit/2519ac9c4405a1d68498d49b3a64626512bbbcd6))

* feat: add skeleton for overlaps step ([`380b0d3`](https://github.com/opentargets/genetics_etl_python/commit/380b0d3bc0a4bc325b8ae12f31333768f1db6654))

* feat(_align_overlapping_tags): nest stats ([`cf9af58`](https://github.com/opentargets/genetics_etl_python/commit/cf9af584e6e5e31143511bea33863411c885055d))

* feat(study_locus): bring more stats columns in `find_overlaps` ([`dc63a97`](https://github.com/opentargets/genetics_etl_python/commit/dc63a9795f89c845a218fe95901f5be5db167adb))

* feat(overlaps_schema): add pvalue, beta and nest statistics ([`6144335`](https://github.com/opentargets/genetics_etl_python/commit/6144335d756dcbf43347e285fc3b611b013d3bf7))

* feat: first dag ([`d675d91`](https://github.com/opentargets/genetics_etl_python/commit/d675d913a14fd11af598e5308c82fcb4e180ea0f))

* feat: first dag ([`6f28714`](https://github.com/opentargets/genetics_etl_python/commit/6f28714c2848581164740778229f2979660d6857))

* feat(Makefile): add ([`afad268`](https://github.com/opentargets/genetics_etl_python/commit/afad268123cebb0193c9151f4f0ccad835b857c5))

* feat(cluster_init): uninstall otgenetics if exists ([`d4ca807`](https://github.com/opentargets/genetics_etl_python/commit/d4ca807afd96b3d7a5cf3f013ca20adecc683ad6))

* feat(Workflow): add args for ssd and disk size ([`45aee26`](https://github.com/opentargets/genetics_etl_python/commit/45aee2632c3e68392116233f39d2af6d1f52a42f))

* feat: change repartition strategy before aggregation ([`83e515e`](https://github.com/opentargets/genetics_etl_python/commit/83e515e7b23f6ed8727d911c7fc45ac6c3628f77))

* feat(Workflow): assign 1000GB to disk boot size ([`0eb317f`](https://github.com/opentargets/genetics_etl_python/commit/0eb317f7f92c133b9644f024667119b934786f95))

* feat(session): assign executors dinamically based on resources ([`7c21ee5`](https://github.com/opentargets/genetics_etl_python/commit/7c21ee530bfe715290e5fe816d378c89c170e30f))

* feat: feat(workflow_template): assign ssd as primary disk + increase max disk utilization threshold ([`df70a0d`](https://github.com/opentargets/genetics_etl_python/commit/df70a0d295846b6da3158d3cb70e4ed19b81d050))

* feat: add persist/unpersist methods to `dataset` ([`1d8a82d`](https://github.com/opentargets/genetics_etl_python/commit/1d8a82d2059ad3599dc7a76f6a981015b75d22fa))

* feat: add kwargs compatibility to `from_parquet` ([`9954fb1`](https://github.com/opentargets/genetics_etl_python/commit/9954fb1dbcac8afc93f2e0529259b4202dbf10d8))

* feat: rewrite `TestValidateSchema` to follow new paradigm ([`26688a9`](https://github.com/opentargets/genetics_etl_python/commit/26688a96ddd247ce785dcc7bf38b84222eb4fb97))

* feat: remove `from_parquet` and add `_get_schema` to every Dataset child class ([`71e3b85`](https://github.com/opentargets/genetics_etl_python/commit/71e3b856f44e161b85db842bbfde9adcf66efddf))

* feat(Dataset): add abstract class to get schema and invoke this inside from_parquet ([`d35d695`](https://github.com/opentargets/genetics_etl_python/commit/d35d69518a6690dc8406d733b25b446d1b89c1fd))

* feat: filter credibleSet to only include variants from the 95% set ([`c4fb894`](https://github.com/opentargets/genetics_etl_python/commit/c4fb8942c90766bb8c66f2354cf6c496d222ac38))

* feat(ldindex): partition by chromosome ([`ee72268`](https://github.com/opentargets/genetics_etl_python/commit/ee722688b109d5c06b843567e5c824964ea2523c))

* feat(ldindex): partition by chromosome ([`6eb9b0a`](https://github.com/opentargets/genetics_etl_python/commit/6eb9b0a6aa5345d4b42d8778f0909e478277cedc))

* feat: adapt gwas_catalog module ([`c12d6db`](https://github.com/opentargets/genetics_etl_python/commit/c12d6dbd3f68c6cf0a3cb3786bc742b16879ca29))

* feat: add `ldSet` to study_locus schema ([`9c26c0b`](https://github.com/opentargets/genetics_etl_python/commit/9c26c0b3b1d1b283c85a644d0bad2caa92f60b4e))

* feat: rewrite `LDAnnotator` ([`cf27bf1`](https://github.com/opentargets/genetics_etl_python/commit/cf27bf16c66a75cce6e10d95be4fd712f3bdaca3))

* feat: refactor `get_gnomad_ancestry_sample_sizes` to `get_gnomad_population_structure` ([`a83a24b`](https://github.com/opentargets/genetics_etl_python/commit/a83a24bd0b3f0e8f7b42765f91a7fc336795f9e8))

* feat: add `_aggregate_ld_index_across_populations` ([`a0fc365`](https://github.com/opentargets/genetics_etl_python/commit/a0fc3650d97098e5f0060532ba4632be7a22017f))

* feat(_create_ldindex_for_population): add population_id ([`f7d5ffa`](https://github.com/opentargets/genetics_etl_python/commit/f7d5ffa501fa5326d0363f151f1e5831c09d3b51))

* feat: deprecate old ld_index dataset ([`c718158`](https://github.com/opentargets/genetics_etl_python/commit/c718158cc59f61b2a860e0aea323aec638897fcd))

* feat(ldindex): add `resolve_variant_indices` ([`03cae0e`](https://github.com/opentargets/genetics_etl_python/commit/03cae0ef97d36040c892d906c7e0547120c385cd))

* feat: wip new ldset dataset ([`4a4594e`](https://github.com/opentargets/genetics_etl_python/commit/4a4594e6ec3327ef045a6d6d0e7fbc246ff3b14b))

* feat: write reference agnostic liftover function ([`3a2cb9f`](https://github.com/opentargets/genetics_etl_python/commit/3a2cb9f0996b426310c215aa376c1159546251cb))

* feat: change r2 threshold to retrieve all variants above .2 and apply filter by .5 on r2overall ([`1789fe4`](https://github.com/opentargets/genetics_etl_python/commit/1789fe4c2e75c5896a26f359345ef80528a8667e))

* feat(_variant_coordinates_in_ldindex): replace groupby by select ([`d3a2c15`](https://github.com/opentargets/genetics_etl_python/commit/d3a2c158405517e04117c9950497c6e24f95d707))

* feat: add `get_study_locus_id` ([`6c9f98f`](https://github.com/opentargets/genetics_etl_python/commit/6c9f98f3c5d12f1b10b6c74063447043414402b5))

* feat: fix ldclumping, tests, and docs ([`68c204e`](https://github.com/opentargets/genetics_etl_python/commit/68c204e80b83891854d9dcac6ef2e4cd5dc77fc6))

* feat: function to clump a summary statistics object ([`19ece6d`](https://github.com/opentargets/genetics_etl_python/commit/19ece6d7df3490e0602c29b7fdc0f21af04df258))

* feat: adding main logic ([`338ed5c`](https://github.com/opentargets/genetics_etl_python/commit/338ed5cc162f8a9a117a58d6e4ade71589ce8060))

* feat: add ld_matrix step ([`fa65725`](https://github.com/opentargets/genetics_etl_python/commit/fa65725a445ff7b1947e691ab1ef76b685742ae1))

* feat(ld): remove unnecessary grouping - variants are no longer duplicated ([`75c7856`](https://github.com/opentargets/genetics_etl_python/commit/75c7856b03bbd65c225d2aaeec88d467cdc54d70))

* feat(ldindex): remove ambiguous variants ([`585f9e2`](https://github.com/opentargets/genetics_etl_python/commit/585f9e2a2517f9df5eb99b5116f6724804228cd3))

* feat: update ingestion fields ([`e397e8e`](https://github.com/opentargets/genetics_etl_python/commit/e397e8eefeb5bf7f0bf6b81382bbb8d731eb7cb1))

* feat: update input/output paths ([`dfe5341`](https://github.com/opentargets/genetics_etl_python/commit/dfe534151e0e714e720f628d3fea19a140cd941f))

* feat: add create-dev-cluster rule to makefile (#94) ([`88ea59f`](https://github.com/opentargets/genetics_etl_python/commit/88ea59f2e60159cbc7afc603396ccc41888b2d6d))

* feat: ukbiobank sample test data ([`a0017ae`](https://github.com/opentargets/genetics_etl_python/commit/a0017aeb728cce292e8494bdf74b07ad0395f613))

* feat: ukbiobank components ([`e7b8562`](https://github.com/opentargets/genetics_etl_python/commit/e7b85627d412c2c3bc53b9ebf85cfbaaea636856))

* feat: add UKBiobank config yaml ([`fb2a6ba`](https://github.com/opentargets/genetics_etl_python/commit/fb2a6ba8a6f3c519424a381fd6f4b85db3754a36))

* feat: add UKBiobank step ID ([`dd5f77b`](https://github.com/opentargets/genetics_etl_python/commit/dd5f77b707969a129ff2c47f0fb5acf092d8bd9f))

* feat: add UKBiobank inputs and outputs ([`1e8fc0f`](https://github.com/opentargets/genetics_etl_python/commit/1e8fc0f9e5c7d5fe94580b6a35b6caa87cecbfdb))

* feat: add UKBiobankStepConfig class ([`34e573f`](https://github.com/opentargets/genetics_etl_python/commit/34e573f9fd51e41d1364077da51497999ed4f1ef))

* feat: add StudyIndexUKBiobank class ([`80d3cd0`](https://github.com/opentargets/genetics_etl_python/commit/80d3cd015c43b06b8055cd1d3673479b36ed80c2))

* feat: ukbiobank study ingestion ([`53dab79`](https://github.com/opentargets/genetics_etl_python/commit/53dab790359adce8767fa55eab419b752d84ea2d))

* feat: replace hardcoded parameters with cutomisable ones ([`bf0c5de`](https://github.com/opentargets/genetics_etl_python/commit/bf0c5de10756e0d084372448cdd7b43f359fa9cb))

* feat: run the code from the version specific path ([`452174b`](https://github.com/opentargets/genetics_etl_python/commit/452174bdfbd8cb93692cbe09ef3af311e6696e43))

* feat: upload code to a version specific path ([`db60882`](https://github.com/opentargets/genetics_etl_python/commit/db60882c8fd656db90510441d2dc69e20dcf8550))

* feat: extract `order_array_of_structs_by_field`, handle nulls and test ([`4bd64e2`](https://github.com/opentargets/genetics_etl_python/commit/4bd64e2fc5c0bbd73b6471a44adaac4d12ace521))

* feat: order output of `_variant_coordinates_in_ldindex` and tests ([`f0d6bc2`](https://github.com/opentargets/genetics_etl_python/commit/f0d6bc2a900a0b4784da810e649c67eccbda0cff))

* feat: extract `get_ld_annotated_assocs_for_population` ([`30fe8e3`](https://github.com/opentargets/genetics_etl_python/commit/30fe8e326248948b578a7fa631a6732b3cfe6f6f))

* feat: extract methods from `variants_in_ld_in_gnomad_pop` (passes) ([`2895e24`](https://github.com/opentargets/genetics_etl_python/commit/2895e24c1aa53460f0d83a5aa496edcca76b1586))

* feat: rewrite FinnGenStep to follow the new logic ([`4b866fe`](https://github.com/opentargets/genetics_etl_python/commit/4b866fe87fd1bcd008a74e95bc4ecd67e2dda6c5))

* feat: implement the new StudyIndexFinnGen class ([`fbe2719`](https://github.com/opentargets/genetics_etl_python/commit/fbe2719e795027cac795186e64b9a52d89e20404))

* feat: add step to DAG ([`0173326`](https://github.com/opentargets/genetics_etl_python/commit/0173326cbf2c27494a2e28f9a5fd43ad5293816c))

* feat: update FinnGen ingestion code in line with the current state of the main branch ([`e6b6397`](https://github.com/opentargets/genetics_etl_python/commit/e6b6397cd853f245be2dd7bbef994ee7402dd356))

* feat: pass configuration values through FinnGenStepConfig ([`ad5e529`](https://github.com/opentargets/genetics_etl_python/commit/ad5e52937dd555388afe433290b503aaa7462e22))

* feat: pass FinnGen configuration through my_finngen.yaml ([`0f113dd`](https://github.com/opentargets/genetics_etl_python/commit/0f113ddbb43486df1964407b6f653cebcecb804e))

* feat: rewrite EFO mapping in pure PySpark ([`6db602a`](https://github.com/opentargets/genetics_etl_python/commit/6db602a67561e6cc4e748d2272c7fb922d561f17))

* feat: validate FinnGen study table against schema ([`76911d1`](https://github.com/opentargets/genetics_etl_python/commit/76911d18a2daa71efe07f1c209114a36ea200ba3))

* feat: implement EFO mapping ([`7ce6437`](https://github.com/opentargets/genetics_etl_python/commit/7ce6437a50f84acc162443935ffb7a6f036464bd))

* feat: write FinnGen study table ingestion ([`19389e8`](https://github.com/opentargets/genetics_etl_python/commit/19389e8ee66579a4d23c2654fe9f0cf197f4e8a6))

* feat: added run script for FinnGen ingestion ([`be5431c`](https://github.com/opentargets/genetics_etl_python/commit/be5431c57f63e37da6d747fe8de62924c77984de))

* feat: fix PICS fine mapping function ([`cd4dee5`](https://github.com/opentargets/genetics_etl_python/commit/cd4dee540f6207f2aa919c69dc4b3ae9cb1c7fb1))

* feat: deprecate StudyLocus._is_in_credset ([`a4bb77f`](https://github.com/opentargets/genetics_etl_python/commit/a4bb77fbfdf89a45f650404a43ff5987f08ff3ed))

* feat: improved flattening of nested structures ([`b2c9e8b`](https://github.com/opentargets/genetics_etl_python/commit/b2c9e8b42811d6eb7790e416df987b26d0f53c03))

* feat: new annotate_credible_sets function and tests ([`779eaf3`](https://github.com/opentargets/genetics_etl_python/commit/779eaf3aac221526f05ac87c9ba8e49c10c263e6))

* feat: changes on the tests sessions with the hope to speed up tests ([`0292454`](https://github.com/opentargets/genetics_etl_python/commit/02924546031fa515ac6b0632f9d6c10289299771))

* feat: update dependencies to GCP 2.1 image version ([`8070a40`](https://github.com/opentargets/genetics_etl_python/commit/8070a4040ceaed4603da754ca91c3f3247ed87e4))

* feat: gpt commit summariser ([`5f6e4e1`](https://github.com/opentargets/genetics_etl_python/commit/5f6e4e1f1c224c4a1229f36ff9eecb994212c54b))

* feat: fixes associated with study locus tests ([`630d902`](https://github.com/opentargets/genetics_etl_python/commit/630d902e2776c8383d8c4d246f84f5cde4baf578))

* feat: first slimmed variant annotation version with partial tests ([`d321d90`](https://github.com/opentargets/genetics_etl_python/commit/d321d9028f7fc690723b9b51900126b40e215646))

* feat: docs badge as link

Docs badge as link ([`7847bd7`](https://github.com/opentargets/genetics_etl_python/commit/7847bd7a12fe50d14769f28e772283a54a73522a))

* feat: README minor updates

- docs badge added
- link to documentation
- remove outdated information
- reference to workflow ([`0c966ea`](https://github.com/opentargets/genetics_etl_python/commit/0c966eaeb435f1ccc69f84db1b621950eb3c1e90))

* feat: change  not to return False when df is of size 1 ([`be5b555`](https://github.com/opentargets/genetics_etl_python/commit/be5b5553b59f7bd8531f5d542d24f62cd38c9f8f))

* feat: add check for duplicated field to `validate_schema` ([`d0b3489`](https://github.com/opentargets/genetics_etl_python/commit/d0b34891e16bfa48aaf2c85170d562d8a70fe47c))

* feat: add support and tests for nested data ([`480539b`](https://github.com/opentargets/genetics_etl_python/commit/480539b2ebd8044a4f132cb8b2644f50bed92409))

* feat: merge with remote branch ([`ad95698`](https://github.com/opentargets/genetics_etl_python/commit/ad956980f48172b11cf6f1142631653800018a63))

* feat: add support and tests for nested data ([`51ad0aa`](https://github.com/opentargets/genetics_etl_python/commit/51ad0aa25b4de641f2d683d46909086374870a40))

* feat: added flatten_schema function and test ([`5414538`](https://github.com/opentargets/genetics_etl_python/commit/5414538497f9104e678eb4b80434d00cb637190c))

* feat: add type checking to validate_schema ([`1f8d9a1`](https://github.com/opentargets/genetics_etl_python/commit/1f8d9a18fe04a7cdd5a8cbbb6249e1122216ed72))

* feat: run tests on main when push (or merge) ([`59edc6f`](https://github.com/opentargets/genetics_etl_python/commit/59edc6f98805f66249149deb6b6d5b298938bc13))

* feat: run tests on main when push (or merge) ([`bf9054f`](https://github.com/opentargets/genetics_etl_python/commit/bf9054fc36332e4e962015496580de76e70da7a2))

* feat: run tests on main when push (or merge) ([`e6806aa`](https://github.com/opentargets/genetics_etl_python/commit/e6806aac4ffdf946fe81d42235f5e36119ac79a8))

* feat: redefine validate_schema to avoid nullability issues ([`f1a3fc1`](https://github.com/opentargets/genetics_etl_python/commit/f1a3fc188426c2a8856d2a459b53d819761814ee))

* feat: redefine `validate_schema` to avoid nullability issues ([`838d4f1`](https://github.com/opentargets/genetics_etl_python/commit/838d4f13a4b5af51c51b1a8e1a6a8e1c5f27df5f))

* feat: working version of dataproc workflow ([`5826f79`](https://github.com/opentargets/genetics_etl_python/commit/5826f796a511ced418f9f85f9bd2af242b00d205))

* feat: add logic to predict step ([`dc0d0e3`](https://github.com/opentargets/genetics_etl_python/commit/dc0d0e3a01bac66ea6171e3bc5d2b6d361a4fca4))

* feat: add fm schema ([`6fe2a1d`](https://github.com/opentargets/genetics_etl_python/commit/6fe2a1d50a0c089e6d5ba3710d6bf3bcd7e73fb0))

* feat: first working version of google workflow ([`e67ff48`](https://github.com/opentargets/genetics_etl_python/commit/e67ff48dc2785d3fb9875e0b72eafe17f7d3468e))

* feat: hail session functionality restored ([`6d0a361`](https://github.com/opentargets/genetics_etl_python/commit/6d0a361694292a338690a0e8dd2f3613b38f8924))

* feat: include pyright typechecking in vscode (no precommit) ([`547633e`](https://github.com/opentargets/genetics_etl_python/commit/547633e5c8a074706a8fec1c00b9c0a8f1ce3119))

* feat: sumstats ingestion added ([`e1a9d2c`](https://github.com/opentargets/genetics_etl_python/commit/e1a9d2c2540febadeab2cd36678585d0209c18eb))

* feat: step to preprocess sumstats added ([`d78e835`](https://github.com/opentargets/genetics_etl_python/commit/d78e8357e4081bc981ca4723d7f676b70b7c80c6))

* feat: half-cooked submission using dataproc workflows ([`3cb603b`](https://github.com/opentargets/genetics_etl_python/commit/3cb603b36b59bf6ccf8281c8747d438dcb50cf13))

* feat: vscode isort on save ([`01365ed`](https://github.com/opentargets/genetics_etl_python/commit/01365ed8e8bf1f27c1c86acaef0cf932e0b872dd))

* feat: updating summary stats ingestion ([`7a48fc4`](https://github.com/opentargets/genetics_etl_python/commit/7a48fc484dced3d26575912157cb5dc8e1591d33))

* feat: yaml config for gene index ([`1617635`](https://github.com/opentargets/genetics_etl_python/commit/1617635184172ef184bfd5b74b36106f56de9754))

* feat: fix CLI to work with hydra ([`931c6cd`](https://github.com/opentargets/genetics_etl_python/commit/931c6cd3f4b334a0b7841300ed7fcfddab2c97ff))

* feat: intervals to v2g refactor and test ([`25b86f8`](https://github.com/opentargets/genetics_etl_python/commit/25b86f89dcca7b267d2597668a1598ece11b1a2a))

* feat: remove gcfs dependency ([`7ac4b78`](https://github.com/opentargets/genetics_etl_python/commit/7ac4b78a0db84582eb9539bfbfc781b1692551da))

* feat: bugfixes associated with increased study index coverage ([`54f4fcf`](https://github.com/opentargets/genetics_etl_python/commit/54f4fcfc89bf2327895d900d4c79ae969e2126ec))

* feat: add feature list as configuration ([`0f79daa`](https://github.com/opentargets/genetics_etl_python/commit/0f79daa6873c7dd82d05c6d7b3e81a06a321fee0))

* feat: improve LocusToGeneModel definition and usage ([`9005372`](https://github.com/opentargets/genetics_etl_python/commit/9005372f95f2ad235bcbdda87ae2792e6b2d736a))

* feat: wip work with real data ([`d915699`](https://github.com/opentargets/genetics_etl_python/commit/d9156996926c7c9ec847cb652f0282b44e06846a))

* feat: adding p-value filter for summary stats ([`e19e933`](https://github.com/opentargets/genetics_etl_python/commit/e19e933b9625bd60380d83bd65ac0113d953b7ab))

* feat: adding summary stats dataset ([`4613617`](https://github.com/opentargets/genetics_etl_python/commit/46136171f986a22692114bb140d9dc2ad067344a))

* feat: several fixes linked to increased test coverage ([`445e1cc`](https://github.com/opentargets/genetics_etl_python/commit/445e1cc7eb1995f88d9be2e40fa876cd5ccf15c4))

* feat: etl runs, tests fail ([`16b64f2`](https://github.com/opentargets/genetics_etl_python/commit/16b64f25a827a2c3af9b5c1c95ce468a76fce9f4))

* feat: working hydra config with optional external yaml ([`ad53e7e`](https://github.com/opentargets/genetics_etl_python/commit/ad53e7efd315c10c1f9ac2fb9ce5ec4e1c6bd89f))

* feat: _convert_from_wide_to_long with spark ([`9bf5af7`](https://github.com/opentargets/genetics_etl_python/commit/9bf5af78cb2100eec19ac25018182b50ec5484b2))

* feat: convert the distance features to long format ([`df3ceaa`](https://github.com/opentargets/genetics_etl_python/commit/df3ceaae7ba4b8e38dee6e664e6c4cfc654e6867))

* feat: add utils to parse schema from pandas ([`703f39d`](https://github.com/opentargets/genetics_etl_python/commit/703f39d4e43a8d897f0136dd60ec88a31b125084))

* feat: add common functions to melt and pivot dfs ([`4be0f74`](https://github.com/opentargets/genetics_etl_python/commit/4be0f74d3556ff62bb3bbf2906d5f8031bf870b7))

* feat: checkpoint, cli works ([`37c113d`](https://github.com/opentargets/genetics_etl_python/commit/37c113ded2d6d362930b3037de2dd3e95088aaa2))

* feat: remove dataset configs, will be handled with struct configs ([`5d28580`](https://github.com/opentargets/genetics_etl_python/commit/5d28580f8e629ae088fe76d74f0d5b59231ea88c))

* feat: l2g step configuration step accommodated in the general cli ([`17d3c65`](https://github.com/opentargets/genetics_etl_python/commit/17d3c65b3e5bf65f7bde8ddc256ef9691ee190cd))

* feat: precompute LD index step ([`0756150`](https://github.com/opentargets/genetics_etl_python/commit/075615029785993e4157cd2d0f6d183d26749b46))

* feat: rename datasets schema to _schema, organise config classes, and progress with cli ([`361959c`](https://github.com/opentargets/genetics_etl_python/commit/361959ce13bd123129ab9ad62c520c4df767afdd))

* feat: ld_clumping ([`3aa413f`](https://github.com/opentargets/genetics_etl_python/commit/3aa413fc662fbf23db524b92d58fb0b55eda0887))

* feat: r are combined by weighted mean ([`a1977c8`](https://github.com/opentargets/genetics_etl_python/commit/a1977c8692e299ce4816920cfe4e194ae3fa8d7b))

* feat: pics refactor ([`fa71843`](https://github.com/opentargets/genetics_etl_python/commit/fa71843737dd56a64ee593b398bc44c92164182e))

* feat: ld annotation ([`0bb9529`](https://github.com/opentargets/genetics_etl_python/commit/0bb952956cc610e2cc81e53cb2aed38e6b93923a))

* feat: gwas catalog splitter ([`1418908`](https://github.com/opentargets/genetics_etl_python/commit/1418908fe56ba68c5473e368789846ec136818f6))

* feat: implement config for L2GStep ([`5b784cc`](https://github.com/opentargets/genetics_etl_python/commit/5b784cc29fd7a4b248e88c27f7304abddaf79001))

* feat: contributors list ([`32de81f`](https://github.com/opentargets/genetics_etl_python/commit/32de81f4235291562af7b2111b9f29671fb0fcd8))

* feat: checkpoint ([`4244a36`](https://github.com/opentargets/genetics_etl_python/commit/4244a363e8ad593c20361054abd9c7d03c81bc3a))

* feat: create mock coloc, study locus and other minor fixes ([`1065a6a`](https://github.com/opentargets/genetics_etl_python/commit/1065a6a0c4b1361e3a39cc4b1f7a3f42c660513c))

* feat: add geneId to studies schema ([`851a15a`](https://github.com/opentargets/genetics_etl_python/commit/851a15a0798d9032252c8cb7d0cb928d742cc187))

* feat: calculate coloc, naive distance features as class methods ([`88682aa`](https://github.com/opentargets/genetics_etl_python/commit/88682aa387989beeb783f95ad31d3c583256eb4a))

* feat: distance to TSS v2g feature ([`a628046`](https://github.com/opentargets/genetics_etl_python/commit/a628046f4af2ce3145470d27b2945f38ed43ea52))

* feat: schemas are now displayed in documentation! ([`4681940`](https://github.com/opentargets/genetics_etl_python/commit/46819403a7e0ac78809b4c7dd5ca5cfc68b34891))

* feat: default p-value threshold ([`9b00e01`](https://github.com/opentargets/genetics_etl_python/commit/9b00e015579b5f18e5e0cebed6addcaa0c0c95ca))

* feat: minor improvements in docs ([`a5f0f02`](https://github.com/opentargets/genetics_etl_python/commit/a5f0f02954561817972d6f7b8c707454a7e5734a))

* feat: major changes on association parsing ([`c1cfee9`](https://github.com/opentargets/genetics_etl_python/commit/c1cfee9e1b42d59acbc26dfb13e12fe2c8b53223))

* feat: graph based clumping ([`31cab8d`](https://github.com/opentargets/genetics_etl_python/commit/31cab8de2d0aa206211e578aa0fb701dd5e064b2))

* feat: gs checkpoint ([`ec3e686`](https://github.com/opentargets/genetics_etl_python/commit/ec3e686bf555100c714a8f89a7f2d20d7392061b))

* feat: function to install custom jars ([`1dd6796`](https://github.com/opentargets/genetics_etl_python/commit/1dd679643779392b7275f54596be07e644090c5d))

* feat: l2g targets skeleton ([`18af3a7`](https://github.com/opentargets/genetics_etl_python/commit/18af3a70b292a11ff2be6c7fc837769990ade7f1))

* feat: minor changes in the parser ([`cfb9f11`](https://github.com/opentargets/genetics_etl_python/commit/cfb9f11d9b8a728c94b12b915e58eef1c2b25176))

* feat: gwascat study ingestion preliminary version ([`5a101d6`](https://github.com/opentargets/genetics_etl_python/commit/5a101d6a7556b1e4aba1367a1690948a95260749))

* feat: incorporating iteration including colocalisation steps ([`d8bc103`](https://github.com/opentargets/genetics_etl_python/commit/d8bc1030a5650089a1e49b381c818dd8adc51169))

* feat: exploring tests with dbldatagen ([`335a689`](https://github.com/opentargets/genetics_etl_python/commit/335a68928fe89e2bf86e21bc23ac7c6ea5d8afa6))

* feat: more step config less class ([`7d38ba8`](https://github.com/opentargets/genetics_etl_python/commit/7d38ba89913cfcb105bf0a925c1aad2f216c40e9))

* feat: not tested va, vi and v2g ([`094d547`](https://github.com/opentargets/genetics_etl_python/commit/094d5475fd6eb3486d30a99a749daea5644de74f))

* feat: directory name changed ([`9893398`](https://github.com/opentargets/genetics_etl_python/commit/989339899c55e7b0a56f68c10742a62fad53c194))

* feat: merging main into il-v2g-distance ([`f75bb40`](https://github.com/opentargets/genetics_etl_python/commit/f75bb40902f3b92aca8cc5794250cb3f4d70c2c5))

* feat: cli working! ([`fa3bdbb`](https://github.com/opentargets/genetics_etl_python/commit/fa3bdbb7bed7f12b675f0755c77c7cc6f7f7c80c))

* feat: first prototype of CLI using do_hydra

Currently implements the variant_annotation and variant_index steps.

Note: During pre-commit pycln introduced a bug in the code by introducing
a functionality that it&#39;s only available in python 3.10+.
To fix it we need to revert `path: str | None` to `path: Optional[str]`
in `variant_annotation.py` and `variant_index.py`. ([`744bde6`](https://github.com/opentargets/genetics_etl_python/commit/744bde62754ef84c8a36f624edd91d075989d3aa))

* feat: poetry lock updated ([`f2167e5`](https://github.com/opentargets/genetics_etl_python/commit/f2167e56d45b03a9509665a015e1ffdae71a4622))

* feat: remove `phenotype_id_gene` dependency ([`c99d2a8`](https://github.com/opentargets/genetics_etl_python/commit/c99d2a890dbdb2dbc24db8c193b7ea3fc78e8132))

* feat: adapt coloc code to newer datasets ([`3ba7a2c`](https://github.com/opentargets/genetics_etl_python/commit/3ba7a2c2675d81cdfbe93e1e7d6d7779fc1215d9))

* feat: merge ([`ace2f31`](https://github.com/opentargets/genetics_etl_python/commit/ace2f31e91821557711a13aff06a4357cda7f7d6))

* feat: reverting flake8 to 5.0.4 ([`0956965`](https://github.com/opentargets/genetics_etl_python/commit/0956965cb36ae19685256df43e3c23c6271e7714))

* feat: feat: adapt coloc to follow new credset schema ([`c5a2e3b`](https://github.com/opentargets/genetics_etl_python/commit/c5a2e3b6346d2cb0f6906c83a33386fbdbb2be01))

* feat: ignore docstrings from private functions in documentation ([`e443211`](https://github.com/opentargets/genetics_etl_python/commit/e4432115bfff0734efb0a2556cfc8400a5a0f69a))

* feat: plugin not to automatically handle required pages ([`3f4b797`](https://github.com/opentargets/genetics_etl_python/commit/3f4b797a37152395f73924a8b3f68972847e9f67))

* feat: filter v2g based on biotypes ([`c044cd4`](https://github.com/opentargets/genetics_etl_python/commit/c044cd4170bebbbaaab2b5471c2fcb7ae1308a92))

* feat: generate v2g independently and read from generated intervals ([`38c7a9a`](https://github.com/opentargets/genetics_etl_python/commit/38c7a9a9d43b88e7617c50a0157875f1136cc74c))

* feat: compute distance to tss v2g functions ([`5c28bee`](https://github.com/opentargets/genetics_etl_python/commit/5c28bee93fed64bd928178bbd3290014ab61bc8a))

* feat: v2g schema v1.0 added ([`27da971`](https://github.com/opentargets/genetics_etl_python/commit/27da9712cefc17d7203fbd59ae3a0fd683f577cd))

* feat: add polyphen, plof, and sift to v2g ([`6b408ca`](https://github.com/opentargets/genetics_etl_python/commit/6b408cab59237136ab70017bd9f7671ec943fa6d))

* feat: trialing codecov coverage threshold ([`8ecb1b1`](https://github.com/opentargets/genetics_etl_python/commit/8ecb1b1cb1bcaaea31cc380eb547dac0c21da3a1))

* feat: integrate interval parsers in v2g model ([`7bc59ef`](https://github.com/opentargets/genetics_etl_python/commit/7bc59efe0bd21d5b80489d375bec82eebfb82f12))

* feat: rearrange interval scripts into vep ([`f1fc024`](https://github.com/opentargets/genetics_etl_python/commit/f1fc024b76e2c9aca4af54703f3be5c6a114178d))

* feat: add skeleton of vep processing ([`fed7eb2`](https://github.com/opentargets/genetics_etl_python/commit/fed7eb2bba4d763fc37f6bb89691a34a641da6d4))

* feat: ipython support

- iPython is installed as development dependency
- Vscode configured to run with ipython ([`1e1717d`](https://github.com/opentargets/genetics_etl_python/commit/1e1717ddcd68daf87d1f45d8f31745c7b28f34c5))

* feat: token added ([`3fa9c76`](https://github.com/opentargets/genetics_etl_python/commit/3fa9c761c0b7f3f053b8051536cc2b1d30dddaf2))

* feat: trigger testing on push ([`76fedda`](https://github.com/opentargets/genetics_etl_python/commit/76feddac76acdd52bbf9af2cbf551d75bfeed21c))

* feat: extra config for codecov ([`e86bfaa`](https://github.com/opentargets/genetics_etl_python/commit/e86bfaaa45f3e248fc18c6fd11e354fd377255dc))

* feat: enhanced codecov options ([`7ab4345`](https://github.com/opentargets/genetics_etl_python/commit/7ab4345c48a59310691ce5bb5c8014fa4c39f333))

* feat: codecov integration ([`9c88b43`](https://github.com/opentargets/genetics_etl_python/commit/9c88b4317f9776e731ef6c8a5759438d242f7809))

* feat: spark namespace to be reused in doctests ([`d9bbe5f`](https://github.com/opentargets/genetics_etl_python/commit/d9bbe5f5b4a84f58a2b6670691b4b79348f7982b))

* feat: validate studies schema ([`9e74b64`](https://github.com/opentargets/genetics_etl_python/commit/9e74b6416a9a776937947548acbd698bcd3418c8))

* feat: update actions to handle groups ([`23a7b6b`](https://github.com/opentargets/genetics_etl_python/commit/23a7b6b1175454762c88b1e29c3daca208bee861))

* feat: license added ([`c722d30`](https://github.com/opentargets/genetics_etl_python/commit/c722d306a640494a598713268d607993972a4978))

* feat: mkdocs github action ([`dcae258`](https://github.com/opentargets/genetics_etl_python/commit/dcae2581d074768b1786b960ef571d35d7202354))

* feat: first version of mkdocs site ([`875d445`](https://github.com/opentargets/genetics_etl_python/commit/875d445d503a1779827c0984183774731bee4946))

* feat: generate study table for GWAS Catalog ([`a0e05d0`](https://github.com/opentargets/genetics_etl_python/commit/a0e05d0424c7062346c93d5d6e2c8e12667497e9))

* feat: docs support

Overall support for docstrings
    - 100% docs coverage
    - docstring linters (flake8-docstring)
    - docstring content checkers (darlint)
    - docstring pre-commit
    - interrogate aims for 95% docs coverage
    - vscode settings bug fixed ([`d7d40c8`](https://github.com/opentargets/genetics_etl_python/commit/d7d40c8c4fda9e7059046616309d8d9407f2b1e1))

* feat: non-nullable fields required in validated df ([`01362c3`](https://github.com/opentargets/genetics_etl_python/commit/01362c330786752e745e119a4e7a4e6e69c02bd0))

* feat: intervals DataFrames are now validated ([`5fd35af`](https://github.com/opentargets/genetics_etl_python/commit/5fd35af91ab0abfc6ab628b099ed57754dfcdd39))

* feat: new mirrors-mypy precommit version ([`63ea0de`](https://github.com/opentargets/genetics_etl_python/commit/63ea0def8def7bf6e21233922eca3acb8f9cc1b1))

* feat: intervals schema added ([`d1b887b`](https://github.com/opentargets/genetics_etl_python/commit/d1b887bf84dfea1cc645f93b882dc5f8e6d31d0f))

* feat: function to validate dataframe schema ([`4393739`](https://github.com/opentargets/genetics_etl_python/commit/43937391fca84fbebcfb90f8c1012a37a5209947))

* feat: handling schemas on read parquet

Incorrect schemas crash read_parquet

- wrapper around read.parquet as part of `ETLSession`
- PoC schema added to project ([`8f02fa3`](https://github.com/opentargets/genetics_etl_python/commit/8f02fa355f187c4c7b2870a482db4a900344c01d))

* feat: adding effect harmonization to gwas ingestion ([`9e6bf85`](https://github.com/opentargets/genetics_etl_python/commit/9e6bf8562e6175e4659374014e09abdc890f8923))

* feat: adding functions to effect harmonizataion ([`dd0d2e9`](https://github.com/opentargets/genetics_etl_python/commit/dd0d2e94ff2e002bea4da8626129169207c6242f))

* feat: gwas association deduplication by minor allele frequency ([`3c1138e`](https://github.com/opentargets/genetics_etl_python/commit/3c1138ed4bb526d3763083ba37444f6b052f1a5a))

* feat: debugging in vscode

- Launch.json contains debugging capabilities
- Debug overwrites the config.path to use the ./configs instead of ./
- 2 different debug configurations use 2 hydra configurations:
    - dataproc setup for developing within a dataproc cluster
    - local setup to develop in local machine (with local files) ([`46db297`](https://github.com/opentargets/genetics_etl_python/commit/46db2975e780ae9e1d41f1a8645729626fe9fae0))

* feat: ignoring __pycache__ ([`7a201ff`](https://github.com/opentargets/genetics_etl_python/commit/7a201ffe430bac65f29c8034b94ae82d5219480a))

* feat: coverage added to dev environment ([`a9d2815`](https://github.com/opentargets/genetics_etl_python/commit/a9d28153361019e32e66bf738814edb85e4c9fad))

* feat: increased functionality of the gwas catalog ingestion + tests ([`ca43ecd`](https://github.com/opentargets/genetics_etl_python/commit/ca43ecde2b138a98166d628fac02391c672b103d))

* feat: mapping GWAS Catalog variants to GnomAD3 ([`4fa9876`](https://github.com/opentargets/genetics_etl_python/commit/4fa9876d59948e8f726007c7b9bd5cf5f8a0b702))

* feat: adding GWAS Catalog association ingest script ([`dc3c70d`](https://github.com/opentargets/genetics_etl_python/commit/dc3c70d7bfcc1da53568830f1ff5725dc1a52086))

* feat: black version bumped ([`e18dd65`](https://github.com/opentargets/genetics_etl_python/commit/e18dd656c5e88975d1d6ebadbe452e9cb4ae63c6))

* feat: adding variant annotation ([`ad57c6a`](https://github.com/opentargets/genetics_etl_python/commit/ad57c6a39cae4afb34b770f8c04226a1e39c9984))

* feat: updated dependencies ([`1e00d39`](https://github.com/opentargets/genetics_etl_python/commit/1e00d39577d86b4d2bc134c3eff5b9e9809ec539))

* feat: Adding parsers for intervals dataset. ([`57ab36d`](https://github.com/opentargets/genetics_etl_python/commit/57ab36de4868711166adbbcbdb4d4667e0910989))

* feat(pre-commit): commitlint hook added ([`e570742`](https://github.com/opentargets/genetics_etl_python/commit/e570742ab587059a9b2e7f0f51d64b73a699834c))

### Fix

* fix: unnecessary option (#351) ([`9508225`](https://github.com/opentargets/genetics_etl_python/commit/95082250cf5ed8c90f12dfac0721e45a6be0dfcd))

* fix: several issues (#349) ([`5e484f6`](https://github.com/opentargets/genetics_etl_python/commit/5e484f675ea48e525654abb81287056f4e9326e3))

* fix: github token (#348) ([`ebbc83a`](https://github.com/opentargets/genetics_etl_python/commit/ebbc83a9165e8a3872fe01daf3ca49fbb13a6596))

* fix: release actions fixes (#344)

* feat: metadata on toml

* feat: several fixes

* refactor: linting

* refactor: externalise python version

* fix: single quotes

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci

---------

Co-authored-by: pre-commit-ci[bot] &lt;66853113+pre-commit-ci[bot]@users.noreply.github.com&gt; ([`91a9ad1`](https://github.com/opentargets/genetics_etl_python/commit/91a9ad193b41303ac136d447ee9bd5404e07f2a5))

* fix(clump): read input files recursively (#292) ([`a6cc21d`](https://github.com/opentargets/genetics_etl_python/commit/a6cc21dc21a3ff30f0d1c106025e47e0063cea79))

* fix: correct and test study splitter when subStudyDescription is the same (#289) ([`793a58b`](https://github.com/opentargets/genetics_etl_python/commit/793a58bce9a2c4b029556a0142f9420954a0ba54))

* fix: making standard_error column optional (#286)

* fix: making standard-error column optional ([`1fca378`](https://github.com/opentargets/genetics_etl_python/commit/1fca3784f221e0befa22770bc9e718b774cf949d))

* fix: proper parsing of gwas catalog study accession from filename (#282)

* fix: proper parsing of gwas catalog study accession from filename

* fix: interestingly, I had to add an extra line to pass doctest on VSCode

* feat: bumping mypy version

---------

Co-authored-by: David Ochoa &lt;ochoa@ebi.ac.uk&gt; ([`4330fe1`](https://github.com/opentargets/genetics_etl_python/commit/4330fe141bcc8221ecb46f6be7f9f3276b95f078))

* fix: local SSD initialisation ([`23e3dc6`](https://github.com/opentargets/genetics_etl_python/commit/23e3dc6d365a03b1b3d1bdca30673f524debe393))

* fix: multiply standard error by zscore in `calculate_confidence_interval` ([`2eee6ca`](https://github.com/opentargets/genetics_etl_python/commit/2eee6ca6a660034c9f3c674db1354a2f6ee85117))

* fix(l2g_gold_standard): fix logic in `remove_false_negatives` ([`0a1ffa0`](https://github.com/opentargets/genetics_etl_python/commit/0a1ffa0b75f9034ed02a6a333969d037e3e171c0))

* fix: wrong lines removed ([`aa717fe`](https://github.com/opentargets/genetics_etl_python/commit/aa717fe503cd9d9f625e39cee3ad1af8a551027c))

* fix: change definition of negative l2g evidence ([`44766d2`](https://github.com/opentargets/genetics_etl_python/commit/44766d22febfb2ae2d2ed4224106c90128ffc5b0))

* fix: studies sample filename ([`e58a959`](https://github.com/opentargets/genetics_etl_python/commit/e58a959c482febab8da1a77f4164c669223656b3))

* fix: typo in position field name ([`4462110`](https://github.com/opentargets/genetics_etl_python/commit/4462110af8001212598b602a0c5d686885d2eb28))

* fix: manually specify schema for eQTL Catalogue summary stats ([`2c0c1d0`](https://github.com/opentargets/genetics_etl_python/commit/2c0c1d0c12634bba55711e7471e09a577bb0b8b0))

* fix: cast nSamples as long ([`83af9eb`](https://github.com/opentargets/genetics_etl_python/commit/83af9eb81998b2b2a5fca2950e1f1f0c155efdb9))

* fix: populating publicationDate ([`571c4dc`](https://github.com/opentargets/genetics_etl_python/commit/571c4dc530cf8a52c645934a70f4c68db196f2f9))

* fix: include header when reading the study index ([`5b3e85f`](https://github.com/opentargets/genetics_etl_python/commit/5b3e85f429e92c7b6917496bbe402fa148086459))

* fix: do not initialise session in the main class ([`eca511c`](https://github.com/opentargets/genetics_etl_python/commit/eca511cd09c68946e06f6eed2e4458ea0a527275))

* fix: name of EqtlCatalogueStep class ([`8243af8`](https://github.com/opentargets/genetics_etl_python/commit/8243af88b69d5d40be564f91c5e34f69b5291130))

* fix: eqtl_catalogue path in docs ([`b1cb048`](https://github.com/opentargets/genetics_etl_python/commit/b1cb0481986db068b349445ef1e139278464d2ab))

* fix: update class names ([`ee031da`](https://github.com/opentargets/genetics_etl_python/commit/ee031da091aaebe49babd851ca7d39d5c270e42a))

* fix: do not partition by chromosome for QTL studies ([`6a900dd`](https://github.com/opentargets/genetics_etl_python/commit/6a900dd17932f52dcb285b2bb6f3845ce01895af))

* fix: coalesce variantid to assign a studylocusid

closes #3151 ([`c7fbac3`](https://github.com/opentargets/genetics_etl_python/commit/c7fbac310892dcd435e021350a23912e0fbfe2dd))

* fix: persist raw gwascat associations to return consistent results ([`e7c0cc8`](https://github.com/opentargets/genetics_etl_python/commit/e7c0cc89377179bc09dfc74561da1e38a8d06ff2))

* fix: gnomad paths are not necessary after #233 ([`1d793b9`](https://github.com/opentargets/genetics_etl_python/commit/1d793b98ce51e4cd73ecf47f7513ac4355faf97c))

* fix: revert testing changes ([`d651a03`](https://github.com/opentargets/genetics_etl_python/commit/d651a033e1ebd5df6ed6031279c54bc2ae64b42d))

* fix(gwas_catalog): clump associations, remove hail and style fixes ([`c79b6fd`](https://github.com/opentargets/genetics_etl_python/commit/c79b6fd9f3fba8643962c6094c728f038090537a))

* fix: extract config in root when we install deps on cluster ([`1ccb99d`](https://github.com/opentargets/genetics_etl_python/commit/1ccb99d433abc9725eec6a51c1aa2a7648d8f2a2))

* fix: wrong python file uri in airflow ([`8731291`](https://github.com/opentargets/genetics_etl_python/commit/8731291e347aada9ab2d08aa3789fb48f3bd7cba))

* fix: commiting example block matrix ([`1143d5b`](https://github.com/opentargets/genetics_etl_python/commit/1143d5b22f026bac19b9c5f76f35a2c74c013846))

* fix(session): add `mode_overwrite` default to configs ([`97af2f0`](https://github.com/opentargets/genetics_etl_python/commit/97af2f028404d7618012ef19ef3cdb1159273265))

* fix: revert 69cb5c13fba97fc0c3a73f51a306f74c099e5d42 ([`b39fbe4`](https://github.com/opentargets/genetics_etl_python/commit/b39fbe42c7cd7b64c4577cc8d974a44ef16b4e4f))

* fix: change chain location to gcp

bug reported here https://github.com/hail-is/hail/issues/13993 ([`601ee58`](https://github.com/opentargets/genetics_etl_python/commit/601ee5888f87bc73acea60ae994c7f38fa9ca649))

* fix: no longer installed dependencies (too long) ([`a271f09`](https://github.com/opentargets/genetics_etl_python/commit/a271f09aa5ef4bd609c32fc16398ee66e532bbac))

* fix: removing unnecessary print statement ([`20f23b3`](https://github.com/opentargets/genetics_etl_python/commit/20f23b3263233fe6910186bb2b0b0079c58c6a7e))

* fix: revert changes in spark fixture ([`6f21ec3`](https://github.com/opentargets/genetics_etl_python/commit/6f21ec31275b9dff3eb56d3df3a92737b67fc532))

* fix: un-comment rows in test_session.py ([`5910718`](https://github.com/opentargets/genetics_etl_python/commit/59107185afdac4545dd9219a59972133c8766c33))

* fix: fix typo ([`d3eb6bd`](https://github.com/opentargets/genetics_etl_python/commit/d3eb6bd4ffdb238499ffac241ee1278b818b281e))

* fix: phasing out session initialization ([`78c9011`](https://github.com/opentargets/genetics_etl_python/commit/78c9011bb7455e3c24637b0b2e5c7efe3e72c108))

* fix: remove default session from steps

related to https://github.com/opentargets/issues/issues/3145 ([`57054d0`](https://github.com/opentargets/genetics_etl_python/commit/57054d0802b53a2b53829dbf4b1743c891cb6d99))

* fix: python_module_path is not built inside `submit_pyspark_job` ([`3faec29`](https://github.com/opentargets/genetics_etl_python/commit/3faec29df216003265520c02bee4594c1cbd3cca))

* fix(session): hail config was not set unless extended_spark_conf was provided ([`995392b`](https://github.com/opentargets/genetics_etl_python/commit/995392b38d2d98943b8bc54c4d41305d312bf641))

* fix(variant_annotation): remove default session in the step ([`d021e6c`](https://github.com/opentargets/genetics_etl_python/commit/d021e6cdb73b85efb73998ed223b3cad211efa76))

* fix: default_factory now takes lambda ([`1cad02b`](https://github.com/opentargets/genetics_etl_python/commit/1cad02bbb963a4a0fe026edd09314d4fa74965b2))

* fix: reverting field from dataclass ([`4bacf1b`](https://github.com/opentargets/genetics_etl_python/commit/4bacf1ba9653d6249b4f1cbdc10f2215a7992e26))

* fix(config): specify start_hail for all configs to avoid recursive interpolation ([`ebe0905`](https://github.com/opentargets/genetics_etl_python/commit/ebe09059a99fa9fe19be35c419fd15f3081b5b31))

* fix(session): config to pick up `start_hail` flag ([`69cb5c1`](https://github.com/opentargets/genetics_etl_python/commit/69cb5c13fba97fc0c3a73f51a306f74c099e5d42))

* fix: different syntax for relative import ([`104f020`](https://github.com/opentargets/genetics_etl_python/commit/104f020634206c718fe6d2437ca09c8a2c1545e5))

* fix: allow relative imports ([`c16194e`](https://github.com/opentargets/genetics_etl_python/commit/c16194e88ac51c37023bd624969dcccb10e752ae))

* fix: rename method in FinnGenSummaryStats ([`ac14a72`](https://github.com/opentargets/genetics_etl_python/commit/ac14a72a7eeb1b29e791d9e0545465a314e932fe))

* fix(airflow): job args are list of strings

This affects the way that args are provided for the pyspark job specification.
DataprocSubmitJobOperator takes args as a list of strings.
The previous function accepted a list of strings or a dictionary as inputs. When provided a dictionary, it&#39;d parse it into a list of strings.
In https://github.com/opentargets/genetics_etl_python/commit/930155765e7e7c474f877faad0018b329d977e79 I introduced a bug that incorrectly assumed that args were provided as a dict. If a list was provided, formatted_args would remain an empty array.
This is actually the cause for the bug reported here https://github.com/opentargets/genetics_etl_python/pull/201#pullrequestreview-1706949353 ([`a1944f8`](https://github.com/opentargets/genetics_etl_python/commit/a1944f8309af8f7cf693319bc20d795f4f53140f))

* fix(pre-commit): ignore d107 rule in pydocstyle due to clash with pydoclint ([`4195d99`](https://github.com/opentargets/genetics_etl_python/commit/4195d99b9936548a361862459a6a6a2ef0c5ca39))

* fix(config): store main config as `default_config` to deduplicate from yaml ([`39160f9`](https://github.com/opentargets/genetics_etl_python/commit/39160f9228af93f0054f869a49945d280c7fe96e))

* fix(docs): add python handler to exclude private methods ([`ed9bbe1`](https://github.com/opentargets/genetics_etl_python/commit/ed9bbe18bc7bdd762edab79863282eea0dbb661c))

* fix(l2g): set default session ([`fb9a72e`](https://github.com/opentargets/genetics_etl_python/commit/fb9a72e507d9e1754f78486974c9255b6a23546f))

* fix(dag): uncomment lines ([`e90b3cd`](https://github.com/opentargets/genetics_etl_python/commit/e90b3cd5b8af8f77be6d8139a1e8fd9a1805d974))

* fix(l2g): add `wandb` to main dependencies ([`04311c5`](https://github.com/opentargets/genetics_etl_python/commit/04311c5b6e4c90187c24f3088d452a2773f4fefc))

* fix: step bugfixes ([`f62a524`](https://github.com/opentargets/genetics_etl_python/commit/f62a524cf1817d55643209f82b59ce0c6526ae10))

* fix: step bugfixes ([`a1c1dc8`](https://github.com/opentargets/genetics_etl_python/commit/a1c1dc8301631040eef1d3c1b129d8a86fd1dbd2))

* fix(l2g): bugfix predict step ([`10b9f27`](https://github.com/opentargets/genetics_etl_python/commit/10b9f2788dda3d446b297ac17a763c99e5ed7340))

* fix(l2g): drop `sources` before conversion to feature matrix ([`24b6ef0`](https://github.com/opentargets/genetics_etl_python/commit/24b6ef0c2d7764a81f2b81b8d1525823263dff43))

* fix(l2g): bugfixes to run step ([`5a6c208`](https://github.com/opentargets/genetics_etl_python/commit/5a6c208c0bf278cf28b475a26e73398135df3092))

* fix(features): remove deprecated feature generation methods ([`bce1971`](https://github.com/opentargets/genetics_etl_python/commit/bce19714478189e7563e93d76a2b16d68e4d0a49))

* fix: pass cluster_name to install_dependencies ([`7f48f9c`](https://github.com/opentargets/genetics_etl_python/commit/7f48f9c083b47214a77332e56cde2a39fa06866e))

* fix: install_dependencies syntax ([`977b048`](https://github.com/opentargets/genetics_etl_python/commit/977b048476ee1d632f5c70ed23eed61cb7b5308c))

* fix: common DAG initialisation parameters ([`83a14a4`](https://github.com/opentargets/genetics_etl_python/commit/83a14a4bf7bf2d82d7b9a580e17033b6d58cc647))

* fix: set DAG owner to fix assertion error ([`aadb8ea`](https://github.com/opentargets/genetics_etl_python/commit/aadb8ea9d0bfd0ea6ff99d0274a37adce68c18cf))

* fix: variable name ([`25cb269`](https://github.com/opentargets/genetics_etl_python/commit/25cb2696a3a035b3011598eb0893be018137bd73))

* fix: delete deprecated files ([`dc5aaec`](https://github.com/opentargets/genetics_etl_python/commit/dc5aaecf6dac5849f5b4f6524d354d0019443938))

* fix: minor fix ([`dbf2ffd`](https://github.com/opentargets/genetics_etl_python/commit/dbf2ffde6b23bfaa30c4680a1965ccfd3c9aaf76))

* fix(intervals): import within Intervals.from_source to avoid circular dependencies ([`1a551ff`](https://github.com/opentargets/genetics_etl_python/commit/1a551ff261aa188e6469f41a60e4487d85ecfd85))

* fix: updated OT doc ([`2783ced`](https://github.com/opentargets/genetics_etl_python/commit/2783ceda51f67e5e55804b13d21b4ab0c0bd2470))

* fix: added to class docstring ([`a966e12`](https://github.com/opentargets/genetics_etl_python/commit/a966e12931c279e8646ccf9fe9f0651d5abd9184))

* fix(docs): update link to roadmap and contributing sections ([`a856efc`](https://github.com/opentargets/genetics_etl_python/commit/a856efcbce21e2873195bcf69d715917f31a76d2))

* fix(docs): correct schemas path for gene and study index ([`41672f5`](https://github.com/opentargets/genetics_etl_python/commit/41672f5c3e448e36cc35b3c7aee87151f1182e67))

* fix(v2g): indicate schema ([`6a3a412`](https://github.com/opentargets/genetics_etl_python/commit/6a3a41295a124a4aa27b1ae58fc17b76922d3c09))

* fix(v2g): indicate schema ([`b2f0d0f`](https://github.com/opentargets/genetics_etl_python/commit/b2f0d0fb99c0cd4dcd5b75e94e85317c514c0866))

* fix(intervals): remove `position` from v2g data ([`138804a`](https://github.com/opentargets/genetics_etl_python/commit/138804a3a7585035bcfa70a565c4a806f705a673))

* fix(liftover): use gcfs to download chain file when provided gcs paths ([`fc95559`](https://github.com/opentargets/genetics_etl_python/commit/fc95559a92607fdc83052d12ed3fe41a32c5c660))

* fix(v2g): read intervals passing spark session ([`ea9e3c4`](https://github.com/opentargets/genetics_etl_python/commit/ea9e3c414048c3006c48ac6080f5bc789712f334))

* fix(variant_annotation): `get_distance_to_tss` returns distance instead of position ([`a37413f`](https://github.com/opentargets/genetics_etl_python/commit/a37413fd5326bdbd9ec31643a136a4188cd6f552))

* fix(gene_index): add `obsoleteSymbols` and simplify schema ([`ec23766`](https://github.com/opentargets/genetics_etl_python/commit/ec237667faf6d307b8d95693ff85f5c3ffdd999e))

* fix(gene_index): add `approvedName` `approvedSymbol` `biotype` to `as_gene_index` ([`68a280b`](https://github.com/opentargets/genetics_etl_python/commit/68a280b484d465601238ab5dc4381e80e990b4c8))

* fix(gene_index): typo in gene_index output dataset ([`aacb00a`](https://github.com/opentargets/genetics_etl_python/commit/aacb00aee0b1e5982cf9d3195471992f470e6eb7))

* fix(gene_index): add `obsoleteSymbols` and simplify schema ([`8599435`](https://github.com/opentargets/genetics_etl_python/commit/8599435f06059458e805f6a5ef0837eb3aa87d21))

* fix(gene_index): add `approvedName` `approvedSymbol` `biotype` to `as_gene_index` ([`6167f58`](https://github.com/opentargets/genetics_etl_python/commit/6167f584f44838608dc6be0978797283f3db4bca))

* fix(gene_index): typo in gene_index output dataset ([`693e3e3`](https://github.com/opentargets/genetics_etl_python/commit/693e3e3ac95487ed4428653b51448c012bc2c674))

* fix(v2g): change `id` to `variantId` ([`da751c6`](https://github.com/opentargets/genetics_etl_python/commit/da751c6e4b8f4eea899356c77cbb0fa135a594a6))

* fix(v2g): convert biotypes to python list ([`7343e96`](https://github.com/opentargets/genetics_etl_python/commit/7343e96916451168ed8c08ca9a96885c20d2fb2d))

* fix: correct thurman typo ([`7878af8`](https://github.com/opentargets/genetics_etl_python/commit/7878af896b08b8767d670464a242172164528695))

* fix(session): revert recursive lookup and use kwargs ([`7e58f47`](https://github.com/opentargets/genetics_etl_python/commit/7e58f47ea67e7ef2fa126f7b39df8b5f081e60d1))

* fix: relative links instead of absolute ([`9164b3f`](https://github.com/opentargets/genetics_etl_python/commit/9164b3f92c750e0c139c3b6850031c56c46d957e))

* fix: trying to specify python version ([`50f8fe7`](https://github.com/opentargets/genetics_etl_python/commit/50f8fe726005cb8d222885484210673de88a83ae))

* fix: trying to force python version ([`af8c7cb`](https://github.com/opentargets/genetics_etl_python/commit/af8c7cbc962b6fe780f6e60585070d9477e2c156))

* fix(test_cross_validate): test learning rate in params ([`e2c92ce`](https://github.com/opentargets/genetics_etl_python/commit/e2c92ce5d9703026f7663247ea2b2de36f202ba4))

* fix(test_train): fix problem of task failing during a barrier stage ([`fb32d20`](https://github.com/opentargets/genetics_etl_python/commit/fb32d2033c00184b2dc9f0a64e8b6b10153e7632))

* fix: conflict with thurman ([`ceb8b1a`](https://github.com/opentargets/genetics_etl_python/commit/ceb8b1aa7c7a8e5c32a5aec5f9754e2dfe0ab110))

* fix: incorrect filename ([`84e8de5`](https://github.com/opentargets/genetics_etl_python/commit/84e8de5565fb3a24556511f8d68ab0079a87b764))

* fix(l2g): set correct output column in `evaluate` ([`c5a9a76`](https://github.com/opentargets/genetics_etl_python/commit/c5a9a76090fbd99e7aee04c7a4efa146f0432b41))

* fix: remove unnecessary lead variant id from feature matrix ([`408a788`](https://github.com/opentargets/genetics_etl_python/commit/408a788ac779781af1976164a772c1d8605c0c48))

* fix: comment out coloc factory ([`427b67b`](https://github.com/opentargets/genetics_etl_python/commit/427b67bfc8db4bf8fcb4e789f2baddcee49b3955))

* fix: do not upload preprocess as part of this PR ([`399b056`](https://github.com/opentargets/genetics_etl_python/commit/399b0563774de8c0d9edd2ac52a81062bd92c84b))

* fix: remove redefining get_schema ([`ac5d812`](https://github.com/opentargets/genetics_etl_python/commit/ac5d81216dd2f818f9797bcbc690c2db65ad2063))

* fix: move Preprocess/SummaryStats changes to the old location ([`8826a13`](https://github.com/opentargets/genetics_etl_python/commit/8826a13594e3c1006bc53300af41602fb2b103ac))

* fix: remove Preprocess/StudyIndex changes ([`0e2f98a`](https://github.com/opentargets/genetics_etl_python/commit/0e2f98a80aba851cb2c15466774f85aec3a247dc))

* fix: update schema name after upstream changes ([`c1e6c8c`](https://github.com/opentargets/genetics_etl_python/commit/c1e6c8c242e866400b1818e52808fe265ce201a0))

* fix: l2g tests fix, more samples added so that splitting doesnt fail ([`491d010`](https://github.com/opentargets/genetics_etl_python/commit/491d010cfcb06ab4ad6e78b282d3a688601a15ca))

* fix: remove typing issues ([`e77956c`](https://github.com/opentargets/genetics_etl_python/commit/e77956c900bd1b1addeee3337e2f2673ba2cdbfe))

* fix: tests pass ([`a37c39d`](https://github.com/opentargets/genetics_etl_python/commit/a37c39da6a2b8089b237e413db47ac104e9dfa26))

* fix: regenerate lock file ([`be8b389`](https://github.com/opentargets/genetics_etl_python/commit/be8b3899d6f8a9e268f53d475fb33f25560e04fe))

* fix: fix study_locus_overlap test - all passing ([`7452ae2`](https://github.com/opentargets/genetics_etl_python/commit/7452ae27d16320bf84a418749688c1ef2b55d80c))

* fix: l2g step uses common etl session ([`46f326a`](https://github.com/opentargets/genetics_etl_python/commit/46f326a4ebbd952c25e5b46e38302f4284b65661))

* fix: remove `data` from .gitignore ([`e217a23`](https://github.com/opentargets/genetics_etl_python/commit/e217a232362fa46541cc53a79730990b0ee99f6b))

* fix: move `schemas` and `data` to root folder ([`2dabff3`](https://github.com/opentargets/genetics_etl_python/commit/2dabff33d890f1836e23c3fe3dbb4a08ef23d523))

* fix: bring back main config for package bundling ([`3cb053b`](https://github.com/opentargets/genetics_etl_python/commit/3cb053b969e7c834c18c10595874cab943c419bb))

* fix: labelling tags with null posterior as false instead of null ([`f6bd1d8`](https://github.com/opentargets/genetics_etl_python/commit/f6bd1d88ac2b0f8a4399b0abb95f0466fa31c11d))

* fix: formatting ([`08d0c14`](https://github.com/opentargets/genetics_etl_python/commit/08d0c1412603b6f5d07c0fc61aaa8d2da511b0ff))

* fix: removing some unneccesary files ([`2cbcdd6`](https://github.com/opentargets/genetics_etl_python/commit/2cbcdd6fe6c5ea77772e6973318f22bc8507889a))

* fix: schema issues due to when condition ([`22974ef`](https://github.com/opentargets/genetics_etl_python/commit/22974efd0d2354f55543f98ec6a6e671eb089bf0))

* fix: removing gsutil cp dags/* from build ([`e7a57e7`](https://github.com/opentargets/genetics_etl_python/commit/e7a57e7e3a2a3c5da500df34e1898ea15875929c))

* fix: update makefile ([`d4d23d7`](https://github.com/opentargets/genetics_etl_python/commit/d4d23d769fc7f95cfe26dcc540ed1489a269411b))

* fix: some studies just don&#39;t have population data ([`f57e815`](https://github.com/opentargets/genetics_etl_python/commit/f57e81531b308b1457d6aab26c80b71c5d1361a1))

* fix: empty array of unknown type cannot be created and saved. Fixed ([`5fa241f`](https://github.com/opentargets/genetics_etl_python/commit/5fa241fef66a660b01a9f6301d300572ec92c92d))

* fix: fixing column selection ([`5b4d52d`](https://github.com/opentargets/genetics_etl_python/commit/5b4d52d2718d17b59008971e970d4260146decaa))

* fix: cleaning clumping ([`5d0a299`](https://github.com/opentargets/genetics_etl_python/commit/5d0a299648bb573b7e2c7e8af2ec40efc93d5693))

* fix: the studylocus returned by window based clumping has qc column ([`a2b9f33`](https://github.com/opentargets/genetics_etl_python/commit/a2b9f33881b276c08bbd5db7c6f1e5c22d5ea2ef))

* fix: typo in length ([`d9bec82`](https://github.com/opentargets/genetics_etl_python/commit/d9bec82ca2f6ca696c65f95b1a60eed116a5afe9))

* fix: dealing with schemas pre and post PICS ([`2c77fed`](https://github.com/opentargets/genetics_etl_python/commit/2c77fedc1aeb0424bd60054b9c7c87d6b175d772))

* fix: unnecessary test ([`0c8c495`](https://github.com/opentargets/genetics_etl_python/commit/0c8c49512871df400b8f826ab19862372cfe9a75))

* fix: useless test ([`0c68bad`](https://github.com/opentargets/genetics_etl_python/commit/0c68bad3fef232591ade9aaa685023150a74260c))

* fix(ids): correcting version numbers for pull request ([`b062a1a`](https://github.com/opentargets/genetics_etl_python/commit/b062a1a5b87cd59baaabe79db5c69e6d7eae1b0c))

* fix: adapt pics test to newer data model ([`54eff0b`](https://github.com/opentargets/genetics_etl_python/commit/54eff0b0633be98cff802aaae134e750d278f2e3))

* fix: un-commented lines to actually fetch data ([`00688d1`](https://github.com/opentargets/genetics_etl_python/commit/00688d10789c30546603dd4b1247d075f57bb1ec))

* fix: updating GWAS Catalog input files ([`d4c15c3`](https://github.com/opentargets/genetics_etl_python/commit/d4c15c31d1bfc41f4a4553b721bc218be168d55c))

* fix: doctest, test ([`aca9d7c`](https://github.com/opentargets/genetics_etl_python/commit/aca9d7c9f785851ca004e2b08b35688d4a1a2e0c))

* fix: doctest ([`2cfd1cb`](https://github.com/opentargets/genetics_etl_python/commit/2cfd1cbec67354e29ef52960c24f89ebbf3f3fb0))

* fix: test fail ([`05f45eb`](https://github.com/opentargets/genetics_etl_python/commit/05f45eb04e816be386d5d89217caa3b4ff065525))

* fix: update coloc logic to new fields ([`6dee154`](https://github.com/opentargets/genetics_etl_python/commit/6dee154e263226a9df4acd753b11eda4c3020156))

* fix: update coloc logic to new fields ([`4b8e92d`](https://github.com/opentargets/genetics_etl_python/commit/4b8e92d0b5b4f5ebfc52cbf962c2dd9f78d40f97))

* fix: column pattern can include numbers ([`6cc4f9e`](https://github.com/opentargets/genetics_etl_python/commit/6cc4f9ea33ee6b3cba56899a80c88308828d5ddc))

* fix: use vanilla spark instead of our custom session ([`f800f92`](https://github.com/opentargets/genetics_etl_python/commit/f800f92b950af801fb754036f308d200c17db311))

* fix: adding sample data for testing ([`37e8a6e`](https://github.com/opentargets/genetics_etl_python/commit/37e8a6e30e6eeef99ffc1c0a6ff22b6ce8be575b))

* fix: merge main + fixing tests ([`2fffe91`](https://github.com/opentargets/genetics_etl_python/commit/2fffe916d45fdf9ab23a67e1f318d8d6dd3b9cff))

* fix: minor udpates ([`42e4879`](https://github.com/opentargets/genetics_etl_python/commit/42e4879dfcd06a689932139a9ff54c3d2e177594))

* fix: renamed columns ([`1c32205`](https://github.com/opentargets/genetics_etl_python/commit/1c3220553b5b0b43d95e7e453b27a4d7652e58df))

* fix: properly use new hooks from mkdocs ([`3ebcb21`](https://github.com/opentargets/genetics_etl_python/commit/3ebcb213e7db51400e0ba6762d87dfa0ea84aaf8))

* fix: merging with main ([`43b7cf3`](https://github.com/opentargets/genetics_etl_python/commit/43b7cf39d0772eee28d90946c1e3d4e9c51b8ed4))

* fix: adjusting dataset initialization ([`d9f5261`](https://github.com/opentargets/genetics_etl_python/commit/d9f526118563bf3052f216729629ce7ef9da30c1))

* fix: updated action to install docs in testing environment ([`9fb2dbb`](https://github.com/opentargets/genetics_etl_python/commit/9fb2dbbb360dfee4f37976bf1eb63e7d7c19274a))

* fix: restore missing file ([`6692f67`](https://github.com/opentargets/genetics_etl_python/commit/6692f677f0800fc65f6f14bf84888699a4927020))

* fix: session setup preventing to run tests in different environments ([`e29a28f`](https://github.com/opentargets/genetics_etl_python/commit/e29a28f21251af2fd9df29abf782f3961790c04b))

* fix: failing airflow tests ([`256fb30`](https://github.com/opentargets/genetics_etl_python/commit/256fb30355b9b099b86372a6a41219fb0c7a42cf))

* fix: missing import ([`8d00ae8`](https://github.com/opentargets/genetics_etl_python/commit/8d00ae89a0c6c6b5f4c3169bf333a31edef412a9))

* fix: incorrect extension name ([`d74f195`](https://github.com/opentargets/genetics_etl_python/commit/d74f195ca2062ba4f8806fd2ae68226a16bb0408))

* fix: correct extensions name ([`8c1954c`](https://github.com/opentargets/genetics_etl_python/commit/8c1954c35623036f1fb180bb87a1799174879490))

* fix: typo in check rule ([`973495a`](https://github.com/opentargets/genetics_etl_python/commit/973495a7933742dc808ad23096026a7c85fb2b81))

* fix: apply ruff suggestions ([`d70464b`](https://github.com/opentargets/genetics_etl_python/commit/d70464b186e0c4f10f3f3b443d8cd5658873d585))

* fix: mess caused when incorrectly pushing my branch ([`ad23462`](https://github.com/opentargets/genetics_etl_python/commit/ad23462c3483db79bd643aa654c7cfcf70116ffc))

* fix: step minor bugfixes ([`49f4ac9`](https://github.com/opentargets/genetics_etl_python/commit/49f4ac9c9c6b25e2b1c40d0862ae0f42df0092f1))

* fix: adapt tests to latest changes in the overlaps schema ([`ab1e8e7`](https://github.com/opentargets/genetics_etl_python/commit/ab1e8e701632343257485f933d7113280d654f9f))

* fix: unnecessary CHANGELOG ([`5d0727c`](https://github.com/opentargets/genetics_etl_python/commit/5d0727cbf50c29dcd2802ef9228a18aa854ffebb))

* fix: failed dependency ([`5d1cc82`](https://github.com/opentargets/genetics_etl_python/commit/5d1cc823f805aadd9bb67236fa7edf7f3d20a39a))

* fix(studylocus): assign hashed studylocusid after study splitting ([`b8c5c87`](https://github.com/opentargets/genetics_etl_python/commit/b8c5c876f61f181bb6ba5813b44a56c0eb578800))

* fix(studylocusgwascatalog): use ([`2a99226`](https://github.com/opentargets/genetics_etl_python/commit/2a99226f1fae171ab561004a81c0f2cc324cd2f2))

* fix: remove repartiitoning and adjust aggregation to a single col - working ([`57109e4`](https://github.com/opentargets/genetics_etl_python/commit/57109e4399fbb5e382e62c32ab0467a3793d1b26))

* fix: lower memory threshold ([`3469434`](https://github.com/opentargets/genetics_etl_python/commit/34694342dd40630a589620b8bea84fbc968e22ac))

* fix: revert addig chromosome to join to resolve variants ([`f98ce28`](https://github.com/opentargets/genetics_etl_python/commit/f98ce286f4de7b5f4c5cf0d018cf3c9836002557))

* fix: adapt schema definition in tests ([`4593525`](https://github.com/opentargets/genetics_etl_python/commit/4593525b3de1966155d54e45048aca583a6c64c4))

* fix: adapt schema definition in tests ([`8dffb77`](https://github.com/opentargets/genetics_etl_python/commit/8dffb77c11348bcd09e5f24aa4d676be737cdb0e))

* fix: correct formula in `_pics_standard_deviation` ([`1c84cc3`](https://github.com/opentargets/genetics_etl_python/commit/1c84cc3c31fca5613fbd9e6c919bab612836cef8))

* fix: typo in pics std calculation as defined by PMID:25363779 ([`8e9c5ab`](https://github.com/opentargets/genetics_etl_python/commit/8e9c5abd6604294e11cb442a35016c7d7e723ab4))

* fix: adapt `test_annotate_credible_sets` mock data ([`952791b`](https://github.com/opentargets/genetics_etl_python/commit/952791b3c39329de692525dca0d75448dfdd29af))

* fix: fix `_qc_unresolved_ld` ([`7947aa1`](https://github.com/opentargets/genetics_etl_python/commit/7947aa1528c5285a22bd1f5ab7f6f68c948d2417))

* fix: correct ld_index_path ([`bb6fec8`](https://github.com/opentargets/genetics_etl_python/commit/bb6fec813e6a049cc0da94ef42b23cac7ab9ee1f))

* fix: minor bugs to success ([`4cfce6c`](https://github.com/opentargets/genetics_etl_python/commit/4cfce6cf086c22ba9ff02515d41b7674ae368eb9))

* fix: set ld_index fields to non nullable ([`cd69b9b`](https://github.com/opentargets/genetics_etl_python/commit/cd69b9b37ef39a34ea5b1be3c5fa654990f16d3d))

* fix: minor bugs for a successful run ([`86a0021`](https://github.com/opentargets/genetics_etl_python/commit/86a0021cfe064556413ac95622ef06152a6830a0))

* fix(_transpose_ld_matrix): bugfix and test ([`91e14c8`](https://github.com/opentargets/genetics_etl_python/commit/91e14c839108ea687cec03449d14b66b47998135))

* fix(ldindex): minor bug fixes to run the new step ([`cd856eb`](https://github.com/opentargets/genetics_etl_python/commit/cd856ebf80ac963af0cebb65c64f3289a84e8350))

* fix(ldindex): minor bug fixes to run the new step ([`aeb4f58`](https://github.com/opentargets/genetics_etl_python/commit/aeb4f5804932c0a915f7a84f3f5ec2cfada90fd3))

* fix(_variant_coordinates_in_ldindex): order variant_coordinates df just by idx and not chromosome ([`fd4cd3d`](https://github.com/opentargets/genetics_etl_python/commit/fd4cd3d5bab29fb3ca3738466267e7c3ae629486))

* fix: start index at 0 in _variant_coordinates_in_ldindex ([`1e2ec4a`](https://github.com/opentargets/genetics_etl_python/commit/1e2ec4a5248676445d2139a8e04dea61be1feb1d))

* fix: updating tests ([`5b91bbd`](https://github.com/opentargets/genetics_etl_python/commit/5b91bbdbc624947d88e5450432442b785cd428c9))

* fix: update expectation in `test__finemap` ([`350a91a`](https://github.com/opentargets/genetics_etl_python/commit/350a91a8dc9774159f84d8cc2085f650337eef10))

* fix: comply with studyLocus requirements ([`17a7014`](https://github.com/opentargets/genetics_etl_python/commit/17a70147d148b17579ba43af5992321e9bb5da00))

* fix: avoid empty credible set (#3016) ([`684bb58`](https://github.com/opentargets/genetics_etl_python/commit/684bb58b2436cce3ef9fee8f7fa6744a0108291a))

* fix: pvalueMantissa as float in summary stats ([`9c4154a`](https://github.com/opentargets/genetics_etl_python/commit/9c4154a3d66c1678c6b08b034eb71701355d2dc5))

* fix: spark does not like functions in group bys ([`877671d`](https://github.com/opentargets/genetics_etl_python/commit/877671df4e2d1bf19928dd830947b7deeeee3b68))

* fix: stacking avatars in docs ([`917847e`](https://github.com/opentargets/genetics_etl_python/commit/917847ef658fef11f8bfb47ff6e619199ac77765))

* fix: typo in docstring ([`2266f58`](https://github.com/opentargets/genetics_etl_python/commit/2266f5863b46faf05c8232f1c9bf95e333bc45d2))

* fix: implement suggestions from code review ([`bb3ec04`](https://github.com/opentargets/genetics_etl_python/commit/bb3ec04fa49763faf32d6daafb539b86a1ae527e))

* fix: revert accidentally removed link ([`696b549`](https://github.com/opentargets/genetics_etl_python/commit/696b5495a1add55aeb972e5c713a3845d69066b6))

* fix: set pvalue text to null if no mapping is available ([`94e1a82`](https://github.com/opentargets/genetics_etl_python/commit/94e1a82396e9c90cc27525420eea7fdce3975a7f))

* fix: revert removing subStudyDescription from the schema ([`8a1da7e`](https://github.com/opentargets/genetics_etl_python/commit/8a1da7e7862ea3cf10d4cbb2a9f985627e2cc4d9))

* fix: use studyId instead of projectId as primary key ([`10a2e6d`](https://github.com/opentargets/genetics_etl_python/commit/10a2e6dde398a10d62cba78c017ce712133fca1a))

* fix: change substudy separator to &#34;/&#34; to fix efo parsing ([`8a44135`](https://github.com/opentargets/genetics_etl_python/commit/8a44135c0c1f9ddca769ae1b7e14dc5606d28e56))

* fix: resolve trait accounting for null p value texts ([`34d44aa`](https://github.com/opentargets/genetics_etl_python/commit/34d44aabe020a2de742df69c4f6d1605fd7cb8d0))

* fix: updated StudyIndexUKBiobank spark efficiency ([`3693739`](https://github.com/opentargets/genetics_etl_python/commit/36937392fc3438b162c4c7c9e2b2448fe849069f))

* fix: update gwascat projectid to gcst ([`d6ffc5b`](https://github.com/opentargets/genetics_etl_python/commit/d6ffc5b047d364ae8750e12e6b6d9dc8dcb1186c))

* fix: updated StudyIndexUKBiobank ([`20a5c8f`](https://github.com/opentargets/genetics_etl_python/commit/20a5c8fdc43af27b482885792c751417b28932e8))

* fix: incorrect parsing of study description ([`2b6a2ae`](https://github.com/opentargets/genetics_etl_python/commit/2b6a2ae0ecb31eb31802e282359da76c3837a78a))

* fix: right image in the workflow ([`1127fcb`](https://github.com/opentargets/genetics_etl_python/commit/1127fcb2d931e324fc2ededab39f350b6a122203))

* fix: update conftest.py read.csv

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`85b6f9d`](https://github.com/opentargets/genetics_etl_python/commit/85b6f9da3992fe386f34c18e11203e7cbe443e0d))

* fix: update gcp.yaml spacing

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`8527be8`](https://github.com/opentargets/genetics_etl_python/commit/8527be8611e247c25c92143465dc56adff6f17c0))

* fix: update gcp.yaml spacing

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`b7479f3`](https://github.com/opentargets/genetics_etl_python/commit/b7479f39535ae872404fe4a17fadb66b714b8d3d))

* fix: correct sumstats url ([`8b2ac60`](https://github.com/opentargets/genetics_etl_python/commit/8b2ac60d5ebe8523816d7c1d019b12254253b5b3))

* fix: decrease partitions by just coalescing ([`eebca6d`](https://github.com/opentargets/genetics_etl_python/commit/eebca6d2b34745d9723eae5561548fea734c6522))

* fix: variable interpolation in workflow_template.py ([`befab8f`](https://github.com/opentargets/genetics_etl_python/commit/befab8fdfd9a70970c789dbc9cca10d0364e9ef9))

* fix: reorganise workflow_template.py to make pytest and argparse work together ([`7f0b0c8`](https://github.com/opentargets/genetics_etl_python/commit/7f0b0c8a7c4cfbd81df82374cbc2586b281984f0))

* fix: typos ([`09e3521`](https://github.com/opentargets/genetics_etl_python/commit/09e352174793ed191dc6b3f007347a20ff8d1c1c))

* fix: revert accidental DAG changes ([`a90b7a2`](https://github.com/opentargets/genetics_etl_python/commit/a90b7a2398692dd5345d9d18418d5b1374d057f5))

* fix: reduce num partitions to fix job failure ([`e92dff2`](https://github.com/opentargets/genetics_etl_python/commit/e92dff27a0fc46494f373d7d11eec00dc59d5885))

* fix: handle empty credibleSet in `annotate_credible_sets` + test ([`d5c3a56`](https://github.com/opentargets/genetics_etl_python/commit/d5c3a56c16ceeaa468dd69f15a228652c3fc1ead))

* fix: control none and add test for _finemap + refactor ([`4a7be65`](https://github.com/opentargets/genetics_etl_python/commit/4a7be65c475a8e753019a1da3bb60617d1628867))

* fix: do not control credibleSet size for applying finemap udf ([`3ae745d`](https://github.com/opentargets/genetics_etl_python/commit/3ae745d994b09be5ed001e82accb1676f5b8707e))

* fix: remove use of aliases to ensure study index is updated ([`3f1a9ef`](https://github.com/opentargets/genetics_etl_python/commit/3f1a9efa4664bbe8a821200a8eb64c6e8e13a554))

* fix: adapt `finemap` to handle nulls + test ([`61c4f95`](https://github.com/opentargets/genetics_etl_python/commit/61c4f957a67b554062acdc628623de12df17e758))

* fix: remove null elements in credibleSet ([`d1452d7`](https://github.com/opentargets/genetics_etl_python/commit/d1452d76833ae83cc6c802b170969de89e2a4a7b))

* fix: handle null credibleSet as empty arrays ([`c15ac92`](https://github.com/opentargets/genetics_etl_python/commit/c15ac92c5dc0a4d341fabf81b981afb2ec215fa9))

* fix: define finemap udf schema from studylocus ([`4c52b99`](https://github.com/opentargets/genetics_etl_python/commit/4c52b996e20a1fb184858aeb8025b71e6c1f4e00))

* fix: add missing columns to `_variant_coordinates_in_ldindex` + new test ([`a39e74c`](https://github.com/opentargets/genetics_etl_python/commit/a39e74caa7682ffd51a2bae43b4b918a0463b153))

* fix: update indices column names in `get_ld_annotated_assocs_for_population` ([`7ed8b10`](https://github.com/opentargets/genetics_etl_python/commit/7ed8b10832098568d82f13bd779ab027e227f13d))

* fix: populate projectId and studyType constant value columns ([`21f5f81`](https://github.com/opentargets/genetics_etl_python/commit/21f5f81295e42fdc15ec6d150f17681f6a10c26d))

* fix: configuration target for FinnGenStepConfig ([`8bc8238`](https://github.com/opentargets/genetics_etl_python/commit/8bc8238eaf1966275715f29a1efe4b50113e3a86))

* fix: update lit column init to agree with `validate_df_schema` behaviour ([`ae77fa0`](https://github.com/opentargets/genetics_etl_python/commit/ae77fa04b9a8a83d992a1d29909133563da20bc9))

* fix: rewrite JSON ingestion with RDD and remove unnecessary dependencies ([`c0add46`](https://github.com/opentargets/genetics_etl_python/commit/c0add4681a7ba3c29d53d44c8e80b114211651dc))

* fix: configuration variable name ([`ae88fd1`](https://github.com/opentargets/genetics_etl_python/commit/ae88fd14a839e20b2ebae032130670c67ac2d68f))

* fix: several errors found during debugging ([`3a1182c`](https://github.com/opentargets/genetics_etl_python/commit/3a1182cf4d2ebe0c5cc81e81db4db5a10adcfee1))

* fix: resolve problems with Pyenv/Poetry installation ([`56aedaf`](https://github.com/opentargets/genetics_etl_python/commit/56aedafe684ad9b85aa13b83928ebaeca45b5a2f))

* fix: activate Poetry shell when setting up the dev environment ([`237c696`](https://github.com/opentargets/genetics_etl_python/commit/237c696e61f21709b92b635f7d32b706ce83b98f))

* fix: update `ld_index_template` according to new location ([`3c33b3f`](https://github.com/opentargets/genetics_etl_python/commit/3c33b3f75acafbf9dea8bd0f239d4c88007379c0))

* fix: revert changes in `.vscode` ([`1224643`](https://github.com/opentargets/genetics_etl_python/commit/1224643779e3f1fab535d67b8c0c68609129d289))

* fix: calculate r2 before applying weighting function ([`fed6ea9`](https://github.com/opentargets/genetics_etl_python/commit/fed6ea9b440b12f4d1b7de826381280a458b0a22))

* fix: restore .gitignore ([`69ec6cb`](https://github.com/opentargets/genetics_etl_python/commit/69ec6cb6bd1c89a23de55f12ddc06fe2a26f299e))

* fix: flatten_schema result test to pyspark 3.1 schema convention ([`c59ad3c`](https://github.com/opentargets/genetics_etl_python/commit/c59ad3c37651a420663a80bc25098f78be4e3db1))

* fix: revert to life pre-gpt ([`d4e5bf3`](https://github.com/opentargets/genetics_etl_python/commit/d4e5bf3e2b5d8baa7b0dc2d7eb6fb7c07fa11c5c))

* fix: altering order of columns to pass validation (provisional hack) ([`3535912`](https://github.com/opentargets/genetics_etl_python/commit/35359122f8015ce917cd1bd414f6ff929476fe8d))

* fix: cascading effects in other schemas ([`852dcac`](https://github.com/opentargets/genetics_etl_python/commit/852dcac32fe7fe76e087bd2c9ceac2660eb08267))

* fix: remove nested filter inside the window in ([`58c5d52`](https://github.com/opentargets/genetics_etl_python/commit/58c5d5250faff8dc2c6616595b3214e55d483116))

* fix: handle duplicated chrom in v2g generation ([`26d9aea`](https://github.com/opentargets/genetics_etl_python/commit/26d9aeae06a9be474b731d284839d64c5ed59c90))

* fix: `_annotate_discovery_sample_sizes` drop default fields before join ([`fe93cf5`](https://github.com/opentargets/genetics_etl_python/commit/fe93cf57091feba709fd4dbf54c2675952cad43a))

* fix: `_annotate_ancestries` drop default fields before join ([`dce0e76`](https://github.com/opentargets/genetics_etl_python/commit/dce0e76986a828dafbeea0c42ab88962149801a2))

* fix: `_annotate_sumstats_info` drop duplicated columns before join and coalesce ([`dec2cf4`](https://github.com/opentargets/genetics_etl_python/commit/dec2cf4dfd1c34f90946d92f7168cd1fe8d03b8f))

* fix: order ld index by idx and unpersist data ([`5a8e03a`](https://github.com/opentargets/genetics_etl_python/commit/5a8e03a7030c88b6b7c39f2ed20123598ff76dea))

* fix: correct attribute names for ld indices ([`b1d56c4`](https://github.com/opentargets/genetics_etl_python/commit/b1d56c48cac25ca11c587527c32df5ddbeddc004))

* fix: join `_variant_coordinates_in_ldindex` on `variantId`

variant_df does not have split coordinates ([`829ef6b`](https://github.com/opentargets/genetics_etl_python/commit/829ef6b982cc7c83c9fa95928140d23987cd67d3))

* fix: move rsId and concordance check outside the filter function

windows cannot be applied inside where clauses ([`c57919d`](https://github.com/opentargets/genetics_etl_python/commit/c57919d2d28e0cea8f4eb67f2b2fb2e30d273945))

* fix: l2g tests fix, more samples added so that splitting doesnt fail ([`ffe825f`](https://github.com/opentargets/genetics_etl_python/commit/ffe825ffd952bf69422191ab08c925287945b137))

* fix: update configure and gitignore ([`6cc9af1`](https://github.com/opentargets/genetics_etl_python/commit/6cc9af18a6061083abbd403b35034f56233dd226))

* fix: type issue ([`0450163`](https://github.com/opentargets/genetics_etl_python/commit/0450163d6ea9f67d524820b6df44f1aa8e4d7cd2))

* fix: typing issue ([`64e5591`](https://github.com/opentargets/genetics_etl_python/commit/64e5591e790750610b59e4adf0789a3011ddd6c0))

* fix: typing issue fixed ([`ddec460`](https://github.com/opentargets/genetics_etl_python/commit/ddec4609be27a39421da7744db59c3e9baa28a56))

* fix: typing and tests ([`7373b13`](https://github.com/opentargets/genetics_etl_python/commit/7373b13f55036b17e2998b3cf01304f0d94997d9))

* fix: right populations in config ([`fd7ceb8`](https://github.com/opentargets/genetics_etl_python/commit/fd7ceb8ba57e38d1a460026e531c651610b162b9))

* fix: gnomad LD populations updated ([`47f5873`](https://github.com/opentargets/genetics_etl_python/commit/47f58733a93ce659dd8c2421e95414e11d73a0d5))

* fix: blocking issues in variant_annotation ([`29a1f0e`](https://github.com/opentargets/genetics_etl_python/commit/29a1f0e5bf92cca7912d1792e6d2f167b8026a27))

* fix: blocking issues in ld_index ([`3fc30ed`](https://github.com/opentargets/genetics_etl_python/commit/3fc30edb9887d2b80fba9513f18faa90f6f4c9ce))

* fix: typing issue ([`f012ffd`](https://github.com/opentargets/genetics_etl_python/commit/f012ffd044c3530044bc06c76809b96f28e29b02))

* fix: tests working again ([`6bec918`](https://github.com/opentargets/genetics_etl_python/commit/6bec91882c7c8ea5ff4e8a69b0a7c4fb987f1e35))

* fix: missing f.lit ([`8e4cfcb`](https://github.com/opentargets/genetics_etl_python/commit/8e4cfcbc8840c1da1deaeafb2257bf4d80c6c058))

* fix: extensive fixes accross all codebase ([`2b48906`](https://github.com/opentargets/genetics_etl_python/commit/2b48906b9d3b89a911198e493a9f58a4d98b9ff7))

* fix: missing chain added ([`2d8815f`](https://github.com/opentargets/genetics_etl_python/commit/2d8815fdf8b56e4b68c8f5d4993d9d329b39d806))

* fix: typo in config ([`8ebde89`](https://github.com/opentargets/genetics_etl_python/commit/8ebde894563aed66c69e29dbdc1baa1d0074d4e8))

* fix: remove typing issues ([`b062a8c`](https://github.com/opentargets/genetics_etl_python/commit/b062a8c6e2eb6d1e8309a3b4423a5e532ac20bce))

* fix: fixing sumstats column nullabitlity ([`62c20a9`](https://github.com/opentargets/genetics_etl_python/commit/62c20a960777acf467700e92be45765dee9f7910))

* fix: clearing up validation ([`2a727ee`](https://github.com/opentargets/genetics_etl_python/commit/2a727ee7c238ec4cfb63560e4feecef4f6398bd7))

* fix: steps as dataclasses ([`19dac76`](https://github.com/opentargets/genetics_etl_python/commit/19dac76a8cc1735b37aae0df3d751839236a4164))

* fix: incorrect config file ([`0f3495c`](https://github.com/opentargets/genetics_etl_python/commit/0f3495c00fd5dbc954f4a0ac4f4d02dbf83c7fc6))

* fix: fixing test allowing nullable column ([`45cd4fd`](https://github.com/opentargets/genetics_etl_python/commit/45cd4fdfc794ba72a01b73fb06ce7a8cd214f80e))

* fix: rename json directory to prevent conflicts ([`c941aa4`](https://github.com/opentargets/genetics_etl_python/commit/c941aa4cdc6ac7c14f8aff5bcff94ba3db7b3db5))

* fix: tests pass ([`8abe6c8`](https://github.com/opentargets/genetics_etl_python/commit/8abe6c81213857078733b9b7a20e6f91677154bd))

* fix: regenerate lock file ([`a4f3cb4`](https://github.com/opentargets/genetics_etl_python/commit/a4f3cb45c5a8e1c9938b2a4ab973bd6951879650))

* fix: fix study_locus_overlap test - all passing ([`d255cb7`](https://github.com/opentargets/genetics_etl_python/commit/d255cb7c82acb9fafccea87d01a5be3577b8788e))

* fix: fixing doctest ([`2faefb4`](https://github.com/opentargets/genetics_etl_python/commit/2faefb469ab6f215ee57988bb98c884e252f9d36))

* fix: merging with do_hydra ([`87415c0`](https://github.com/opentargets/genetics_etl_python/commit/87415c0514210a2a7be61dd9b5434d5912c1f5e3))

* fix: l2g step uses common etl session ([`94a8808`](https://github.com/opentargets/genetics_etl_python/commit/94a88082033aff1ba12fdcd874ff8290b708cadd))

* fix: addressing various comments ([`a866f3b`](https://github.com/opentargets/genetics_etl_python/commit/a866f3b1eed798364238da017d628aae97511289))

* fix: missing dependency for testing ([`735609c`](https://github.com/opentargets/genetics_etl_python/commit/735609c233b76198a4579e25f72650b4297414ea))

* fix: missed dbldatagen dependency ([`8cd76dd`](https://github.com/opentargets/genetics_etl_python/commit/8cd76dd3ec7e7f323d4bf4d1e0b64bed4115cf48))

* fix: pyupgrade to stop messing with typecheck ([`239f3db`](https://github.com/opentargets/genetics_etl_python/commit/239f3dbe75812e6c5547788ebfc60800a8c21dca))

* fix: operative pytest again ([`54255c3`](https://github.com/opentargets/genetics_etl_python/commit/54255c3f6b6dd24a5a03b084703f0e3849b4eb0b))

* fix: wrong import ([`bb76019`](https://github.com/opentargets/genetics_etl_python/commit/bb76019f768bff677d2d4bbe6517f9b143a690f3))

* fix: df not considered ([`608e371`](https://github.com/opentargets/genetics_etl_python/commit/608e371c5609eb70407121fec7a9b03af5efd746))

* fix: remove `data` from .gitignore ([`a8aa934`](https://github.com/opentargets/genetics_etl_python/commit/a8aa934f249e7fba140745eab72b39fe418ac4ee))

* fix: move `schemas` and `data` to root folder ([`be96609`](https://github.com/opentargets/genetics_etl_python/commit/be96609a649fb6e41376b00fa9e33a839b39a9c5))

* fix: consolidate path changes ([`39d5276`](https://github.com/opentargets/genetics_etl_python/commit/39d5276eb9a48e9d3b6d0204e72d5523e96c1979))

* fix: bring back main config for package bundling ([`cba7bcd`](https://github.com/opentargets/genetics_etl_python/commit/cba7bcd42c6f468edcca8440e7599bc493b643b8))

* fix: using normalise column ([`9c1a294`](https://github.com/opentargets/genetics_etl_python/commit/9c1a294cc23375387edfdb038934ca495c83ddfb))

* fix: bug with config field ([`55a4f61`](https://github.com/opentargets/genetics_etl_python/commit/55a4f612c83bf718c55a540571a93222a48098ec))

* fix: exclude unneccessary content ([`0bb26a7`](https://github.com/opentargets/genetics_etl_python/commit/0bb26a7df554d4ee2ce4518a6dc14f333a92051a))

* fix: typo in the schema ([`4e112cc`](https://github.com/opentargets/genetics_etl_python/commit/4e112cc39e5e81fa39426c40ca9cd1df34daaf42))

* fix: import fix ([`0ddd969`](https://github.com/opentargets/genetics_etl_python/commit/0ddd969094b8f14cec85f5a42eb5e53f3039c39b))

* fix: finalizing claping logic ([`f27fd07`](https://github.com/opentargets/genetics_etl_python/commit/f27fd07fde21cedc1dfeee150c688e9e66915c44))

* fix: type fixes ([`edd67eb`](https://github.com/opentargets/genetics_etl_python/commit/edd67eb6aeddb3ce3918cfcbd8d08c5c41c4b215))

* fix: issues about clumping ([`b981941`](https://github.com/opentargets/genetics_etl_python/commit/b9819414ea15f20bae224b797cb8dc0a79117120))

* fix: merge conflicts ([`24174c0`](https://github.com/opentargets/genetics_etl_python/commit/24174c02fbd4d9cdd51d904280baf91380553873))

* fix: merge conflicts resolved ([`f34a016`](https://github.com/opentargets/genetics_etl_python/commit/f34a01667ce096d2ceb915b03d446d63fee36e2e))

* fix: incorrect super statement ([`f038772`](https://github.com/opentargets/genetics_etl_python/commit/f03877235f8214a7fa30afb5be74ebd2bf814a3c))

* fix: wrong paths ([`8753644`](https://github.com/opentargets/genetics_etl_python/commit/875364417b3949852f2650a6ac84dde035b52722))

* fix: fix normalisation of the inverse distance values ([`e1ba2b2`](https://github.com/opentargets/genetics_etl_python/commit/e1ba2b227ca98738232049d0e45076f74e84e459))

* fix: minor bugfixes ([`91586a4`](https://github.com/opentargets/genetics_etl_python/commit/91586a4a4d24d3027f6998f77a197807cc366209))

* fix: schema fix ([`874f1b3`](https://github.com/opentargets/genetics_etl_python/commit/874f1b338c2ce4fdc43820ebd09304548b9b46a8))

* fix: dropping pyupgrade due to problems with hydra compatibility in Union type ([`373a5ec`](https://github.com/opentargets/genetics_etl_python/commit/373a5eccdf69c900f447d93d55dafd65517d0af0))

* fix: comment out filter on chrom 22 ([`52a4ee2`](https://github.com/opentargets/genetics_etl_python/commit/52a4ee26aca968d34f5c12e615c040d4ac6afe94))

* fix: update tests ([`086452c`](https://github.com/opentargets/genetics_etl_python/commit/086452c5704b22607ce433407ee40a77461d73fc))

* fix: remove repartition step in intevals df ([`5ced127`](https://github.com/opentargets/genetics_etl_python/commit/5ced127813f77f0918d0a2a601872cc8f8259d45))

* fix: fix tests data definition ([`a382ffa`](https://github.com/opentargets/genetics_etl_python/commit/a382ffaf3876feae152e39cfd010f4062cc61d87))

* fix: revert unwanted change ([`d726ba2`](https://github.com/opentargets/genetics_etl_python/commit/d726ba260f9ecfd6b3a1a9517e299cb044917c55))

* fix: correct for nullable fields in `studies.json ([`e0cb874`](https://github.com/opentargets/genetics_etl_python/commit/e0cb874b4cd1cb2812703dd46072052d1d635276))

* fix: write gwas studies in overwrite mode ([`8b0031f`](https://github.com/opentargets/genetics_etl_python/commit/8b0031f29d35600c5c6eec947d578e754abb75d2))

* fix: correct column names of `mock_rsid_filter` ([`8c41c76`](https://github.com/opentargets/genetics_etl_python/commit/8c41c76aa007580d932f1e0e3e8c4fed796bea5b))

* fix: typo in mock data in mock_allele_columns ([`bca99a1`](https://github.com/opentargets/genetics_etl_python/commit/bca99a1889104a18dabb57d7b8b2c70b0ec1f470))

* fix: camel case associations dataset ([`ae26935`](https://github.com/opentargets/genetics_etl_python/commit/ae269359974be49c35e850817a9711ff380c74ed))

* fix: structure dev dependencies ([`782f27a`](https://github.com/opentargets/genetics_etl_python/commit/782f27a97ce68879f8d78fb19363cebf8e4dd737))

* fix: fetch-depth=0 ([`6fde7ee`](https://github.com/opentargets/genetics_etl_python/commit/6fde7eea6cd53d050db5ccbbe82d87ae53f90e44))

* fix: depth=0 as suggested by warning ([`43aa631`](https://github.com/opentargets/genetics_etl_python/commit/43aa631460733e5cf03712fc13155b87c7fb1a6b))

* fix: module needs to be installed for mkdocstring ([`377597a`](https://github.com/opentargets/genetics_etl_python/commit/377597a5a02628435c7de2633bad54f9208bb816))

* fix: docs gh action refactor ([`913a6f6`](https://github.com/opentargets/genetics_etl_python/commit/913a6f6b402028732fd7dd27e35cffa015140f29))

* fix: variable name changed ([`7773fe4`](https://github.com/opentargets/genetics_etl_python/commit/7773fe4063423f029bfb562fb0dcee7715c4cbc6))

* fix(pr comments): abstracting some of the hardcoded values, changing z-score calculation ([`589bbd9`](https://github.com/opentargets/genetics_etl_python/commit/589bbd9882c52aafeb04d61022b3721da5cbc4f6))

* fix: clash between vscode and pretty-format-json ([`4c5af7e`](https://github.com/opentargets/genetics_etl_python/commit/4c5af7e3c2875f8c741ee9b604b2140187dc03f3))

* fix: clash between black and pretty format json

Defaulting to 2 indent spaces (like black) ([`6c001c7`](https://github.com/opentargets/genetics_etl_python/commit/6c001c7c58f00019270da2bd080233033ad168c1))

* fix(config): parametrizing target index ([`18752d4`](https://github.com/opentargets/genetics_etl_python/commit/18752d41526d903f92160ef00ed8eaa7569c0653))

* fix(schema): camel casing column names ([`7dc16ea`](https://github.com/opentargets/genetics_etl_python/commit/7dc16ea38fa5e4a88cc44bbfa3fa6379c2442824))

* fix: updates for new gene index ([`b368f08`](https://github.com/opentargets/genetics_etl_python/commit/b368f0856498b8045274100ca7f4b60eb8aeb9a4))

* fix: flake FS002 issue resolved ([`305415e`](https://github.com/opentargets/genetics_etl_python/commit/305415e0b6e57ce921caf20d436cf7d1714af4e6))

* fix: copy target index into the release playground bucket ([`2058318`](https://github.com/opentargets/genetics_etl_python/commit/2058318762fe4433677c283173eef311cc8d5205))

* fix: fixed imports in intervals ([`e061f22`](https://github.com/opentargets/genetics_etl_python/commit/e061f22e47d5ba3cc9701a03283ba3b1bd3326bc))

* fix: might be required ([`51ea89e`](https://github.com/opentargets/genetics_etl_python/commit/51ea89e6a318f7f87863b333e4e27d434cb64f7e))

* fix: pytest gh actions now adds coverage ([`9f4b478`](https://github.com/opentargets/genetics_etl_python/commit/9f4b47801305f7d92e3f7dde8311b8d63fab85c9))

* fix: more fixes to gh action ([`debcf80`](https://github.com/opentargets/genetics_etl_python/commit/debcf8077bcd5d57ea76f750472bf8c04188e1e6))

* fix: pytest gh action ([`be491cc`](https://github.com/opentargets/genetics_etl_python/commit/be491ccfed125b10d1f1424cf72d8557f63d4b85))

* fix: gwas ingestion adapted to new structure

- makefile works for step
- avoids to pass config object inside the module ([`c791da3`](https://github.com/opentargets/genetics_etl_python/commit/c791da3eab27a6c841e26817f433c1bc09990e03))

* fix: testing strategy moved to github actions ([`5fab4f7`](https://github.com/opentargets/genetics_etl_python/commit/5fab4f719cdd4d37ec5ba24b48097b00bc71e9a9))

* fix: pytest hook fixed ([`9b59a7c`](https://github.com/opentargets/genetics_etl_python/commit/9b59a7cdf2ea2fbbf009086f3b31fb62757e72e3))

* fix: python version and dependencies

- Documentation more clear about how to handle python version
- Windows requirements ignored on build ([`3886693`](https://github.com/opentargets/genetics_etl_python/commit/38866933963a4e8674fd047bbd895a4e122d922f))

* fix: adding hail to dependency + moving gcsfs to production with the same version as on pyspark ([`b6d7d07`](https://github.com/opentargets/genetics_etl_python/commit/b6d7d07e4586ae372e3fcfd0d42c4bb8fcbec741))

* fix: fixing python and spark version

Adjust to dataproc versions ([`b5be97d`](https://github.com/opentargets/genetics_etl_python/commit/b5be97dbca460033bd0dcf35e7103bbbde48f403))

* fix: Missing interval parser added. ([`34914c4`](https://github.com/opentargets/genetics_etl_python/commit/34914c4d818b99da77c59af6133e88049dd66d6a))

* fix: commitlint hook was not working ([`b6f13b5`](https://github.com/opentargets/genetics_etl_python/commit/b6f13b50e35e9d2829300f2079f5d98ef76cf623))

### Performance

* perf: persist data after first aggregation ([`d60aaa8`](https://github.com/opentargets/genetics_etl_python/commit/d60aaa80e3bb6b06b7a15f266be978cc89b1811a))

* perf: persist data after first aggregation ([`186c186`](https://github.com/opentargets/genetics_etl_python/commit/186c186c2b57aa71d0d9d934c7ae88d622d6c7f6))

* perf: follow hail spark context guidelines ([`cf22242`](https://github.com/opentargets/genetics_etl_python/commit/cf22242759fdff7bbdda24141d15a104b0bce15d))

* perf: set number of shuffle partitions to 10_000 ([`0f160e1`](https://github.com/opentargets/genetics_etl_python/commit/0f160e17e3111126f72eac089183d86ca37cfa7d))

* perf: transpose ld_matrix just before aggregating ([`17188d9`](https://github.com/opentargets/genetics_etl_python/commit/17188d9fa1658a7ff961ab649a945a2ff41cce64))

* perf: repartition each ldindex before unioning

to decrease data shuffling ([`5f73695`](https://github.com/opentargets/genetics_etl_python/commit/5f73695c6657f2882b188defa44c975cb82f5839))

* perf: add chromosome to join to resolve variants ([`23b16fb`](https://github.com/opentargets/genetics_etl_python/commit/23b16fb58c758c6640180567d53f782a2400f2e1))

* perf: do not enforce nullability status ([`7b07147`](https://github.com/opentargets/genetics_etl_python/commit/7b071475f6d564b362eaa3b585ed9696c36cb69d))

* perf: replace withColumn calls with a select ([`5d82d00`](https://github.com/opentargets/genetics_etl_python/commit/5d82d008379859fcf19e8862ce67d0d1e8b33e6b))

### Refactor

* refactor: stop inheriting datasets in parsers (#313)

* refactor: stop inheriting datasets in parsers

* fix: typing issue

* refactor: include datasets in datasources

* test: fix incorrect import

* test: doctest function calls fixed

* test: doctest function calls fixed in studyindex

---------

Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt; ([`ee73572`](https://github.com/opentargets/genetics_etl_python/commit/ee735724798a4e90601ecfc60788e4735f96be17))

* refactor: removing odds ratio, and confidence intervals from the schema ([`0db58a7`](https://github.com/opentargets/genetics_etl_python/commit/0db58a7d27939dc355383e81a8cdc892b01dfad0))

* refactor(gold_standard): move logic to refine gold standards to `L2GGoldStandard` ([`1518156`](https://github.com/opentargets/genetics_etl_python/commit/1518156f4f44aa10d544e45ca9bb321ffc485d97))

* refactor: turn `OpenTargetsL2GGoldStandard` into class methods ([`65be470`](https://github.com/opentargets/genetics_etl_python/commit/65be4708f8b1dbfd24b6e5c6b34cd4098932b9f1))

* refactor: move hardcoded values to constants ([`f7eba79`](https://github.com/opentargets/genetics_etl_python/commit/f7eba79b6d466a962997f6e8306c469c7483cea8))

* refactor: modularise logic for gold standards ([`e17df5b`](https://github.com/opentargets/genetics_etl_python/commit/e17df5b84d705faec1c021a8691cf80ede7c4fe4))

* refactor: move gene ID joining into the study index class ([`c95adf1`](https://github.com/opentargets/genetics_etl_python/commit/c95adf147e6ac889a8e375bbbfb729057638a623))

* refactor: reorganise study index ingestion for readability ([`be5efad`](https://github.com/opentargets/genetics_etl_python/commit/be5efad1826249aabbb0cda0830a61bc9103549f))

* refactor: update eQTL study index import ([`6ba36b4`](https://github.com/opentargets/genetics_etl_python/commit/6ba36b42870e60d71e0e1a4a98729369d139ea5e))

* refactor: `get_ld_variants` to return a df or none ([`b8ad278`](https://github.com/opentargets/genetics_etl_python/commit/b8ad278dba04a59e3a175dbcd3b3319909825036))

* refactor: final solution using defaults ([`13a287f`](https://github.com/opentargets/genetics_etl_python/commit/13a287f811a53cd8dd059e5815121c5b47944681))

* refactor: use defaults override for configuration ([`4b7b65e`](https://github.com/opentargets/genetics_etl_python/commit/4b7b65e3efafd8294b2b217bac40708d056404b3))

* refactor: remove config store ([`e161183`](https://github.com/opentargets/genetics_etl_python/commit/e161183aa10768507369935050ef3f357d7399f0))

* refactor: remove legacy start_hail attributes ([`7727ced`](https://github.com/opentargets/genetics_etl_python/commit/7727ced4cd4c0546bf810f062728c7d1fc94a237))

* refactor: explicitly set spark_uri for local session ([`189876f`](https://github.com/opentargets/genetics_etl_python/commit/189876f5c481b3703f61b346a9004c77353d4392))

* refactor: remove repeatedly setting default: dataproc ([`a06c4e7`](https://github.com/opentargets/genetics_etl_python/commit/a06c4e77eec974c09a8f4d618f73138dc24ed02f))

* refactor: move session default to config.yaml ([`201e2f2`](https://github.com/opentargets/genetics_etl_python/commit/201e2f21060277f116339b58b366ce28a88cfe46))

* refactor: gnomad ld test refactored ([`794c7cf`](https://github.com/opentargets/genetics_etl_python/commit/794c7cfc19161e18c6de6f386527092bcf6fa7c8))

* refactor: changes specific to FinnGen ([`3687216`](https://github.com/opentargets/genetics_etl_python/commit/36872167b6695a4d0f06042b00ce4652cf482c24))

* refactor: simplify reading YAML config ([`6cc7601`](https://github.com/opentargets/genetics_etl_python/commit/6cc7601bbabf260a725ddd16b402da8e841a5c60))

* refactor: use generate_dag() to simplify ETL DAG ([`ecdab7f`](https://github.com/opentargets/genetics_etl_python/commit/ecdab7fb24b4175d0162d5abdef1fb6c299d3ffa))

* refactor: use generate_dag() to simplify Preprocess DAG ([`6dc37de`](https://github.com/opentargets/genetics_etl_python/commit/6dc37de8cea67b670bf31ef43daa789e7c7f390f))

* refactor: implement generate_dag() to further simplify layout ([`163074f`](https://github.com/opentargets/genetics_etl_python/commit/163074fe1e98b4804be224ed6b4c6810624c0dd6))

* refactor: use submit_step() to simplify the main DAG ([`2fb56ca`](https://github.com/opentargets/genetics_etl_python/commit/2fb56caab92d66bf54dcd39556eee78526c18d3b))

* refactor: add submit_step() as a common Airflow routine ([`67da568`](https://github.com/opentargets/genetics_etl_python/commit/67da56817d46244395d313c54e6cb8959ee35261))

* refactor: make step docstrings uniform ([`86730be`](https://github.com/opentargets/genetics_etl_python/commit/86730be5e833581a640c1f71f5a297275e8e3c72))

* refactor: change run() to __post_init__() ([`da44e0f`](https://github.com/opentargets/genetics_etl_python/commit/da44e0f94f257e231430be70a9952289958d746a))

* refactor: remove step.run() from cli.py ([`e847b4b`](https://github.com/opentargets/genetics_etl_python/commit/e847b4b3ce0be7f79d08060819426486784d9d0c))

* refactor: rename all steps without &#34;my_&#34; ([`57f5703`](https://github.com/opentargets/genetics_etl_python/commit/57f5703b1e53c48d8b395b7f27657e191ca58e92))

* refactor: update docs and remaining classes ([`38926dc`](https://github.com/opentargets/genetics_etl_python/commit/38926dc573f80dce1f1d5785c3c8940206749767))

* refactor: finalise VariantIndexStep ([`f7a8bdf`](https://github.com/opentargets/genetics_etl_python/commit/f7a8bdf92ac7840cc6b62ede9c169a9fc4502b62))

* refactor: finalise VariantAnnotationStep ([`00bef84`](https://github.com/opentargets/genetics_etl_python/commit/00bef84b5912369da4b5ab2a49a947bfdb3b32b5))

* refactor: finalise V2GStep ([`32678d4`](https://github.com/opentargets/genetics_etl_python/commit/32678d41eaf8b74cd69604d5ef53a6cb1ac3f10f))

* refactor: finalise UKBiobankStep ([`df5fb3a`](https://github.com/opentargets/genetics_etl_python/commit/df5fb3a79314690de58b9588ad3779cc606a5c6e))

* refactor: finalise StudyLocusOverlapStep ([`ca4d86a`](https://github.com/opentargets/genetics_etl_python/commit/ca4d86aaa535247144f650dfecdab12f890a94b6))

* refactor: finalise LDIndexStepConfig ([`8643baa`](https://github.com/opentargets/genetics_etl_python/commit/8643baa2bf591173d46d9aea3a8bf131fa18007b))

* refactor: finalise GWASCatalogSumstatsPreprocessStep ([`b2b1d0a`](https://github.com/opentargets/genetics_etl_python/commit/b2b1d0ae00286aeb4ef2f693616b4cd243c3d428))

* refactor: finalise GWASCatalogStep ([`cdb743e`](https://github.com/opentargets/genetics_etl_python/commit/cdb743e4277cdec9fc9e6030fc3d782c992b69a7))

* refactor: finalise GeneIndexStep ([`d6f4b70`](https://github.com/opentargets/genetics_etl_python/commit/d6f4b706a052ace721491d721bd250e8dbd8f54d))

* refactor: finalise FinnGenStep ([`be00656`](https://github.com/opentargets/genetics_etl_python/commit/be00656ce30dd5deedfe84fce788245b7ff5698a))

* refactor: finalise ColocalisationStep ([`986686d`](https://github.com/opentargets/genetics_etl_python/commit/986686d1530d54b0b7b33ba0b05b607cf7a2177a))

* refactor: remove config store entries for steps ([`5d4cd68`](https://github.com/opentargets/genetics_etl_python/commit/5d4cd6815da15aa3deb40b629112450b64a24991))

* refactor: add _target_ to YAML configuration files ([`116f897`](https://github.com/opentargets/genetics_etl_python/commit/116f897f6a294a17d7fc30c1efdef539856cf433))

* refactor: remove defaults + comments from YAML config files ([`24fb6cc`](https://github.com/opentargets/genetics_etl_python/commit/24fb6cc45d69e41d88b9edb0387e7970c322fd2a))

* refactor: remove *StepConfig from documentation ([`7ea0f15`](https://github.com/opentargets/genetics_etl_python/commit/7ea0f15db7067a640d932b8d0ed023b16bbd179c))

* refactor(feature): move `L2GFeature` inside dataset ([`597b13b`](https://github.com/opentargets/genetics_etl_python/commit/597b13b55d0cc3121aca0d65dcee5f5019ecf15c))

* refactor: add install_dependencies to common_airflow ([`8a0cd9e`](https://github.com/opentargets/genetics_etl_python/commit/8a0cd9ee8a012f7285774d53fc869b9e4ec04e05))

* refactor: use a single shared cluster for all tasks ([`62a1fed`](https://github.com/opentargets/genetics_etl_python/commit/62a1fed03ae8c010bc6eade85f1b279d32ade671))

* refactor: subgroup and DAG definition ([`9298b75`](https://github.com/opentargets/genetics_etl_python/commit/9298b753f30568315de8544d4ecfe1329deb800d))

* refactor: submitting a PySpark job ([`4b26db5`](https://github.com/opentargets/genetics_etl_python/commit/4b26db5a7ebfff6d26fc7ef4c5c6edf7883a5097))

* refactor: remove redundant step to install dependencies ([`642a45c`](https://github.com/opentargets/genetics_etl_python/commit/642a45c3000f3550c523655b9ca7e151304298bb))

* refactor: unify cluster deletion ([`93af2bb`](https://github.com/opentargets/genetics_etl_python/commit/93af2bb325d04862c3165336c4b3d7ba6f9792b2))

* refactor: unify cluster creation ([`6ab2aa2`](https://github.com/opentargets/genetics_etl_python/commit/6ab2aa2bdd2f5115673e3c27d4ed24fa94100a2f))

* refactor: reconcile changes with main ([`9342490`](https://github.com/opentargets/genetics_etl_python/commit/9342490cd7da385f8b07083802cb60d739243e1a))

* refactor(intervals): change input structure to dict ([`5e57a47`](https://github.com/opentargets/genetics_etl_python/commit/5e57a47ed367108b03d898d7fa32b2cd6e8b32a1))

* refactor(variant_annotation): remove cols parameter in filter_by_variant_df ([`6bc680f`](https://github.com/opentargets/genetics_etl_python/commit/6bc680fcdaeddffc59a26516b2aaabb3268a5ef7))

* refactor(liftover): download chain file with google cloud api and not gcsfs ([`b16382c`](https://github.com/opentargets/genetics_etl_python/commit/b16382cc5843251375fc5f13040bcb9569b7e428))

* refactor(variant_annotation): remove `position` from v2g data ([`d888305`](https://github.com/opentargets/genetics_etl_python/commit/d888305f53778462dcc40711cb140c1f996f5b14))

* refactor(variant_annotation): remove `label`from `get_sift_v2g` and improve docs ([`b17dc5b`](https://github.com/opentargets/genetics_etl_python/commit/b17dc5be640297d912c44e3ff8411bbbb6d7b813))

* refactor(variant_annotation): remove `label`from `get_polyphen_v2g` and improve docs ([`6e7efdb`](https://github.com/opentargets/genetics_etl_python/commit/6e7efdbaf543c358717526edb4e192742588bc8e))

* refactor(variant_annotation): `get_most_severe_vep_v2g` doesn&#39;t parse csq and `label`is removed ([`23968f4`](https://github.com/opentargets/genetics_etl_python/commit/23968f44e3dad6050fadb6ffb44214390cce89f6))

* refactor(studylocus): simplify `unique_variants_in_locus` and improve testing ([`28527cd`](https://github.com/opentargets/genetics_etl_python/commit/28527cd066d85bc5b34ba58c63cc5c74f575828c))

* refactor(variantindex): add logic to filter for variants to `VariantIndex.from_variant_annotation` ([`0663b2d`](https://github.com/opentargets/genetics_etl_python/commit/0663b2dde0d51482bbb029b44d8301504114feb5))

* refactor(variant_index): checkpoint ([`9fab191`](https://github.com/opentargets/genetics_etl_python/commit/9fab19143eac49fb40497c981df89f892a04694e))

* refactor: convert `L2GFeatureMatrix.fill_na` to class method ([`359face`](https://github.com/opentargets/genetics_etl_python/commit/359face57ffbf99148bbefb1876b94aa475940bd))

* refactor: use featurematrix as input to predict and fit ([`6d5fb0c`](https://github.com/opentargets/genetics_etl_python/commit/6d5fb0c8dfff8644d7c5a0ac081131534dff121f))

* refactor: simplifying clumping function call ([`ec4f8db`](https://github.com/opentargets/genetics_etl_python/commit/ec4f8dba43cf2e915b101355e33768bbdcbf4292))

* refactor: move L2GFeature back to feature_factory to avoid circular import ([`e09fe0e`](https://github.com/opentargets/genetics_etl_python/commit/e09fe0e138b28b0ef51ae8a9b03b171d2b3bbf7a))

* refactor: move L2GFeature inside datasets ([`7f2dac8`](https://github.com/opentargets/genetics_etl_python/commit/7f2dac84ce109c02fa07dc46fa7fc55e722ce2e5))

* refactor: rename classifier to estimator + improvements ([`e9cae1d`](https://github.com/opentargets/genetics_etl_python/commit/e9cae1d11b52a9edb3b0b420e5544aa86e82b102))

* refactor: add &#34;add_pipeline_stage&#34; to nested cv ([`f7d10bb`](https://github.com/opentargets/genetics_etl_python/commit/f7d10bb48bba76cbe318fba5484f1334087889d3))

* refactor: relocate schemas inside dataset ([`a7c3eba`](https://github.com/opentargets/genetics_etl_python/commit/a7c3eba5c8fa15f98a058878e1017562113889b6))

* refactor: `_get_reverse_complement` returns null if allele is nonsensical ([`1e40dd2`](https://github.com/opentargets/genetics_etl_python/commit/1e40dd249a9c823a39a47ad2f78ec0c20b0a81ff))

* refactor: generalise ld annotation to study locus level ([`45ed8b0`](https://github.com/opentargets/genetics_etl_python/commit/45ed8b081db3133c0c743ee06a57404663ea9185))

* refactor(pics): remove unnecessary absolute value for r2 ([`b9a194e`](https://github.com/opentargets/genetics_etl_python/commit/b9a194e50f98e680f4aa0f935a74634d44c0cb15))

* refactor: tidying one function ([`1df9e55`](https://github.com/opentargets/genetics_etl_python/commit/1df9e555c301f1af755db1e524738099aa4a97b3))

* refactor: remove notebook ([`177cfd9`](https://github.com/opentargets/genetics_etl_python/commit/177cfd9b86e2c4a740ad8af08a340707347e9a08))

* refactor: generalizing clumping funcion ([`4dbc147`](https://github.com/opentargets/genetics_etl_python/commit/4dbc14703338744035c797ec96df19f690b4dbb7))

* refactor: rename left and right studyLocusId to camel case ([`8ba245a`](https://github.com/opentargets/genetics_etl_python/commit/8ba245a35c0d4f174674e958278718d528b47a5c))

* refactor: rename colocalisation fields to follow camelcase ([`361478a`](https://github.com/opentargets/genetics_etl_python/commit/361478a3ffd2e730c705f77cf18b36bf7a5d5046))

* refactor: rename  to ([`ff51796`](https://github.com/opentargets/genetics_etl_python/commit/ff51796e3edd0a35a9185cd92e152fcf0f0a04be))

* refactor: minor changes to ld ([`6ebd79e`](https://github.com/opentargets/genetics_etl_python/commit/6ebd79e67e6adb927df346fa48719199c5ed8570))

* refactor: `clump` and `unique_lead_tag_variants` to use the ldSet ([`c37d5e0`](https://github.com/opentargets/genetics_etl_python/commit/c37d5e0f97e61e06035b8e75c7f4b42a8db35fa9))

* refactor: rewrite `StudyLocus.annotate_ld` to accommodate new logic ([`7ec3684`](https://github.com/opentargets/genetics_etl_python/commit/7ec3684f8fa237fd7f342d16707674af8095d71d))

* refactor: move iteration over pops to `from_gnomad` ([`765909a`](https://github.com/opentargets/genetics_etl_python/commit/765909a8b5a0fdc288d40ee433715ef3b11c1b83))

* refactor: the logic in calculate_confidence_interval_for_summary_statistics is moved ([`c73275a`](https://github.com/opentargets/genetics_etl_python/commit/c73275ab01234692e1ec92f452413ae2a7ac5b7b))

* refactor(ld_annotation_by_locus_ancestry): use select instead of drop for intelligibility ([`dc2132d`](https://github.com/opentargets/genetics_etl_python/commit/dc2132de5fdefde08c5aa9923e3c5c19e0750b26))

* refactor: create credibleSet according to full schema ([`f085065`](https://github.com/opentargets/genetics_etl_python/commit/f0850653e519ab8e5a81ff0e3499c9a88b416df2))

* refactor(ld_annotation_by_locus_ancestry): get unique pops ([`3f842c9`](https://github.com/opentargets/genetics_etl_python/commit/3f842c9aa9a5636a58191fe9f7b8e79d6c8ad22c))

* refactor: some minor issues sorted out around summary statistics ([`5db1731`](https://github.com/opentargets/genetics_etl_python/commit/5db1731ae8ca92570c8a5c3c7449b31cfb1b8d41))

* refactor(ldindex): remove unnecessary nullability check ([`2df071c`](https://github.com/opentargets/genetics_etl_python/commit/2df071c7003a6948b8d815085de6b9c3a9a13a92))

* refactor: remoce subStudyDescription from associations dataset ([`0ce6a45`](https://github.com/opentargets/genetics_etl_python/commit/0ce6a45e4739cfce2253654ea8c0a0ea9b019c8a))

* refactor: deprecate _is_in_credset ([`2f2a3eb`](https://github.com/opentargets/genetics_etl_python/commit/2f2a3eb07c27feea28113d0173b02c14dfaa3c19))

* refactor: remove chromosome from is_in_credset function ([`c317c55`](https://github.com/opentargets/genetics_etl_python/commit/c317c552c728f1ac20d190d6357db3993cf20a2d))

* refactor: drop redundants in tests

redundancy only happens in the testing contest ([`b22feb3`](https://github.com/opentargets/genetics_etl_python/commit/b22feb3e820c19e0ef83c1a3a9f1666ef5358a2d))

* refactor: move L2GFeature back to feature_factory to avoid circular import ([`a97830f`](https://github.com/opentargets/genetics_etl_python/commit/a97830f1764a332a9836fb8e3694345b9e5dd480))

* refactor: move L2GFeature inside datasets ([`0bc320e`](https://github.com/opentargets/genetics_etl_python/commit/0bc320e1b5db925363058f31599f4139294eb7da))

* refactor: rename classifier to estimator + improvements ([`8b0d766`](https://github.com/opentargets/genetics_etl_python/commit/8b0d7665e990416aefbc8b11f7b8b6b1b5021272))

* refactor: add &#34;add_pipeline_stage&#34; to nested cv ([`3b933e0`](https://github.com/opentargets/genetics_etl_python/commit/3b933e09bba26cc770ab10a2f5516b48105ac4a8))

* refactor: relocate schemas inside dataset ([`b34681f`](https://github.com/opentargets/genetics_etl_python/commit/b34681fc23b5a40b43e3654688323d837b275241))

* refactor: `_get_reverse_complement` returns null if allele is nonsensical ([`3d33599`](https://github.com/opentargets/genetics_etl_python/commit/3d33599b79b5ce300c781184614b8fd9e5f6fe10))

* refactor: exploring custom jar integration ([`19ee061`](https://github.com/opentargets/genetics_etl_python/commit/19ee0612348a89c074b94abf724e75014fb2ef2a))

* refactor: review biotype filter ([`aa8148b`](https://github.com/opentargets/genetics_etl_python/commit/aa8148b7467aa48eef76ef8be78a78f05468c13f))

* refactor: rename to `geneFromPhenotypeId` ([`9528b72`](https://github.com/opentargets/genetics_etl_python/commit/9528b72c9362ee082180c7b480706c1e25e4915a))

* refactor: rename `coloc_metadata` to `utils` to add `extract_credible_sets` fun ([`9d60925`](https://github.com/opentargets/genetics_etl_python/commit/9d609257cdc58871e2d7f033dd901c972552b63e))

* refactor: state private methods and rename `coloc_utils` folder ([`f627500`](https://github.com/opentargets/genetics_etl_python/commit/f62750067dbc16e93ce4b90b22a227bb2a86ebef))

* refactor: simplify logic in run_coloc ([`cfbc1b7`](https://github.com/opentargets/genetics_etl_python/commit/cfbc1b763876b6d7bbbc357107e92313f88c09f6))

* refactor: find_all_vs_all_overlapping_signals processes a df ([`55d3f8e`](https://github.com/opentargets/genetics_etl_python/commit/55d3f8e1b2c6acedbe8d2ca4d90856b22dcb69c1))

* refactor: export indepdently, part by chrom and ordered by pos ([`3fdb2eb`](https://github.com/opentargets/genetics_etl_python/commit/3fdb2ebd161e19d47fe8bdcff1de8ba9e4449aca))

* refactor: extract the logic to read data from the main function to reuse the dfs ([`4535077`](https://github.com/opentargets/genetics_etl_python/commit/4535077b8b1733bff87600b011ba827108a912d0))

* refactor: convert parameters to dataframes, not paths ([`126ffeb`](https://github.com/opentargets/genetics_etl_python/commit/126ffeb6ba214d8eeafa7dc1907f6be152b048dc))

* refactor: bigger machine, added score to jung, optimisations ([`655c07b`](https://github.com/opentargets/genetics_etl_python/commit/655c07b03b8dc25e8b64879516ad032c81aa788d))

* refactor: adapt config and remove run_intervals.py ([`19f78c9`](https://github.com/opentargets/genetics_etl_python/commit/19f78c93e85dd1076ad1648cb2940722c806d513))

* refactor: default maximum window for liftover set to 0 ([`2855145`](https://github.com/opentargets/genetics_etl_python/commit/2855145b1c38303d1dbb74732ac5290c99620246))

* refactor: minor improvements ([`5c20160`](https://github.com/opentargets/genetics_etl_python/commit/5c2016080c1be4b6802c49b496daafca14a78071))

* refactor: apply review suggestions ([`07ab14c`](https://github.com/opentargets/genetics_etl_python/commit/07ab14ca7d48f18cf4c0827d681513198b3cab57))

* refactor: optimise `test_gwas_process_assoc` ([`bcd908d`](https://github.com/opentargets/genetics_etl_python/commit/bcd908d4390b38c7db7927aac661f2dd34ee53f1))

* refactor: reorder `process_associations` ([`7460eda`](https://github.com/opentargets/genetics_etl_python/commit/7460eda5c11c8f06883bbe3ab7be37ed965a5e5b))

* refactor: adapt `map_variants` to variant schema ([`425a4bb`](https://github.com/opentargets/genetics_etl_python/commit/425a4bbbc809104537bc2131556edc24c354983e))

* refactor: reorder `ingest_gwas_catalog_studies` ([`8f4efee`](https://github.com/opentargets/genetics_etl_python/commit/8f4efee7423a7309411deb9bfd929b977fb62ac3))

* refactor: slim `extract_discovery_sample_sizes` ([`3e2e40f`](https://github.com/opentargets/genetics_etl_python/commit/3e2e40fc1f8a15703298b53ce07c0d019bf727ec))

* refactor: optimise study sumstats annotation ([`63be3c3`](https://github.com/opentargets/genetics_etl_python/commit/63be3c3af06cf4121ed8a8c920480ccda685f076))

* refactor: optimise `parse_efos` ([`695b4f0`](https://github.com/opentargets/genetics_etl_python/commit/695b4f004c13c4dc28b10f2db9fb4e7a28e4cc48))

* refactor: use select statements in `get_sumstats_location` ([`886bdca`](https://github.com/opentargets/genetics_etl_python/commit/886bdca5b3c0d0ee78ff5967283917ea0f141ff8))

* refactor: merging main branch ([`e2ca92d`](https://github.com/opentargets/genetics_etl_python/commit/e2ca92d829f77337f21a0b43e0cdaac1ecf77717))

### Style

* style: docstring for eQTL Catalogue summary stats ingestion ([`59a5391`](https://github.com/opentargets/genetics_etl_python/commit/59a5391566f5adc94d55c4779b684b1be060d2dd))

* style: move session config to top ([`4c98b24`](https://github.com/opentargets/genetics_etl_python/commit/4c98b24a692743fede5b8b422c39f7819f35f1c7))

* style: use uniform lists in YAML ([`ffa87c6`](https://github.com/opentargets/genetics_etl_python/commit/ffa87c67c9b2adf46888fbe7ad22f53c9dcd7ea3))

* style: complete typing and docs ([`dccc6d8`](https://github.com/opentargets/genetics_etl_python/commit/dccc6d8efcde295faa2daf97a565c1370ad5688f))

* style: rename top module variables to upper case ([`ef0e471`](https://github.com/opentargets/genetics_etl_python/commit/ef0e471b42891bc398b753f7fb7d26824a65b710))

* style: fix docstring linting issues ([`9301557`](https://github.com/opentargets/genetics_etl_python/commit/930155765e7e7c474f877faad0018b329d977e79))

* style: fix docstring linting issues ([`02ae439`](https://github.com/opentargets/genetics_etl_python/commit/02ae439014496a2fced66b091208f982c74839f6))

* style(config): rename `my_config` to `config` ([`95cdda9`](https://github.com/opentargets/genetics_etl_python/commit/95cdda95d0b0b2cbff9d38294cc5e36b260bf723))

* style: simplify task names ([`3b5ba61`](https://github.com/opentargets/genetics_etl_python/commit/3b5ba6185619b0a65e80651b7ffb62e8acd2af10))

* style(intervals): rename individual reading methods to common name ([`0e673eb`](https://github.com/opentargets/genetics_etl_python/commit/0e673eb361b698dbb01b78ec1b995b55a05780e9))

* style: self code review ([`66fa6eb`](https://github.com/opentargets/genetics_etl_python/commit/66fa6eb3f8a253f15f58d0f8c67b2515d355e9d1))

* style(study_locus): remove unnecessary logic ([`7c63ac8`](https://github.com/opentargets/genetics_etl_python/commit/7c63ac87207ab43b9242c389c11354071b41d981))

* style: import row ([`de85023`](https://github.com/opentargets/genetics_etl_python/commit/de85023fd8ab19e09f9b1ea583d52bf115c2f1ba))

* style: reorganise individual function calls into a concatenated chain of calls ([`7a4a8d6`](https://github.com/opentargets/genetics_etl_python/commit/7a4a8d6a948208bfd8def728e9661ecdfa87f5a9))

* style: uniform comments in gcp.yaml ([`6978170`](https://github.com/opentargets/genetics_etl_python/commit/69781709584043c5485dd74dec9570ba35b7760e))

* style: reorganise configuration in line with the new structure ([`eed3d9e`](https://github.com/opentargets/genetics_etl_python/commit/eed3d9e095a660ebe078f41096e81fa7ce086318))

* style: rename ld indices location to directory and no extension ([`512c842`](https://github.com/opentargets/genetics_etl_python/commit/512c842e7e5ac744820b017fc5640b8bf009ef24))

* style: using fstring for concatenation ([`2855069`](https://github.com/opentargets/genetics_etl_python/commit/28550697affe1151a28073549d61a0e30f273d06))

* style: validate variant inputs ([`ebe6717`](https://github.com/opentargets/genetics_etl_python/commit/ebe6717b1207d1629685a71421b4cbce3c4b590b))

### Test

* test: Improvements to `test_dataset` and `test_clump_step` (#312)

* test: rename TestDataset to MockDataset

* test: output test_clumpstep_summary_stats results to temp dir ([`ba4a431`](https://github.com/opentargets/genetics_etl_python/commit/ba4a431bf69b28bec044e2cde40f3a9dd7658bdd))

* test: failing doctest in different python version (#320)

* test: issue on test with different python version

* refactor: value error instead of assertion ([`bbd9df4`](https://github.com/opentargets/genetics_etl_python/commit/bbd9df4af3bde6c8b8d3855996184721800bd5b1))

* test(l2g_gold_standard): add `test_remove_false_negatives` ([`aa4246c`](https://github.com/opentargets/genetics_etl_python/commit/aa4246ccea29023e7022663eaec338eeb7a57f18))

* test: add `test_filter_unique_associations` ([`8007726`](https://github.com/opentargets/genetics_etl_python/commit/80077267bac0b948e8618581bc3dfa10b39388cb))

* test: testing for `process_gene_interactions` ([`ca94412`](https://github.com/opentargets/genetics_etl_python/commit/ca94412df9a0ebc950b3b10cb0b32c403eacc0d0))

* test: add `test_expand_gold_standard_with_negatives_same_positives` ([`8347b2f`](https://github.com/opentargets/genetics_etl_python/commit/8347b2f896d5a808a57d924050cd7f498eb30a2a))

* test: fix and test logic in `expand_gold_standard_with_negatives` ([`dd95d9c`](https://github.com/opentargets/genetics_etl_python/commit/dd95d9c51fec65f754d702a67301925fa7d97827))

* test: add `test_parse_positive_curation` ([`ab29c9a`](https://github.com/opentargets/genetics_etl_python/commit/ab29c9a70281186c947f5ce93b4c8bdd4ea4bc96))

* test: add test for eQTL Catalogue summary stats ([`7bcf1c8`](https://github.com/opentargets/genetics_etl_python/commit/7bcf1c8989bd55c8724b6548579f3a6be09f9e47))

* test: add test for eQTL Catalogue study index ([`430f677`](https://github.com/opentargets/genetics_etl_python/commit/430f677a87c67e43b275b92c9c336d16b9f1e811))

* test: add sample eQTL Catalogue summary stats ([`3b00bb9`](https://github.com/opentargets/genetics_etl_python/commit/3b00bb917632c7ef7f496867ad4b7c33787557b1))

* test: add sample eQTL Catalogue studies ([`4c87a9e`](https://github.com/opentargets/genetics_etl_python/commit/4c87a9efc159093886300143de71256372000fbf))

* test: add conftests for eQTL Catalogue ([`e27b065`](https://github.com/opentargets/genetics_etl_python/commit/e27b0650fe9bbf2016e9cb4793afc2a8c603a65d))

* test: refactor test_gnomad_ld ([`335cedf`](https://github.com/opentargets/genetics_etl_python/commit/335cedfd914a87984eb76205b624049377d9f892))

* test: add test for empty ld_slice wip ([`86c3a8a`](https://github.com/opentargets/genetics_etl_python/commit/86c3a8ada0419ab741cb460c832a209bb6d34903))

* test: adding test for variant resolved LD function ([`4aa2c78`](https://github.com/opentargets/genetics_etl_python/commit/4aa2c78f82699b95b9f0c07f38533460efa4d235))

* test: rescue missing test ([`5987ac3`](https://github.com/opentargets/genetics_etl_python/commit/5987ac37489d1d70d0c4f6afd7deeb90f1296ccd))

* test: fixing hail initialization ([`0c462d5`](https://github.com/opentargets/genetics_etl_python/commit/0c462d582df7d0de9849046feee8cc68dd256282))

* test: add hail config to spark session to fix `test_hail_configuration` ([`5f4aee8`](https://github.com/opentargets/genetics_etl_python/commit/5f4aee8dabaacd3bc2a57caaf92d1fad69c98597))

* test: adjusting environment ([`7067227`](https://github.com/opentargets/genetics_etl_python/commit/7067227c47f41c5c3a25712cf77b6dde35403e1a))

* test: add `hail_home` fixture ([`6b737fc`](https://github.com/opentargets/genetics_etl_python/commit/6b737fc318646ed33990e530d6986f0cff4d9dc7))

* test: a better way to test import of DAGs ([`5a63093`](https://github.com/opentargets/genetics_etl_python/commit/5a6309399a87ad7b9ed18408e430160e6facb8a7))

* test: use a different way to amend path ([`dd89403`](https://github.com/opentargets/genetics_etl_python/commit/dd89403ae4b0137b63b880c32bc69c1a2653bcc8))

* test: re-enable and fix test_no_import_errors ([`abb9d68`](https://github.com/opentargets/genetics_etl_python/commit/abb9d687d54faf112a2d92099eb524908d02187c))

* test(l2g): add `test_get_tss_distance_features` ([`7809278`](https://github.com/opentargets/genetics_etl_python/commit/780927801f85fa7d42cc7001bd7e87fa1a61d38e))

* test: add `test_get_coloc_features` ([`eb0e399`](https://github.com/opentargets/genetics_etl_python/commit/eb0e39933eb2c2124885743f2356c602ab28d624))

* test(schemas): add doctest in _get_spark_schema_from_pandas_df ([`66dfaa2`](https://github.com/opentargets/genetics_etl_python/commit/66dfaa26bdbe3b688b3de28f22c3a9c81007651e))

* test(l2g): add test_l2g_gold_standard ([`e906ca3`](https://github.com/opentargets/genetics_etl_python/commit/e906ca3055f39da1eab42042f65d2fdda59da3dc))

* test: fixing clumping test ([`86279b6`](https://github.com/opentargets/genetics_etl_python/commit/86279b6eacca12c24bcf0125fdd3c40237a9b695))

* test: update the tests ([`a990baf`](https://github.com/opentargets/genetics_etl_python/commit/a990baf6093a8330e78640a2ca04ec7f958b19c3))

* test: update FinnGen summary stats data sample ([`689bd47`](https://github.com/opentargets/genetics_etl_python/commit/689bd476eaadb9015a8de9fa56a5ff1e2d153b1f))

* test: add tests for FinnGenSummaryStats ([`82be100`](https://github.com/opentargets/genetics_etl_python/commit/82be1006f492475d6ed1b0fdf1639a6525c66b73))

* test: update existing tests ([`a6e1f2b`](https://github.com/opentargets/genetics_etl_python/commit/a6e1f2b16d61e61bc2593b198e95702827eb1c56))

* test: add test for `train` method ([`670bb3e`](https://github.com/opentargets/genetics_etl_python/commit/670bb3e084807bbd8a88a2ac68cbdfabbbba3ae1))

* test: add test for cross validation ([`ddeca2c`](https://github.com/opentargets/genetics_etl_python/commit/ddeca2c229466fe55bac6669c1b2a0d13ee1c23f))

* test: add tests for l2g datasets ([`178b6f6`](https://github.com/opentargets/genetics_etl_python/commit/178b6f645cdc4944c8d72cd0e7d09fadbd4f9dac))

* test: add test for `add_pipeline_stage` ([`b3bdd40`](https://github.com/opentargets/genetics_etl_python/commit/b3bdd40cc7b324f654bf003fb7158decc2c0dd9c))

* test: text fixes ([`9b3a65e`](https://github.com/opentargets/genetics_etl_python/commit/9b3a65e216a52fb6810bafc8dbec5714244de47d))

* test: the expectation is that an empty locus can result in none ([`952b5ab`](https://github.com/opentargets/genetics_etl_python/commit/952b5aba925563f32e496cc000f3953cad8caeb9))

* test: qc_nopopulation ([`3bf071c`](https://github.com/opentargets/genetics_etl_python/commit/3bf071c0f3a47b8a81eeefb387498aacac84e3e2))

* test: fix tests ([`3518f53`](https://github.com/opentargets/genetics_etl_python/commit/3518f53e9460869e687ced0b57c1f8df64102f95))

* test: adding test for gwas catalog study ingestion ([`428c880`](https://github.com/opentargets/genetics_etl_python/commit/428c880f267ae4b38f1b807f6b3938da8674bb8a))

* test: additional tests around finemapping ([`46b2020`](https://github.com/opentargets/genetics_etl_python/commit/46b202077e29c1544de0419c78705b9ee1764e29))

* test: duplicated test filename ([`c93eb71`](https://github.com/opentargets/genetics_etl_python/commit/c93eb71f69e77f93962f746af4ecb94f9b70f3e0))

* test: correct sample file ([`989d7c4`](https://github.com/opentargets/genetics_etl_python/commit/989d7c45ae6fd6073dc021bd51fab8848e672965))

* test: fix sample dataset used ([`8a3de54`](https://github.com/opentargets/genetics_etl_python/commit/8a3de5432e889d16f4ca70776970320b4031aa8b))

* test: isolated pyspark for _prune_peak function ([`cc30179`](https://github.com/opentargets/genetics_etl_python/commit/cc3017982d4d04841d7b0274b6b762fa7d0b5953))

* test: chromosome missing in fixture ([`c186ce9`](https://github.com/opentargets/genetics_etl_python/commit/c186ce9f9c4799124a964bff14208e6138f11366))

* test: adding tests ([`13c83d7`](https://github.com/opentargets/genetics_etl_python/commit/13c83d76a4b2ccbae2eb0ac571952bd98bb0b841))

* test: added `test_overlapping_peaks` ([`b532ddb`](https://github.com/opentargets/genetics_etl_python/commit/b532ddbe347c1fcfc602f49fd00b4ec537afb50c))

* test: added `test_study_locus_overlap_from_associations` ([`2b25284`](https://github.com/opentargets/genetics_etl_python/commit/2b25284390496e8cdc7073ba78ee268f12a650aa))

* test: add `TestLDAnnotator` ([`c07c51e`](https://github.com/opentargets/genetics_etl_python/commit/c07c51e39f3eebcc30967d57954d9c4690a39e13))

* test: add `test__resolve_variant_indices` ([`a1882ec`](https://github.com/opentargets/genetics_etl_python/commit/a1882eca506287a841bdddfafbcc12a8726e3585))

* test: update ldindex test ([`b71c5f2`](https://github.com/opentargets/genetics_etl_python/commit/b71c5f2843e8d1d900ec638dd664fcf189687a1d))

* test: add doctest to `_aggregate_ld_index_across_populations` ([`c7651fe`](https://github.com/opentargets/genetics_etl_python/commit/c7651fe42068d8b491bdf1ff91fecc9b28975b04))

* test: add `calculate_confidence_interval` doctest ([`8ab0e88`](https://github.com/opentargets/genetics_etl_python/commit/8ab0e88f07507981df13adafe75209bb6d137264))

* test: add `test__finemap` ([`7e7fb27`](https://github.com/opentargets/genetics_etl_python/commit/7e7fb27a50c127da0f50f963ddb138f2fa9abe67))

* test: update failing test ([`0343621`](https://github.com/opentargets/genetics_etl_python/commit/0343621a985cac92933f849b1010d13ea6863d4c))

* test: more clarity in _collect_clump test ([`31363aa`](https://github.com/opentargets/genetics_etl_python/commit/31363aaa933a2a3fadd06b8f8d79826ec682d751))

* test: _filter_leads functions doctest ([`e6f0343`](https://github.com/opentargets/genetics_etl_python/commit/e6f0343061b2c2762129a301ce2215dc296ce3d3))

* test: testing for window based clumping function ([`aedecb9`](https://github.com/opentargets/genetics_etl_python/commit/aedecb981922993ef5da6a756ba0b5811bcc4c27))

* test: update test sample data ([`e5ca634`](https://github.com/opentargets/genetics_etl_python/commit/e5ca63496c3b488c186fe144f10a5a3ce9c28bda))

* test: update tests ([`b28a642`](https://github.com/opentargets/genetics_etl_python/commit/b28a6422255b5a0aeb3409522b318278530e7fd0))

* test: add UKBiobank study index test ([`3f79d82`](https://github.com/opentargets/genetics_etl_python/commit/3f79d82a97b88be5181807e9431393a99ca19952))

* test: add UKBiobank study configuration test ([`ebe8a63`](https://github.com/opentargets/genetics_etl_python/commit/ebe8a63d9f01f27af319de3378c26214087224f5))

* test: add `test_finemap_pipeline` ([`56fc5a8`](https://github.com/opentargets/genetics_etl_python/commit/56fc5a82a852da32a65f6ea1427a597c55cbf36a))

* test: create non zero p values ([`97bcdc3`](https://github.com/opentargets/genetics_etl_python/commit/97bcdc3f369bf19539b500197999a9bef90cba4f))

* test: add `test_finemap_null_r2` (fails) ([`56c782a`](https://github.com/opentargets/genetics_etl_python/commit/56c782ae58e25efbf21b385cab6cd2a8d5027711))

* test: improve ld tests ([`7d2eb68`](https://github.com/opentargets/genetics_etl_python/commit/7d2eb6878251681216dbb9661c3f35b2ec729433))

* test: fix FinnGen study test ([`bc3ec92`](https://github.com/opentargets/genetics_etl_python/commit/bc3ec92c971677a2df6994f0b7ed4f85fda592e1))

* test: test ingestion from source for StudyIndexFinnGen ([`08cb9f9`](https://github.com/opentargets/genetics_etl_python/commit/08cb9f96b95772129b4031a4bef0b27dd7d8b8a8))

* test: implement sample FinnGen study data fixture ([`d4b64bc`](https://github.com/opentargets/genetics_etl_python/commit/d4b64bccf365edc2c08671a47f3d0bfd69a9364e))

* test: prepare sample FinnGen study data ([`c1a69ac`](https://github.com/opentargets/genetics_etl_python/commit/c1a69acbca4e8ef2a557e3dc5123a3f530f63134))

* test: test schema compliance for StudyIndexFinnGen ([`e9a6a2e`](https://github.com/opentargets/genetics_etl_python/commit/e9a6a2e8ab0b34f8628a68a068ed48d9974e7499))

* test: implement mock study index fixture for FinnGen ([`1125d64`](https://github.com/opentargets/genetics_etl_python/commit/1125d645e437b1b2bdf48d76fcb7bd43981449ec))

* test: add `test_variants_in_ld_in_gnomad_pop` (hail misconfiguration error) ([`8cabb5f`](https://github.com/opentargets/genetics_etl_python/commit/8cabb5f61e1f8698d25e1aa483dac605c8ed3907))

* test: add test_variant_coordinates_in_ldindex (passes) ([`840dcc7`](https://github.com/opentargets/genetics_etl_python/commit/840dcc7987cdd0168dadfb3ae8960fbd9a12ffc6))

* test: doctest dataframe ordering unchanged ([`0494730`](https://github.com/opentargets/genetics_etl_python/commit/0494730881bafa790a17966f51abe2ef15f9af75))

* test: map_to_variant_annotation_variants ([`b99b58d`](https://github.com/opentargets/genetics_etl_python/commit/b99b58da903e1c7394439c0af48de2cc1f05aa83))

* test: study locus gwas catalog from source ([`a27b51f`](https://github.com/opentargets/genetics_etl_python/commit/a27b51ffa2fa357bb3ff7e5108da60bbb059b82d))

* test: added ([`577c50a`](https://github.com/opentargets/genetics_etl_python/commit/577c50aa4117f47c31c33c9b1543effad238b7fa))

* test: is in credset function ([`65bc2aa`](https://github.com/opentargets/genetics_etl_python/commit/65bc2aace3bc76f735f57a792f1e9ac1b47a3d47))

* test: added `test_validate_schema_different_datatype` ([`f76e62f`](https://github.com/opentargets/genetics_etl_python/commit/f76e62f4d562e497839ea4f66b00e194a3498757))

* test: add `TestValidateSchema` suite ([`b997962`](https://github.com/opentargets/genetics_etl_python/commit/b9979622cda602dc806a5b17db76eeccc8313b0f))

* test: add test for `train` method ([`4c9f134`](https://github.com/opentargets/genetics_etl_python/commit/4c9f1340bf44f44936eb95e21d7a027320dc519f))

* test: add test for cross validation ([`f233352`](https://github.com/opentargets/genetics_etl_python/commit/f2333529abed7aa66b9c41e9e317bc7ce088914c))

* test: add tests for l2g datasets ([`4ec2248`](https://github.com/opentargets/genetics_etl_python/commit/4ec22484f1af7b0c32c9f7e88589dc962a4b4395))

* test: add test for `add_pipeline_stage` ([`23fc0e9`](https://github.com/opentargets/genetics_etl_python/commit/23fc0e9166125025859f4d28b3dd35081af018ad))

* test: text fixes ([`d08a5dc`](https://github.com/opentargets/genetics_etl_python/commit/d08a5dc25cf27d8e4d101899130bfa2f41f34768))

* test: convert_odds_ratio_to_beta doctest ([`483822c`](https://github.com/opentargets/genetics_etl_python/commit/483822c8f83a787617b2993e0975e59942811f44))

* test: adding more tests to summary statistics ([`c4d7a78`](https://github.com/opentargets/genetics_etl_python/commit/c4d7a78e4a17ec9c97d106d755be373844cc344c))

* test: gene_index step added ([`db081ec`](https://github.com/opentargets/genetics_etl_python/commit/db081ece3cd58b03052f98021a06c041a57e4400))

* test: concatenate substudy description ([`4d598ef`](https://github.com/opentargets/genetics_etl_python/commit/4d598ef84671ac88dbd0867c9b270c07f33a03ea))

* test: qc_all in study-locus ([`517ac48`](https://github.com/opentargets/genetics_etl_python/commit/517ac4832bda032e69219f568ca95a8ea06cd774))

* test: more qc tests ([`a7ab2a5`](https://github.com/opentargets/genetics_etl_python/commit/a7ab2a523a06ae88927e2d390143b9df004c058a))

* test: qc incomplete mapping ([`4c9d46a`](https://github.com/opentargets/genetics_etl_python/commit/4c9d46a4667d2a420433d7c86ee6a76deac11699))

* test: qc unmapped variants ([`2db11ab`](https://github.com/opentargets/genetics_etl_python/commit/2db11ab9c9cb477f6a3b11dec97894ddb9cb66dd))

* test: gnomad position to ensembl ([`23bab11`](https://github.com/opentargets/genetics_etl_python/commit/23bab11743f9f961a05a8dce7c6fa36550d65b1c))

* test: helpers ([`49d5586`](https://github.com/opentargets/genetics_etl_python/commit/49d558614587524be784aa1b1df42451859f021c))

* test: session ([`628eca9`](https://github.com/opentargets/genetics_etl_python/commit/628eca9bed663b348abd4e7e6b0e6a753168227e))

* test: splitter ([`d6669bb`](https://github.com/opentargets/genetics_etl_python/commit/d6669bbfbe190ff682097234dd19aa99b6e4f487))

* test: additional coverage in ld_index and gene_index datasets ([`733394f`](https://github.com/opentargets/genetics_etl_python/commit/733394ffc480dcdcc7af433a9b1fb18e8f470f1d))

* test: additional tests ([`4e50e45`](https://github.com/opentargets/genetics_etl_python/commit/4e50e45fb4d5c8dfd609a1917a3020d85edc8755))

* test: pvalue normalisation ([`57711d4`](https://github.com/opentargets/genetics_etl_python/commit/57711d4b2a89fef68991c3f81dc6a9e7578d4557))

* test: failing test ([`1d038c7`](https://github.com/opentargets/genetics_etl_python/commit/1d038c7bfed02426dcc1f1a85117cdb46009f5f8))

* test: pvalue parser ([`bbe9f7d`](https://github.com/opentargets/genetics_etl_python/commit/bbe9f7db4b3403425da9e77d5351cc8227a64fd5))

* test: unnecessary tests ([`9f896a5`](https://github.com/opentargets/genetics_etl_python/commit/9f896a53dc820d68aa3d8f3a58779bde170102e9))

* test: palindromic alleles ([`dafb66c`](https://github.com/opentargets/genetics_etl_python/commit/dafb66c503ef9934334eb7c63868ea9c63327720))

* test: adding tests for summary stats ingestion ([`64194d7`](https://github.com/opentargets/genetics_etl_python/commit/64194d7230793722f90ba67e913fcc43db3d8a54))

* test: pytest syntax complies with flake ([`c29c29d`](https://github.com/opentargets/genetics_etl_python/commit/c29c29d783c6f6b969148c02ae0c4262bf37780d))

* test: ld clumping test ([`13cf805`](https://github.com/opentargets/genetics_etl_python/commit/13cf80579f0b2d6a89b0cd9527fbbb65d7e4e31d))

* test: archive deprecated tests and ignore them ([`aae3978`](https://github.com/opentargets/genetics_etl_python/commit/aae3978284aa8baed647a4928464338c648f59d0))

* test: added tests for distance features ([`680184e`](https://github.com/opentargets/genetics_etl_python/commit/680184e3f0e4195c8638bc0f57fd69c6d47fb5e4))

* test: add validation step ([`e89e48c`](https://github.com/opentargets/genetics_etl_python/commit/e89e48c26863713dda4982530ca2cf7289138d84))

* test: refactor of testing suite ([`9aa4014`](https://github.com/opentargets/genetics_etl_python/commit/9aa401448b7cbfc568d3554e0bc2007ddf11b44d))

* test: added test suite for v2g evidence ([`9745304`](https://github.com/opentargets/genetics_etl_python/commit/974530459614aab1d7ca2c0c9dce60ca1bf64971))

* test: test only running on PRs again ([`53873ee`](https://github.com/opentargets/genetics_etl_python/commit/53873ee20aa638b5ce6ab8404bd8f0c68d12f270))

* test: revert change ([`6aaacc2`](https://github.com/opentargets/genetics_etl_python/commit/6aaacc29ffc97a962e4b0daa709691be88b1eea3))

* test: adapted to camelcase variables ([`ab3d4da`](https://github.com/opentargets/genetics_etl_python/commit/ab3d4daa3e2e8384ea69a3bef2965df427a51515))

* test: checking all schemas are valid ([`57712bb`](https://github.com/opentargets/genetics_etl_python/commit/57712bb57f0cfb802f40673d6c712ffedf17d938))

* test: pytest added to vscode configuration ([`9048fd8`](https://github.com/opentargets/genetics_etl_python/commit/9048fd8680038c3a6da511eb31b3e37251edabb4))

* test: nicer outputs with pytest-sugar ([`49928a5`](https://github.com/opentargets/genetics_etl_python/commit/49928a545a117a7ad2611d3a14f7cab24d630a85))

* test: adding more tests on v2d ([`0b586ab`](https://github.com/opentargets/genetics_etl_python/commit/0b586ab391c74edaad595309042ba9113106d8ff))

### Unknown

* Merge pull request #275 from opentargets/dependabot/pip/pyarrow-14.0.1

build(deps): bump pyarrow from 11.0.0 to 14.0.1 ([`22c5a94`](https://github.com/opentargets/genetics_etl_python/commit/22c5a9482b60582b17a2e13f2bab7f4165419e31))

* Merge branch &#39;main&#39; into dependabot/pip/pyarrow-14.0.1 ([`60882b9`](https://github.com/opentargets/genetics_etl_python/commit/60882b96f83eef5924d30b46f4b983c128a2f34f))

* Merge pull request #255 from opentargets/il-l2g-negative-gs

fix: change definition of negative l2g evidence ([`cd3c325`](https://github.com/opentargets/genetics_etl_python/commit/cd3c32586464a3ea7f93ffd4ceb53ff904be3210))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-negative-gs ([`5de5b77`](https://github.com/opentargets/genetics_etl_python/commit/5de5b7741ddd10b1e920ba1f0b236ad50b524932))

* Merge pull request #273 from opentargets/do_ignore_env

gitignore env file ([`525e9dd`](https://github.com/opentargets/genetics_etl_python/commit/525e9dd32b13280bf142fcde3676f7fe877afeea))

* Merge pull request #269 from opentargets/tskir-fix-local-ssds

Fix local SSD attachment syntax ([`24addab`](https://github.com/opentargets/genetics_etl_python/commit/24addabc54ef594308085b787f45f251f6dddd7c))

* Merge branch &#39;main&#39; into tskir-fix-local-ssds ([`9358cd9`](https://github.com/opentargets/genetics_etl_python/commit/9358cd962158078dee25f169590edc9e8b7513ab))

* Merge pull request #268 from opentargets/il-fix-calculate_confidence_interval

fix: multiply standard error by zscore in `calculate_confidence_interval` ([`e5e9f29`](https://github.com/opentargets/genetics_etl_python/commit/e5e9f29746a85e900f632d5d03c599f47df12f3e))

* Merge pull request #267 from opentargets/ds_3039_sumstats_update

Removing odds ratio, and confidence intervals from the schema ([`48c03f9`](https://github.com/opentargets/genetics_etl_python/commit/48c03f92dd03639e0750e1b0e8311789fb1d92ed))

* Merge branch &#39;main&#39; into ds_3039_sumstats_update ([`7ee2266`](https://github.com/opentargets/genetics_etl_python/commit/7ee2266e88e8c4563a8623a4ffcc44604cd2ba30))

* Merge pull request #238 from opentargets/tskir-3109-ingest-eqtl-catalog

Ingest eQTL Catalogue ([`3081441`](https://github.com/opentargets/genetics_etl_python/commit/30814418269cb5c006dcbb01d93206397c8fc3fb))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-negative-gs ([`1c6040b`](https://github.com/opentargets/genetics_etl_python/commit/1c6040b101eaaefddd812ec85ac47cec5ac861ce))

* Merge branch &#39;main&#39; into tskir-3109-ingest-eqtl-catalog ([`03de3ae`](https://github.com/opentargets/genetics_etl_python/commit/03de3ae960b61ad44d4138aaabd870b6e4d783d8))

* Merge pull request #266 from opentargets/ds_3039_sumstats_update

Updating summary stats schema and ingtesion ([`09dd2bc`](https://github.com/opentargets/genetics_etl_python/commit/09dd2bc355185d8d2fae5999b0ad8413c22a8735))

* Merge branch &#39;main&#39; into ds_3039_sumstats_update ([`0acb106`](https://github.com/opentargets/genetics_etl_python/commit/0acb106d04b013853bbb2e903bb4a12f330fed81))

* Merge branch &#39;main&#39; into tskir-3109-ingest-eqtl-catalog ([`4d5f7a4`](https://github.com/opentargets/genetics_etl_python/commit/4d5f7a4e63b4754117e4de42468f486311abc125))

* Merge pull request #265 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`4997432`](https://github.com/opentargets/genetics_etl_python/commit/4997432bf65f5ab87e26f20c4eb48337a8a0adb2))

* Merge pull request #264 from opentargets/il-empty-parquet

feat: raise error in `from_parquet` when df is empty ([`6b33ddd`](https://github.com/opentargets/genetics_etl_python/commit/6b33ddde58cb000ffaefc5ae2e39acb0fe5284d7))

* Merge branch &#39;main&#39; into il-empty-parquet ([`a4393fd`](https://github.com/opentargets/genetics_etl_python/commit/a4393fd661810353c0353c32ad159475405c7aa7))

* Merge pull request #263 from opentargets/dependabot/pip/mkdocstrings-python-1.7.4

build(deps-dev): bump mkdocstrings-python from 1.7.3 to 1.7.4 ([`85288f2`](https://github.com/opentargets/genetics_etl_python/commit/85288f2478c812dcb837a87e136f0d990bb49c2c))

* Merge branch &#39;main&#39; into dependabot/pip/mkdocstrings-python-1.7.4 ([`5bba5da`](https://github.com/opentargets/genetics_etl_python/commit/5bba5da83508743d9e2be666be354c88ff30dc15))

* Merge pull request #262 from opentargets/dependabot/pip/mkdocs-material-9.4.10

build(deps-dev): bump mkdocs-material from 9.4.8 to 9.4.10 ([`318d423`](https://github.com/opentargets/genetics_etl_python/commit/318d423596b8a7ebccdd37dc73d7ef7aac4b1b20))

* Merge branch &#39;main&#39; into dependabot/pip/mkdocs-material-9.4.10 ([`c318346`](https://github.com/opentargets/genetics_etl_python/commit/c3183462d77a4e819f8c77e6bdc9883846888f0f))

* Merge pull request #261 from opentargets/dependabot/pip/mypy-1.7.0

build(deps-dev): bump mypy from 1.6.1 to 1.7.0 ([`b4e1714`](https://github.com/opentargets/genetics_etl_python/commit/b4e17144eed3d2fe55ecbf617f047d7e4ad4c49b))

* Merge pull request #260 from opentargets/dependabot/pip/apache-airflow-providers-google-10.11.1

build(deps-dev): bump apache-airflow-providers-google from 10.11.0 to 10.11.1 ([`7b74c9c`](https://github.com/opentargets/genetics_etl_python/commit/7b74c9cb29030c9ddbe963669f8f16f75b0da24d))

* Merge branch &#39;main&#39; into dependabot/pip/apache-airflow-providers-google-10.11.1 ([`c4c13c4`](https://github.com/opentargets/genetics_etl_python/commit/c4c13c4d39e5489bec13edb5118d9d71e5817d43))

* Merge pull request #257 from opentargets/do_airflow_enhance_common

Extra parametrisation on airflow common ([`6bf49da`](https://github.com/opentargets/genetics_etl_python/commit/6bf49dac7677d51e1cd6bbbb2a38d25c2dc6c489))

* Merge branch &#39;main&#39; into do_airflow_enhance_common ([`8feeb1a`](https://github.com/opentargets/genetics_etl_python/commit/8feeb1abc6000db1db8db4c41424f12dc1a82b03))

* Merge pull request #246 from opentargets/dependabot/pip/google-cloud-dataproc-5.7.0

build(deps-dev): bump google-cloud-dataproc from 5.6.0 to 5.7.0 ([`a36b78f`](https://github.com/opentargets/genetics_etl_python/commit/a36b78f01c933e1e42fcd4ff366bb4446f09339e))

* Merge branch &#39;main&#39; into dependabot/pip/google-cloud-dataproc-5.7.0 ([`afa4ddd`](https://github.com/opentargets/genetics_etl_python/commit/afa4dddd186ca65173ea9b909c18d26b8c13b72e))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-negative-gs ([`186d2b3`](https://github.com/opentargets/genetics_etl_python/commit/186d2b3bc8f96a53c3efd4f9fb564b98d5a45d11))

* Merge branch &#39;main&#39; into do_airflow_enhance_common ([`ca6e5d9`](https://github.com/opentargets/genetics_etl_python/commit/ca6e5d96d32e72595ec51a3abbd592f749bab8c3))

* Merge branch &#39;main&#39; into do_airflow_enhance_common ([`6f73218`](https://github.com/opentargets/genetics_etl_python/commit/6f732180da658f2b566e34f43d73eebf2b52d92d))

* Merge pull request #259 from opentargets/do_pics_step

New PICS step ([`4e5a277`](https://github.com/opentargets/genetics_etl_python/commit/4e5a27780ca54bca26ced599594e5b774a8205c5))

* Merge branch &#39;main&#39; into do_pics_step ([`af90845`](https://github.com/opentargets/genetics_etl_python/commit/af908457d24e8f28695b162e40b414c0fbb012a1))

* Update src/otg/pics.py

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`f8c9aa8`](https://github.com/opentargets/genetics_etl_python/commit/f8c9aa8fdbdf42ce2aa86427f33942f0f242575c))

* Update src/otg/pics.py

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`edfc608`](https://github.com/opentargets/genetics_etl_python/commit/edfc608e8539881eb026632a6254618e6ce7827b))

* Update src/otg/pics.py

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`913a599`](https://github.com/opentargets/genetics_etl_python/commit/913a599faa8d97c59d0a1b3ebfc8e2a0addc2c0d))

* Update src/otg/pics.py

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`18e9577`](https://github.com/opentargets/genetics_etl_python/commit/18e9577805e614bb3e026def0e19018a1392cd75))

* Revert &#34;feat: datasets config&#34;

This reverts commit a07c7c4bf16df61fbcc4285b7e4dec946667beab. ([`0dab2f5`](https://github.com/opentargets/genetics_etl_python/commit/0dab2f57b99d484a08aa52d2b90dbbeaa13833c7))

* Merge pull request #256 from opentargets/tskir-3155-enable-local-ssds

Enable local SSDs for worker nodes ([`1351728`](https://github.com/opentargets/genetics_etl_python/commit/1351728fb8df45e82a747c1d218211274ab7a506))

* Merge branch &#39;il-l2g-negative-gs&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-negative-gs ([`a33e797`](https://github.com/opentargets/genetics_etl_python/commit/a33e797c04420e1c0de52e8d40b3aab2c9ce7f16))

* Merge branch &#39;main&#39; into il-l2g-negative-gs ([`e6b20f1`](https://github.com/opentargets/genetics_etl_python/commit/e6b20f190d65729902f75cbe83ddff83496fc390))

* Merge pull request #250 from opentargets/dependabot/pip/wandb-0.16.0

build(deps): bump wandb from 0.13.11 to 0.16.0 ([`a5cfd66`](https://github.com/opentargets/genetics_etl_python/commit/a5cfd6640397452f9a4dc52f640023f71400c8ee))

* Merge branch &#39;main&#39; into dependabot/pip/wandb-0.16.0 ([`f131c09`](https://github.com/opentargets/genetics_etl_python/commit/f131c097b7822b437cc6ffcd4da881a12a7620b9))

* Merge branch &#39;dependabot/pip/wandb-0.16.0&#39; of https://github.com/opentargets/genetics_etl_python into il-run-l2g ([`8759f43`](https://github.com/opentargets/genetics_etl_python/commit/8759f439a9ffff1e2a0e2085c5ff47c7c20311b7))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-run-l2g ([`ee461d4`](https://github.com/opentargets/genetics_etl_python/commit/ee461d4fc30c03d9795a62cc9f39c1573697277b))

* Merge pull request #252 from opentargets/il-3152

fix: persist raw gwascat associations to return consistent results ([`8ed6159`](https://github.com/opentargets/genetics_etl_python/commit/8ed61594d6c2b49cbde12aa266f0f102776caccd))

* Merge branch &#39;main&#39; into il-3152 ([`b5090c2`](https://github.com/opentargets/genetics_etl_python/commit/b5090c289d073d2566175da6efdaf20e38d7210a))

* Merge pull request #254 from opentargets/tskir-3154-fix-cluster

Adjust running parameters to increase stability ([`f6afd40`](https://github.com/opentargets/genetics_etl_python/commit/f6afd4094aa976e34dd97985089f5117229e09b4))

* Merge branch &#39;main&#39; into il-3152 ([`c1b7f7d`](https://github.com/opentargets/genetics_etl_python/commit/c1b7f7db03d6f1413ffbe039d7c9962293c14652))

* Merge branch &#39;main&#39; into tskir-3154-fix-cluster ([`8787cdd`](https://github.com/opentargets/genetics_etl_python/commit/8787cdd4aa290d7fcd86573b7e5d2b081819ba7c))

* Merge pull request #253 from opentargets/il-3151

fix: coalesce variantid to assign a studylocusid ([`9ec4f8b`](https://github.com/opentargets/genetics_etl_python/commit/9ec4f8b470f14c3bc582bc55c658c4cb37c27896))

* revert: repartition call ([`3f06a67`](https://github.com/opentargets/genetics_etl_python/commit/3f06a6724c28e497456a8735ad54b401d333a59e))

* revert: repartition call ([`75dd3d6`](https://github.com/opentargets/genetics_etl_python/commit/75dd3d6e5b0688e0737b550d7b9311c39b6e45c9))

* Merge branch &#39;main&#39; into il-3151 ([`9528c13`](https://github.com/opentargets/genetics_etl_python/commit/9528c136e016dc9671fe332460b765a52c95606d))

* Merge pull request #251 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`634c410`](https://github.com/opentargets/genetics_etl_python/commit/634c410f0f7a68f6c63961c87e2cfed4162ef057))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/astral-sh/ruff-pre-commit: v0.1.4 → v0.1.5](https://github.com/astral-sh/ruff-pre-commit/compare/v0.1.4...v0.1.5)
- [github.com/psf/black: 23.10.1 → 23.11.0](https://github.com/psf/black/compare/23.10.1...23.11.0)
- [github.com/alessandrojcm/commitlint-pre-commit-hook: v9.7.0 → v9.8.0](https://github.com/alessandrojcm/commitlint-pre-commit-hook/compare/v9.7.0...v9.8.0)
- [github.com/pre-commit/mirrors-mypy: v1.6.1 → v1.7.0](https://github.com/pre-commit/mirrors-mypy/compare/v1.6.1...v1.7.0) ([`cd2dd7a`](https://github.com/opentargets/genetics_etl_python/commit/cd2dd7a335c067ce73a287c2831448bc3d6bf8b2))

* Merge pull request #237 from opentargets/il-fix-hail

fix(session): hail config was not set unless extended_spark_conf was provided ([`07fe644`](https://github.com/opentargets/genetics_etl_python/commit/07fe644f21575380b0724a3e606249e3b21d26d4))

* Merge branch &#39;main&#39; into il-fix-hail ([`dba3fd9`](https://github.com/opentargets/genetics_etl_python/commit/dba3fd99f0c40e2cd4e7802aeee351112bd98894))

* Merge pull request #247 from opentargets/dependabot/pip/pytest-xdist-3.4.0

build(deps-dev): bump pytest-xdist from 3.3.1 to 3.4.0 ([`8cee2cd`](https://github.com/opentargets/genetics_etl_python/commit/8cee2cd7fc3d77b4d45534f8ed813058b01fa64f))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-run-l2g ([`e2f4d9a`](https://github.com/opentargets/genetics_etl_python/commit/e2f4d9a8a8accb97f5eea2aefc38f578402c84a4))

* Merge branch &#39;il-fix-hail&#39; of https://github.com/opentargets/genetics_etl_python into il-fix-hail ([`aa6088e`](https://github.com/opentargets/genetics_etl_python/commit/aa6088e5724ade4172a90a6d505f2dca5100424e))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-fix-hail ([`db3baef`](https://github.com/opentargets/genetics_etl_python/commit/db3baef7d056bf5dd729163a24bbd2103757efc6))

* Merge pull request #248 from opentargets/dependabot/pip/mkdocs-material-9.4.8

build(deps-dev): bump mkdocs-material from 9.4.7 to 9.4.8 ([`c392fe8`](https://github.com/opentargets/genetics_etl_python/commit/c392fe88a06c4e00760d6c48ca41d264d10c6b92))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-run-l2g ([`4690e15`](https://github.com/opentargets/genetics_etl_python/commit/4690e15bdaf6f9f375cc8693891fd8a7e88b67ba))

* Merge pull request #244 from opentargets/il-gwascat-clump

fix(gwas_catalog): clump associations, remove hail and style fixes ([`5a38980`](https://github.com/opentargets/genetics_etl_python/commit/5a38980bdaf85ce586c793c60de916bc5a8d338b))

* Merge branch &#39;main&#39; into il-fix-hail ([`18219b5`](https://github.com/opentargets/genetics_etl_python/commit/18219b5816124c0a629a35437450872c56abf700))

* Merge branch &#39;main&#39; into il-gwascat-clump ([`366f8ab`](https://github.com/opentargets/genetics_etl_python/commit/366f8abdfe1882cf0f3d3116989b4597a5da944c))

* Merge pull request #245 from opentargets/il-gnomad_matrix_slice-suggestions

feat(ld): minor improvements to gnomad matrix methods ([`4844c6d`](https://github.com/opentargets/genetics_etl_python/commit/4844c6d3b10fb6875af6cbc7554520c319c6d31f))

* Merge branch &#39;main&#39; into il-gnomad_matrix_slice-suggestions ([`980ca92`](https://github.com/opentargets/genetics_etl_python/commit/980ca92a27703727aa3fd7ce94cb87b01d8a312f))

* Merge pull request #240 from opentargets/ds_access_gnomad_matrix_slice

Access gnomad matrix slice with resolved variant identifiers ([`9e6f12a`](https://github.com/opentargets/genetics_etl_python/commit/9e6f12a01a3898a59146a737f66bf4b95c27aaf1))

* Merge branch &#39;main&#39; into ds_access_gnomad_matrix_slice ([`24c96d9`](https://github.com/opentargets/genetics_etl_python/commit/24c96d9759061377aa995c2f73a8a1c2eee6e8f8))

* Merge branch &#39;main&#39; into il-fix-hail ([`1eb5f7b`](https://github.com/opentargets/genetics_etl_python/commit/1eb5f7bb25c2a13844627d3431e51142d249c41e))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-fix-hail ([`60e58c3`](https://github.com/opentargets/genetics_etl_python/commit/60e58c33d4ff37b2e8f67202ac00072f570c4f0b))

* Merge pull request #243 from opentargets/il-fix-config-update

fix: extract config in root when we install deps on cluster ([`c584775`](https://github.com/opentargets/genetics_etl_python/commit/c5847753c4abe73d1fb1250d5c66aab0dc79efa8))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`4b19bab`](https://github.com/opentargets/genetics_etl_python/commit/4b19babf54a0a2b6f04e11b49cf222a542d9908b))

* Merge branch &#39;main&#39; into il-fix-config-update ([`9d6daa7`](https://github.com/opentargets/genetics_etl_python/commit/9d6daa79f5cdf274f6155621f3ad44ea9d92eec2))

* Merge pull request #241 from opentargets/do_hydra_full_error

feat: hydra full error on dataproc ([`f6e0a7e`](https://github.com/opentargets/genetics_etl_python/commit/f6e0a7e5062f4ebc31955914a435080fa912f61e))

* Merge branch &#39;main&#39; into do_hydra_full_error ([`3437648`](https://github.com/opentargets/genetics_etl_python/commit/3437648e64c9c8b52d50a0609123c8082ef0eb0e))

* Merge pull request #242 from opentargets/do_fix_python_module_path

fix: wrong python file uri in airflow ([`1b9a37b`](https://github.com/opentargets/genetics_etl_python/commit/1b9a37b88cedaee2e62e02900d463292f794d283))

* Merge branch &#39;main&#39; into ds_access_gnomad_matrix_slice ([`f33fa6c`](https://github.com/opentargets/genetics_etl_python/commit/f33fa6c2bc0d8e8f671175baf178619639a89e95))

* Merge pull request #226 from opentargets/tskir-3145-fix-hydra-instantiation

Simplify configuration and remove config store ([`68a77b3`](https://github.com/opentargets/genetics_etl_python/commit/68a77b38094b61d2c12d7b5d5536e8b9396856aa))

* Update config/step/session/local.yaml

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`d72bee1`](https://github.com/opentargets/genetics_etl_python/commit/d72bee123da5bb43c9757bc7e72516bbb4511a98))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-fix-hail ([`e90a43c`](https://github.com/opentargets/genetics_etl_python/commit/e90a43cb4848faf588ea6d3a9ba35c5de222cd95))

* Merge remote-tracking branch &#39;origin&#39; into ds_access_gnomad_matrix_slice ([`c73d967`](https://github.com/opentargets/genetics_etl_python/commit/c73d967745ac6c015935d5dc71cd64599649eaf8))

* Merge pull request #239 from opentargets/do_types_enhancements

Type enhancements ([`784d226`](https://github.com/opentargets/genetics_etl_python/commit/784d226a5f9b50013f5b8e87bfd03cb054b2a067))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-fix-hail ([`eddb928`](https://github.com/opentargets/genetics_etl_python/commit/eddb9288b6d232100f3f9b79ad272bc686c594a8))

* Merge pull request #236 from opentargets/ds_access_gnomad_matrix_slice

Small update in functionality to the GnomAD datasource toolset ([`a2a0209`](https://github.com/opentargets/genetics_etl_python/commit/a2a0209fc4cc34cd99538a095159323d70c22922))

* Merge pull request #235 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`2c883ff`](https://github.com/opentargets/genetics_etl_python/commit/2c883ffdccb4cbf79afc9e1d190ae0cc24b1c13d))

* Merge branch &#39;main&#39; into pre-commit-ci-update-config ([`8436065`](https://github.com/opentargets/genetics_etl_python/commit/84360655038fab45e868b1111a26e9e4b6957b16))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-fix-hail ([`14d3c02`](https://github.com/opentargets/genetics_etl_python/commit/14d3c02f88683decc52646cb5231596b8cf09abc))

* Merge pull request #234 from opentargets/do_gnomad_variants_within_class

GnomAD variants within class configuration ([`407890f`](https://github.com/opentargets/genetics_etl_python/commit/407890f23187797da497bc05db403cb01da5f367))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/astral-sh/ruff-pre-commit: v0.1.3 → v0.1.4](https://github.com/astral-sh/ruff-pre-commit/compare/v0.1.3...v0.1.4) ([`fe23397`](https://github.com/opentargets/genetics_etl_python/commit/fe2339751b35a8a92aeff9b744d3c633f4a368c7))

* Merge branch &#39;main&#39; into do_gnomad_variants_within_class ([`a22b537`](https://github.com/opentargets/genetics_etl_python/commit/a22b537cc7d14767020ab6f264567c99d9ea9557))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-fix-hail ([`01be7d0`](https://github.com/opentargets/genetics_etl_python/commit/01be7d0cc2ad48ae09b9332f80f7f5bd1b89dbb8))

* Merge pull request #233 from opentargets/dsildo_gnomadLD_withpaths

gnomAD LD datasource with paths within class ([`219e2c8`](https://github.com/opentargets/genetics_etl_python/commit/219e2c8473e496c7549cfa2aa78b53faf73928af))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-fix-hail ([`1ff533e`](https://github.com/opentargets/genetics_etl_python/commit/1ff533e7e7cea80d873678fd4c3a83b299df128b))

* Merge pull request #232 from opentargets/dependabot/pip/hail-0.2.126

build(deps): bump hail from 0.2.122 to 0.2.126 ([`2e9ef64`](https://github.com/opentargets/genetics_etl_python/commit/2e9ef64807943eed5c89375ffd12a6aafa00d511))

* Merge branch &#39;main&#39; into dependabot/pip/hail-0.2.126 ([`09790f7`](https://github.com/opentargets/genetics_etl_python/commit/09790f7e2e886e92c43b2dd449bec5933ca4a0f7))

* Merge pull request #227 from opentargets/tskir-3142-enable-autoscaling

Enable autoscaling, tune cluster parameters, add docs ([`be27312`](https://github.com/opentargets/genetics_etl_python/commit/be273123bcb567b3af6090bf633641ad5f5f39a2))

* Merge branch &#39;main&#39; into dependabot/pip/hail-0.2.126 ([`bad08aa`](https://github.com/opentargets/genetics_etl_python/commit/bad08aaaec3939aa88c2759297574d00d91b2039))

* Merge pull request #222 from opentargets/tskir-3143-2-fix-for-uid

Airflow set up, part 2: Fixing issue with setting `AIRFLOW_UID` ([`16f3478`](https://github.com/opentargets/genetics_etl_python/commit/16f3478ba6ce5bdefe6a891a6833c51fe558aeec))

* Merge pull request #230 from opentargets/dependabot/pip/apache-airflow-2.7.3

build(deps-dev): bump apache-airflow from 2.7.2 to 2.7.3 ([`9ec86ea`](https://github.com/opentargets/genetics_etl_python/commit/9ec86eafd322c292086d76e75191681e350fa158))

* Merge pull request #221 from opentargets/tskir-3143-1-style-changes

Airflow set up, part 1: Style updates for docs and DAG code ([`2efea7c`](https://github.com/opentargets/genetics_etl_python/commit/2efea7cb6697c84269bc38d5684cc2482dd559e7))

* Merge branch &#39;main&#39; into tskir-3143-1-style-changes ([`7181a27`](https://github.com/opentargets/genetics_etl_python/commit/7181a27da3a25573ff02978774619b44f30e261b))

* Merge pull request #215 from opentargets/do_l2g_feature_dataset

chore: move L2GFeature to datasets ([`0c953d1`](https://github.com/opentargets/genetics_etl_python/commit/0c953d1c04ba1b4e64a19bd150da08715d0cc78b))

* Merge branch &#39;main&#39; into do_l2g_feature_dataset ([`697d6e2`](https://github.com/opentargets/genetics_etl_python/commit/697d6e263212b26fea48cd69aac3b2ea86abe30f))

* Merge branch &#39;main&#39; into tskir-3143-1-style-changes ([`5e7bd04`](https://github.com/opentargets/genetics_etl_python/commit/5e7bd0485acf74606dd66ae56aa92c17a4d4a635))

* Merge pull request #228 from opentargets/dependabot/pip/pre-commit-3.5.0

build(deps-dev): bump pre-commit from 2.21.0 to 3.5.0 ([`1bbc4fd`](https://github.com/opentargets/genetics_etl_python/commit/1bbc4fd91bb0ab95ef5b4fd2b58d017d865ca059))

* Update src/airflow/dags/common_airflow.py

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`2fa5d93`](https://github.com/opentargets/genetics_etl_python/commit/2fa5d93c678a93d3802f86a4c57afba48b8ad60a))

* Merge pull request #224 from opentargets/il-preprocess-ldindex

feat(airflow): move `variant_annotation` and `ldindex` steps to preprocessing dag ([`7925c2f`](https://github.com/opentargets/genetics_etl_python/commit/7925c2f1912a565208ef2ae6418a666f38c6dd1c))

* Merge branch &#39;tskir-3140-better-dag-import-fix&#39; ([`4e3d966`](https://github.com/opentargets/genetics_etl_python/commit/4e3d966371ccfcaf4b77a305860cf6c919c837df))

* Merge pull request #216 from opentargets/tskir-3106-ingest-finngen

Implement Preprocess DAG following recent architecture updates ([`90e7d44`](https://github.com/opentargets/genetics_etl_python/commit/90e7d44f8e800aca2e172f30dc84fd28b290e892))

* Merge branch &#39;main&#39; into do_l2g_feature_dataset ([`3344772`](https://github.com/opentargets/genetics_etl_python/commit/334477244f4129d5cda9318eacd3b59fd3632e76))

* Merge pull request #220 from opentargets/tskir-3140-better-dag-import-fix

A better way to fix DAG import test ([`2014e48`](https://github.com/opentargets/genetics_etl_python/commit/2014e48aaaf879103fde6fb029ec82add0f35425))

* Merge pull request #219 from opentargets/il-fix-job-submission

fix(airflow): job args are list of strings ([`4f6d496`](https://github.com/opentargets/genetics_etl_python/commit/4f6d4969b72cb2a4c41463780532de0d87e28486))

* Merge pull request #218 from opentargets/tskir-3140-fix-dag-test

Re-enable and fix the `test_no_import_errors` Airflow DAG test ([`59cacdb`](https://github.com/opentargets/genetics_etl_python/commit/59cacdb8ec4321f53babd4a1bcfda4fc7b51e80f))

* Merge pull request #217 from opentargets/il-dag

chore(dag): add l2g and overlaps steps to `dag.yaml` ([`c9d4e70`](https://github.com/opentargets/genetics_etl_python/commit/c9d4e70519a99e831fb27f4e8ba8f5bdb71a0c41))

* Merge pull request #201 from opentargets/do_airflow_docker

Streamline Airflow in Docker for GCP configuration ([`9533cb8`](https://github.com/opentargets/genetics_etl_python/commit/9533cb8b7d2b9801e91d40cedf7be8ba2553be80))

* Merge pull request #214 from opentargets/il-airflow-docker

feat(airflow): change docker image to `apache/airflow:slim-latest-python3.9` and minor changes ([`452c8be`](https://github.com/opentargets/genetics_etl_python/commit/452c8bebaedc011c5a688883ef30dbe81ceeb69f))

* Merge pull request #67 from opentargets/il-l2g-prototype

L2G progress based on Hydra branch ([`fa9faf5`](https://github.com/opentargets/genetics_etl_python/commit/fa9faf577cf760fcd30a3608de8c7192384c67fc))

* Merge branch &#39;do_airflow_docker&#39; of https://github.com/opentargets/genetics_etl_python into il-airflow-docker ([`e05c0a7`](https://github.com/opentargets/genetics_etl_python/commit/e05c0a757aa550811ad7318d752c42917833bb1d))

* Update docs/development/airflow.md

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`95e8277`](https://github.com/opentargets/genetics_etl_python/commit/95e8277bd154c1c48a2ac176708bc00eee4fc66b))

* Merge branch &#39;do_airflow_docker&#39; of https://github.com/opentargets/genetics_etl_python into do_airflow_docker ([`5921ae3`](https://github.com/opentargets/genetics_etl_python/commit/5921ae37f9184dea0d4339a53bcf4be4c05a89d9))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into do_airflow_docker ([`3a4f483`](https://github.com/opentargets/genetics_etl_python/commit/3a4f483340e2b70c6d22fba20a8eb64294e538fd))

* Update src/airflow/Dockerfile

Co-authored-by: Kirill Tsukanov &lt;tskir@users.noreply.github.com&gt; ([`dc7a0a9`](https://github.com/opentargets/genetics_etl_python/commit/dc7a0a9136100cd31280af24aa44d54a7b239998))

* Update docs/development/airflow.md

Co-authored-by: Kirill Tsukanov &lt;tskir@users.noreply.github.com&gt; ([`44450b0`](https://github.com/opentargets/genetics_etl_python/commit/44450b0dcf629567e0ae52353d962021ca7e6a9a))

* Update docs/development/airflow.md

Co-authored-by: Kirill Tsukanov &lt;tskir@users.noreply.github.com&gt; ([`d924fe7`](https://github.com/opentargets/genetics_etl_python/commit/d924fe726f72123d3e134d204e4dd433e7142dce))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`7788eb6`](https://github.com/opentargets/genetics_etl_python/commit/7788eb63002fe4eb7b1fddf47da5b7cb26642a01))

* Merge pull request #205 from opentargets/dependabot/pip/mkdocs-minify-plugin-0.7.1

build(deps-dev): bump mkdocs-minify-plugin from 0.5.0 to 0.7.1 ([`18d068d`](https://github.com/opentargets/genetics_etl_python/commit/18d068d8380a5801114cb50891bd2017a00f24b7))

* Merge branch &#39;main&#39; into dependabot/pip/mkdocs-minify-plugin-0.7.1 ([`980c554`](https://github.com/opentargets/genetics_etl_python/commit/980c554237dce3e3aaf792eaf8341a5f1d348751))

* Merge pull request #213 from opentargets/do_docs_classnames

feat: display class name in docs and cleanup docs ([`769c810`](https://github.com/opentargets/genetics_etl_python/commit/769c8109d161faf25d6f98789a360471771d1da5))

* Merge branch &#39;main&#39; into do_docs_classnames ([`667b0ae`](https://github.com/opentargets/genetics_etl_python/commit/667b0ae6a3b7c35c5df3305b9d2c51b8268c03e4))

* Merge branch &#39;main&#39; into dependabot/pip/mkdocs-minify-plugin-0.7.1 ([`e51250e`](https://github.com/opentargets/genetics_etl_python/commit/e51250e4b32facfe87e89f0b750412ea889aea05))

* Merge pull request #211 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`0a4a734`](https://github.com/opentargets/genetics_etl_python/commit/0a4a734dd73038bee0babf0038eed95318f882d7))

* Merge branch &#39;do_docs_classnames&#39; of https://github.com/opentargets/genetics_etl_python into do_docs_classnames ([`3f4caae`](https://github.com/opentargets/genetics_etl_python/commit/3f4caae9c61cd4a2a4be51655ecea519da066fb5))

* Merge branch &#39;main&#39; into pre-commit-ci-update-config ([`31ed38f`](https://github.com/opentargets/genetics_etl_python/commit/31ed38fd6adb4245e62678c61352b007d6b3348e))

* Merge branch &#39;main&#39; into do_docs_classnames ([`d0cb0b1`](https://github.com/opentargets/genetics_etl_python/commit/d0cb0b198cedee9d480e82d675f515ce01d89279))

* Merge pull request #212 from opentargets/il-docstring-linter

feat: use `pydoclint` as a documentation linter ([`6b2fc53`](https://github.com/opentargets/genetics_etl_python/commit/6b2fc5349e014b3ee103445000d32718f753cc5b))

* Revert &#34;feat: configure pydoclint to require return docstring when returning nothing&#34;

This reverts commit 116ff4fa451e36c17caeec7bad0006e662bf92db. ([`ffcb0f6`](https://github.com/opentargets/genetics_etl_python/commit/ffcb0f6bf9a7d4a0177729ac12366568d4161d8d))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/astral-sh/ruff-pre-commit: v0.1.2 → v0.1.3](https://github.com/astral-sh/ruff-pre-commit/compare/v0.1.2...v0.1.3) ([`3229d5b`](https://github.com/opentargets/genetics_etl_python/commit/3229d5bcf18cc7aa5d971966e92b5eb7df198e2e))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-docstring-linter ([`5acafea`](https://github.com/opentargets/genetics_etl_python/commit/5acafea6a8828ba3c1b5c5a353d2bbddfedf50f8))

* Merge branch &#39;il-l2g-prototype&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`84f402b`](https://github.com/opentargets/genetics_etl_python/commit/84f402b7a3e9bd49631de6baf1901fc666627199))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`d4c9336`](https://github.com/opentargets/genetics_etl_python/commit/d4c93369c65923fc96ede2ceb0886f612e9130cc))

* Merge pull request #200 from opentargets/tskir-3132-follow-up

Follow up changes to step configuration refactor ([`8525981`](https://github.com/opentargets/genetics_etl_python/commit/8525981faf079416f166e9184043b5699a9e288e))

* Merge branch &#39;tskir-3132-follow-up&#39; of https://github.com/opentargets/genetics_etl_python into tskir-3132-follow-up ([`6cc4e1e`](https://github.com/opentargets/genetics_etl_python/commit/6cc4e1e995016c361f1824e7686bf6d5917e856c))

* Merge branch &#39;main&#39; into tskir-3132-follow-up ([`5763f12`](https://github.com/opentargets/genetics_etl_python/commit/5763f122c998e88a453399f130d04149bf504eda))

* Merge pull request #210 from opentargets/il-l2g-spark

feat(session): start session inside step instance ([`ef2bd4a`](https://github.com/opentargets/genetics_etl_python/commit/ef2bd4a4f8f67ca58e210a7ecf6d3297e86a95ce))

* Merge pull request #204 from opentargets/dependabot/pip/mkdocs-material-9.4.7

build(deps-dev): bump mkdocs-material from 9.4.6 to 9.4.7 ([`d12cd15`](https://github.com/opentargets/genetics_etl_python/commit/d12cd15964e105a8f8cc2edbdfc3992e6a73ee8e))

* Merge pull request #206 from opentargets/dependabot/pip/ruff-0.1.3

build(deps-dev): bump ruff from 0.0.287 to 0.1.3 ([`26fbbc2`](https://github.com/opentargets/genetics_etl_python/commit/26fbbc28f8c3fa1354e5f28f619f9012587e6d2a))

* Merge branch &#39;main&#39; into dependabot/pip/ruff-0.1.3 ([`89d4ee4`](https://github.com/opentargets/genetics_etl_python/commit/89d4ee4ebb99d0bcc226829b37adb75a61a0a47d))

* Merge pull request #209 from opentargets/ds_adding_notebooks

feat: adding notebooks for finngen and ld matrix ([`60043c8`](https://github.com/opentargets/genetics_etl_python/commit/60043c8d5f1b9c9e5758cb9b632ff276c3be67b9))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`3729231`](https://github.com/opentargets/genetics_etl_python/commit/372923178af9a74669f4241b3a09adb3f3ac9962))

* Merge branch &#39;main&#39; into do_airflow_docker ([`7531775`](https://github.com/opentargets/genetics_etl_python/commit/7531775fa18d1d4e689eb0d765cfe36501255268))

* Merge pull request #208 from opentargets/dependabot/docker/src/airflow/apache/airflow-2.7.2-python3.10

build(deps): bump apache/airflow from 2.7.1-python3.10 to 2.7.2-python3.10 in /src/airflow ([`ddcd0ac`](https://github.com/opentargets/genetics_etl_python/commit/ddcd0ace4111f94e9a96db97a8f8cc341b056324))

* Merge pull request #202 from opentargets/do_dependabot_docker

Dependabot to monitor airflow docker ([`68b7770`](https://github.com/opentargets/genetics_etl_python/commit/68b777079c0a6d7cef70b894c78f00a7846bdbbe))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`6fe09c2`](https://github.com/opentargets/genetics_etl_python/commit/6fe09c235101a1753ffe8b9216f50b7e44a53fbd))

* Merge pull request #199 from opentargets/tskir-3132-step-configuration

Refactor step configuration ([`36f89f3`](https://github.com/opentargets/genetics_etl_python/commit/36f89f3698cb87bb93bbb0c408493830f70a9197))

* Revert &#34;feat(study_index): harmonise all study indices under common path&#34;

This reverts commit cd81547c7abc5e6e73303f15ae66840c4189410d. ([`2b3787f`](https://github.com/opentargets/genetics_etl_python/commit/2b3787fd3648910599d271fd527432a16693820b))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`26f877a`](https://github.com/opentargets/genetics_etl_python/commit/26f877ad0d264e4d7d10698b422077739050d1e2))

* revert 597b13b55d0cc3121aca0d65dcee5f5019ecf15c ([`ef7d505`](https://github.com/opentargets/genetics_etl_python/commit/ef7d5050e6c4c5989321e7a1a3bd5ae03d78b2d1))

* Merge pull request #136 from opentargets/il-spark-conf

feat: adapt session to extend the spark env configuration + consolidate testing env ([`5298e14`](https://github.com/opentargets/genetics_etl_python/commit/5298e140b888cf55b00a03f42ddec5f4e713ce68))

* Merge branch &#39;main&#39; into il-spark-conf ([`71e2d87`](https://github.com/opentargets/genetics_etl_python/commit/71e2d87530e12b1f1008af358228cbcb99c6ee9c))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`9d32568`](https://github.com/opentargets/genetics_etl_python/commit/9d325682ae3b7fe0039338e66a45f543c2ec5a88))

* Update src/otg/common/session.py

Co-authored-by: Kirill Tsukanov &lt;tskir@users.noreply.github.com&gt; ([`8fcb324`](https://github.com/opentargets/genetics_etl_python/commit/8fcb3241ecaec1ffbd92cd1d3947113046006e63))

* Update src/utils/spark.py

Co-authored-by: Kirill Tsukanov &lt;tskir@users.noreply.github.com&gt; ([`2db83fd`](https://github.com/opentargets/genetics_etl_python/commit/2db83fdf473aa5e980dbe8e7f798ee5367d001c8))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`781d601`](https://github.com/opentargets/genetics_etl_python/commit/781d60166e19fbaf02f8435da651fbbd4d7d6730))

* Merge pull request #166 from opentargets/tskir-3104-airflow-refactor-common

[Preprocess #3] Refactor existing Airflow implementation ([`0e4e2ff`](https://github.com/opentargets/genetics_etl_python/commit/0e4e2ff34a08d6981b040923b7717a04acf72f8b))

* Merge pull request #164 from opentargets/tskir-3102-airflow-set-up

 [Preprocess #2] Instructions for setting up Airflow ([`99a662a`](https://github.com/opentargets/genetics_etl_python/commit/99a662a8de8e03614d0f649a9808c1e919236419))

* Merge branch &#39;il-spark-conf&#39; of https://github.com/opentargets/genetics_etl_python into il-spark-conf ([`7c82281`](https://github.com/opentargets/genetics_etl_python/commit/7c8228104436e055af25f8660a5897ecde756207))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-spark-conf ([`117267c`](https://github.com/opentargets/genetics_etl_python/commit/117267c6f34147046ff79467aa5d08a23b8d7c50))

* Update docs/development/airflow.md

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`b130ef6`](https://github.com/opentargets/genetics_etl_python/commit/b130ef685c390b5988cdbaffe2afa3fa05ac0bc0))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`9a225c2`](https://github.com/opentargets/genetics_etl_python/commit/9a225c2c22d6a70f3097bdba45081a5ef29b826a))

* Merge pull request #194 from opentargets/dependabot/pip/apache-airflow-providers-google-10.10.1

build(deps-dev): bump apache-airflow-providers-google from 10.10.0 to 10.10.1 ([`fe74705`](https://github.com/opentargets/genetics_etl_python/commit/fe74705f4522af0f39d9ef7102a7fb950c468421))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`d14b40e`](https://github.com/opentargets/genetics_etl_python/commit/d14b40e39d1666ea291c98db03bb59fb1975759c))

* Merge pull request #191 from opentargets/hn-data-docs

Added UKBB and open targets data source documentation ([`0c2da89`](https://github.com/opentargets/genetics_etl_python/commit/0c2da891288c9c1086822a9ac744b8c76c40b333))

* Merge branch &#39;main&#39; into hn-data-docs ([`6499293`](https://github.com/opentargets/genetics_etl_python/commit/6499293b0fa1dc6129d1af3f387a2fccb347fd27))

* Merge pull request #196 from opentargets/dependabot/pip/mkdocs-autolinks-plugin-0.7.1

build(deps-dev): bump mkdocs-autolinks-plugin from 0.6.0 to 0.7.1 ([`0ed0a3d`](https://github.com/opentargets/genetics_etl_python/commit/0ed0a3d0f44aad6dee2d814775cc47cb6cdbd09e))

* Merge branch &#39;main&#39; into hn-data-docs ([`acecffa`](https://github.com/opentargets/genetics_etl_python/commit/acecffabef357866c9aaf536b2be08326a9b7ae6))

* Merge pull request #197 from opentargets/dependabot/pip/mypy-1.6.1

build(deps-dev): bump mypy from 0.971 to 1.6.1 ([`0cf8892`](https://github.com/opentargets/genetics_etl_python/commit/0cf8892f2b7dd9ff7bab1b7b59129a919752f51a))

* Merge branch &#39;main&#39; into dependabot/pip/mypy-1.6.1 ([`b954a51`](https://github.com/opentargets/genetics_etl_python/commit/b954a510356f047b687f9ee25644e76879ceb3ca))

* Merge pull request #190 from opentargets/il-run-v2g

feat: run variant to gene step ([`ed221f0`](https://github.com/opentargets/genetics_etl_python/commit/ed221f0f93997af7a6c578399b65360ba6f94224))

* Merge branch &#39;main&#39; into dependabot/pip/mypy-1.6.1 ([`9902d24`](https://github.com/opentargets/genetics_etl_python/commit/9902d2490f30533e1849a6d1089d26a6bab6c526))

* Merge branch &#39;main&#39; into il-run-v2g ([`0881f66`](https://github.com/opentargets/genetics_etl_python/commit/0881f666034533bf7f14cb788af8720f65418e13))

* Merge pull request #198 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`e07e58d`](https://github.com/opentargets/genetics_etl_python/commit/e07e58decd7ec70526e1c63b8348b11266ff01dc))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/astral-sh/ruff-pre-commit: v0.1.0 → v0.1.1](https://github.com/astral-sh/ruff-pre-commit/compare/v0.1.0...v0.1.1)
- [github.com/psf/black: 23.10.0 → 23.10.1](https://github.com/psf/black/compare/23.10.0...23.10.1) ([`a063261`](https://github.com/opentargets/genetics_etl_python/commit/a0632610e4eedc0b09bf79d6d687b87098cb278e))

* Merge pull request #195 from opentargets/dependabot/pip/pyspark-3.3.3

build(deps): bump pyspark from 3.3.0 to 3.3.3 ([`8905f80`](https://github.com/opentargets/genetics_etl_python/commit/8905f80cdab041688bbca4f528846b8b4b1bdc46))

* Merge pull request #192 from opentargets/il-docs-fix

fix(docs): schemas path for gene and study index ([`ea2ffb7`](https://github.com/opentargets/genetics_etl_python/commit/ea2ffb7585f50f71ded70440bcf692f12515dd92))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`0992fc4`](https://github.com/opentargets/genetics_etl_python/commit/0992fc416cd4077950b99f67984813baa4253c92))

* Merge branch &#39;il-run-v2g&#39; of https://github.com/opentargets/genetics_etl_python into il-run-v2g ([`4efb452`](https://github.com/opentargets/genetics_etl_python/commit/4efb452d8c0297118cede45d4b73df7b61fddc59))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`1d8ddda`](https://github.com/opentargets/genetics_etl_python/commit/1d8dddaf9600254212e77dddbaec7865863ca03c))

* Merge branch &#39;main&#39; into il-run-v2g ([`941ca6d`](https://github.com/opentargets/genetics_etl_python/commit/941ca6dc3752f1ff2eaa0d192125fe659a6318c0))

* Revert &#34;fix(v2g): indicate schema&#34;

This reverts commit b2f0d0fb99c0cd4dcd5b75e94e85317c514c0866. ([`80c6379`](https://github.com/opentargets/genetics_etl_python/commit/80c63799194c344eb1aea60802526d31bb160775))

* Merge pull request #188 from opentargets/il-variant-index

feat: run variant index step ([`c021e6b`](https://github.com/opentargets/genetics_etl_python/commit/c021e6b538bed320bfba5a3b07d4fe5b3e5fc4f6))

* Merge branch &#39;main&#39; into il-variant-index ([`2e812f5`](https://github.com/opentargets/genetics_etl_python/commit/2e812f58ca35214f1268675f86041f6df9fa3525))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-run-v2g ([`d8ba13f`](https://github.com/opentargets/genetics_etl_python/commit/d8ba13f9071fa44670278d3ba8ec2cc7d006ef3b))

* Merge pull request #189 from opentargets/il-geneindex-missing-cols

Add missing columns in gene index ([`6389ff1`](https://github.com/opentargets/genetics_etl_python/commit/6389ff172f8ff3ee487d624fff48bb9e8a4fce57))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-variant-index ([`c7a297d`](https://github.com/opentargets/genetics_etl_python/commit/c7a297dbf1624ba3a994f7e1a7f717dd5ecb3a43))

* Merge pull request #180 from opentargets/dependabot/github_actions/actions/cache-3

chore(deps): bump actions/cache from 2 to 3 ([`e26f2c4`](https://github.com/opentargets/genetics_etl_python/commit/e26f2c4cdddce8d5c34bf7c7a34ed53bcbf9f977))

* Merge branch &#39;main&#39; into dependabot/github_actions/actions/cache-3 ([`31959cb`](https://github.com/opentargets/genetics_etl_python/commit/31959cb9950c1f0c0ecabcad07bad0fe144dd3f2))

* Merge pull request #183 from opentargets/dependabot/pip/apache-airflow-providers-google-10.10.0

build(deps-dev): bump apache-airflow-providers-google from 10.6.0 to 10.10.0 ([`c9591ae`](https://github.com/opentargets/genetics_etl_python/commit/c9591aefc88a8561b0184d34f846c48d6ceaf72c))

* Merge pull request #173 from opentargets/do_update

bumping dependency versions ([`33a5dac`](https://github.com/opentargets/genetics_etl_python/commit/33a5daccd3216a2f028cffe4235a15b7eae75591))

* Merge branch &#39;main&#39; into do_update ([`b860cf4`](https://github.com/opentargets/genetics_etl_python/commit/b860cf463c1022bbaafa64b0d4acf48410fe443b))

* Merge pull request #181 from opentargets/dependabot/github_actions/actions/setup-python-4

chore(deps): bump actions/setup-python from 2 to 4 ([`30875f1`](https://github.com/opentargets/genetics_etl_python/commit/30875f1a784044155c614edf1c8b0af43c2b9fe0))

* Merge branch &#39;main&#39; into do_update ([`292367c`](https://github.com/opentargets/genetics_etl_python/commit/292367ceb435a1599f22eb460a76051394d8c317))

* Merge branch &#39;main&#39; into dependabot/github_actions/actions/cache-3 ([`0aef06a`](https://github.com/opentargets/genetics_etl_python/commit/0aef06a53f4ce290fe34d44ef23080e2862ead22))

* Merge pull request #182 from opentargets/dependabot/github_actions/actions/checkout-4

chore(deps): bump actions/checkout from 2 to 4 ([`b8347d5`](https://github.com/opentargets/genetics_etl_python/commit/b8347d52aeeda91956a2d03c3462ca7be4ebc5ea))

* Merge pull request #177 from opentargets/do_dependabot

Github dependabot ([`bbe5343`](https://github.com/opentargets/genetics_etl_python/commit/bbe5343ba73cdb691a0ac19715b47b5fa664d3e5))

* Merge branch &#39;main&#39; into do_dependabot ([`5393384`](https://github.com/opentargets/genetics_etl_python/commit/539338408ba659729f668cf6a6c6e1da4e460267))

* Merge pull request #176 from opentargets/do_action_poetry_packages

Revert changes in main not to use actions ([`dc028db`](https://github.com/opentargets/genetics_etl_python/commit/dc028dbc01d4f14362b67e130fd6bf2881574780))

* Merge branch &#39;main&#39; into do_action_poetry_packages ([`926116b`](https://github.com/opentargets/genetics_etl_python/commit/926116b71f18775638ecbdf1ae534eee67386151))

* Merge pull request #179 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`409cb06`](https://github.com/opentargets/genetics_etl_python/commit/409cb06ac676526050d57b2468237011245b1972))

* Merge branch &#39;main&#39; into pre-commit-ci-update-config ([`1c6ca8d`](https://github.com/opentargets/genetics_etl_python/commit/1c6ca8de26498337b61b43c1b738fd8637abc2ab))

* Merge pull request #178 from opentargets/do_docs_imgs

issue with docs images ([`4615151`](https://github.com/opentargets/genetics_etl_python/commit/461515137a8aaddd3fc7390da8030f55cc98d27b))

* Merge branch &#39;main&#39; into do_docs_imgs ([`06e0586`](https://github.com/opentargets/genetics_etl_python/commit/06e058636a522be95616fc8b314127a18904ab8b))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/hadialqattan/pycln: v2.2.2 → v2.3.0](https://github.com/hadialqattan/pycln/compare/v2.2.2...v2.3.0)
- [github.com/pre-commit/mirrors-mypy: v1.5.1 → v1.6.0](https://github.com/pre-commit/mirrors-mypy/compare/v1.5.1...v1.6.0) ([`9e09394`](https://github.com/opentargets/genetics_etl_python/commit/9e09394a2c2b2f9bbd1e048f4b2e627b73e4d1c7))

* Merge branch &#39;main&#39; into do_update ([`a8ffa3e`](https://github.com/opentargets/genetics_etl_python/commit/a8ffa3e59faeaf348389443156791cf1c59534ec))

* revert: dependency errors ([`7a4530b`](https://github.com/opentargets/genetics_etl_python/commit/7a4530bf319922f7904950212225646954772a66))

* Merge pull request #175 from opentargets/do_action_poetry_packages

feat: changing the action ([`8d4a9a2`](https://github.com/opentargets/genetics_etl_python/commit/8d4a9a25496f287d0555a256db1bfb3f54deb24e))

* Merge pull request #174 from opentargets/do_action_poetry_packages

automatically PR to bump poetry packages ([`98b4f3b`](https://github.com/opentargets/genetics_etl_python/commit/98b4f3ba6944db3c87dff18f51337ed3eeb5fb1e))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`33019d6`](https://github.com/opentargets/genetics_etl_python/commit/33019d611aca1c4b2b02d4f004061b46ffd6b1df))

* Merge pull request #172 from opentargets/do_docs_imgs

Documentation structural changes and data source images ([`e1ab18e`](https://github.com/opentargets/genetics_etl_python/commit/e1ab18ee5b9d2624e417bc5c7e766f71ed0fd8a3))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`f73cddd`](https://github.com/opentargets/genetics_etl_python/commit/f73cddd76802803c441165670db23b0aa504f69a))

* Merge branch &#39;main&#39; into do_docs_imgs ([`da9fa7c`](https://github.com/opentargets/genetics_etl_python/commit/da9fa7c86ad885a063cf20eacce56c56c5d2b224))

* Merge pull request #170 from opentargets/do_docstring_fixes

docs: several fixes in docstrings ([`dc5d2b6`](https://github.com/opentargets/genetics_etl_python/commit/dc5d2b68d541e7cf44a594870822de0b09832cf6))

* Merge branch &#39;main&#39; into do_docstring_fixes ([`b87f2e5`](https://github.com/opentargets/genetics_etl_python/commit/b87f2e5a841c994abc7f7f625ffabb6124a66908))

* Merge pull request #169 from opentargets/ds_3117_refactor_clumping

Ds 3117 refactor clumping ([`cc1f636`](https://github.com/opentargets/genetics_etl_python/commit/cc1f6361b4be58bd960e5dce3dae3c6815f64b08))

* Merge branch &#39;main&#39; into il-l2g-prototype ([`c328368`](https://github.com/opentargets/genetics_etl_python/commit/c3283686b1d38d38285b1448e318d74f3d7eef0a))

* Merge branch &#39;il-l2g-prototype&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`35d83f1`](https://github.com/opentargets/genetics_etl_python/commit/35d83f1acbc41994c3835e4037c0cbb9f0c7dc29))

* Merge pull request #150 from opentargets/tskir-3095-finngen-sumstat

[Preprocess #1] Business logic for FinnGen summary stats ingestion ([`f8ed420`](https://github.com/opentargets/genetics_etl_python/commit/f8ed42042d55d0c579a34860222e6d9c4756b791))

* Merge pull request #165 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`f02ae41`](https://github.com/opentargets/genetics_etl_python/commit/f02ae4119d6adbd1d27c572d17168011dfeb646d))

* Merge branch &#39;do_hydra&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`180187d`](https://github.com/opentargets/genetics_etl_python/commit/180187db5d2e276aad35a351e60d6e9f6032e32a))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/pre-commit/pre-commit-hooks: v4.4.0 → v4.5.0](https://github.com/pre-commit/pre-commit-hooks/compare/v4.4.0...v4.5.0) ([`1a3d6b1`](https://github.com/opentargets/genetics_etl_python/commit/1a3d6b1a1777691cf01b672275b1b6101cd18a92))

* Merge pull request #162 from opentargets/do_ifwhenissue ([`4bbc983`](https://github.com/opentargets/genetics_etl_python/commit/4bbc983fd98dbca90343d30cde233dde4054e58f))

* Merge branch &#39;main&#39; into do_ifwhenissue ([`07a528c`](https://github.com/opentargets/genetics_etl_python/commit/07a528c5e057b77f1c1f16e18c300d5a9fbe6738))

* Merge pull request #160 from opentargets/do_ifwhenissue

fix: schema issues due to when condition ([`92abf53`](https://github.com/opentargets/genetics_etl_python/commit/92abf53b2e0269d9a433246093e5a9c1abc41758))

* Merge branch &#39;main&#39; into do_ifwhenissue ([`9d59706`](https://github.com/opentargets/genetics_etl_python/commit/9d59706df5cc3e7764a0cebb45c4b9bf1189e3d6))

* Merge pull request #161 from opentargets/dc16

fix: Removing old gsutil cp command ([`0c84984`](https://github.com/opentargets/genetics_etl_python/commit/0c84984ac456a6fcb251e753d088d81eabd89fbd))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into dc16 ([`ff9bb16`](https://github.com/opentargets/genetics_etl_python/commit/ff9bb160444e35623f55b786bcf121c35f15ad9a))

* Merge pull request #159 from opentargets/do_credible_set_annotation

Enhancements around credible set annotation ([`6cf098d`](https://github.com/opentargets/genetics_etl_python/commit/6cf098d86e55ec6eb896fb90a1d06bb06ee72ca9))

* revert: study-locus class ([`c027ace`](https://github.com/opentargets/genetics_etl_python/commit/c027ace7bfc393cc3ddf647e5fbe5b852994aedb))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into dc16 ([`0aaba54`](https://github.com/opentargets/genetics_etl_python/commit/0aaba54a96e32799143d9fbe96d518bd0a4c6be5))

* Merge pull request #157 from opentargets/ds_failing_LD_annotation

Some studies just don&#39;t have population data causing LD annotation fail ([`17578ec`](https://github.com/opentargets/genetics_etl_python/commit/17578ec96f3ed3ff0e3d69de322d29a746a764f8))

* revert: pre-commit ([`0e30cf4`](https://github.com/opentargets/genetics_etl_python/commit/0e30cf459ff38ac07fa7fe6568a48b1e65ea8b09))

* Merge pull request #158 from opentargets/main

merge with main ([`ee0bfdb`](https://github.com/opentargets/genetics_etl_python/commit/ee0bfdb2d11c52c1e6f4aa14bf0b8c878458159d))

* Merge pull request #155 from opentargets/ds_3100_bug_in_clumping

fix: cleaning clumping ([`af7729a`](https://github.com/opentargets/genetics_etl_python/commit/af7729ae132da119a52946d75747ee0b3985c04a))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into ds_3100_bug_in_clumping ([`cfa4bc4`](https://github.com/opentargets/genetics_etl_python/commit/cfa4bc47cfaa46f620a9849a242f205e469d18fc))

* Merge pull request #156 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`7622036`](https://github.com/opentargets/genetics_etl_python/commit/76220363d82c3e33e93a86f88b44512cc61d294e))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/astral-sh/ruff-pre-commit: v0.0.291 → v0.0.292](https://github.com/astral-sh/ruff-pre-commit/compare/v0.0.291...v0.0.292) ([`92c4bec`](https://github.com/opentargets/genetics_etl_python/commit/92c4becf927b69e5c49509abcfbe82c6f844943a))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into ds_3100_bug_in_clumping ([`b1a0b27`](https://github.com/opentargets/genetics_etl_python/commit/b1a0b2796926a0ee5409085a9f317ffba258d9f0))

* Merge pull request #154 from opentargets/do_cleandependency

chore: unused dependency ([`3f8b1a3`](https://github.com/opentargets/genetics_etl_python/commit/3f8b1a32bc1fb959217739db1d102cb7825f236a))

* Merge pull request #152 from opentargets/do_ldpics

Various refactors and bug fixes around ld annotation and pics ([`8b8c2ee`](https://github.com/opentargets/genetics_etl_python/commit/8b8c2ee1eb02d7ad09f976cad0509fb2c617b672))

* Merge branch &#39;main&#39; into do_ldpics ([`4534b8b`](https://github.com/opentargets/genetics_etl_python/commit/4534b8b6789c6456a6ff51ec61fa065c70fc3990))

* Merge pull request #153 from opentargets/ds_3097_making_windows_flexible

feat: flexible window set up for locus collection ([`06596d6`](https://github.com/opentargets/genetics_etl_python/commit/06596d654618c64119008d2c01ec5fc7f64a8c53))

* Merge pull request #151 from opentargets/main

Merge branch with main ([`4254833`](https://github.com/opentargets/genetics_etl_python/commit/4254833fa2b059e68b58d451f9f50f1e21c63c17))

* Merge pull request #149 from opentargets/ds_3094_generalizing_population_mapping

Generalizing population mapping ([`f60e720`](https://github.com/opentargets/genetics_etl_python/commit/f60e7205ea1c4306b85099e9fcb4e149b3ec5e1d))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python ([`0c748b3`](https://github.com/opentargets/genetics_etl_python/commit/0c748b3a6b1667dfd5faf990e4f96a717282857a))

* Merge pull request #141 from opentargets/do_datasources

Externalise data sources ([`d23b57a`](https://github.com/opentargets/genetics_etl_python/commit/d23b57a54561cd8851fe1e9ed5292bfd5af775b0))

* Merge branch &#39;main&#39; into do_datasources ([`ea3b51e`](https://github.com/opentargets/genetics_etl_python/commit/ea3b51e75ae0ca525e8b70013f31d3e4341c9677))

* Merge branch &#39;main&#39; into il-spark-conf ([`3401cd5`](https://github.com/opentargets/genetics_etl_python/commit/3401cd517f4e71e48d4c26734fdb6294c7e72334))

* Merge pull request #142 from opentargets/gwas_update

fix: updating GWAS Catalog input files ([`ace6253`](https://github.com/opentargets/genetics_etl_python/commit/ace6253a0cc5e3ac23d271b6b58d8bb1dee66752))

* Merge branch &#39;main&#39; into do_datasources ([`0f6b945`](https://github.com/opentargets/genetics_etl_python/commit/0f6b945be4b2d2face934df199da2f6ca96d07a3))

* Merge branch &#39;main&#39; into gwas_update ([`56ca046`](https://github.com/opentargets/genetics_etl_python/commit/56ca04609dc7e7941c72899ef4aed93aeee71349))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python ([`d1edad6`](https://github.com/opentargets/genetics_etl_python/commit/d1edad6ecfd00bcab62a338adc53eb3d9525d06c))

* Merge pull request #143 from opentargets/il-ld-annotator

Correct formula for PICS standard deviation (duplicated) ([`0c115c5`](https://github.com/opentargets/genetics_etl_python/commit/0c115c54cfee410f184abc8331adf2ed97963685))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-ld-annotator ([`2e56cc2`](https://github.com/opentargets/genetics_etl_python/commit/2e56cc21ae61c58d9e31ebbfee45399f0c1375e8))

* Merge pull request #145 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`3d64a49`](https://github.com/opentargets/genetics_etl_python/commit/3d64a496cc489b97623bc9e5033f81107bd6a4d0))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/astral-sh/ruff-pre-commit: v0.0.290 → v0.0.291](https://github.com/astral-sh/ruff-pre-commit/compare/v0.0.290...v0.0.291) ([`90b14d3`](https://github.com/opentargets/genetics_etl_python/commit/90b14d3cb46a13884f264fcb6cfb8a877fa76ee7))

* Merge branch &#39;main&#39; into il-ld-annotator ([`738f035`](https://github.com/opentargets/genetics_etl_python/commit/738f035d4bc69345a45f8e607a568a29a0f1bf82))

* Merge pull request #113 from opentargets/il-pics-fix

Correct formula for PICS standard deviation ([`15909a9`](https://github.com/opentargets/genetics_etl_python/commit/15909a95e72ae8886f717045ca349905a1059372))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`cea22d1`](https://github.com/opentargets/genetics_etl_python/commit/cea22d1c64f7d35bb04c4a38e86ba3014494dd1e))

* Merge branch &#39;main&#39; into il-spark-conf ([`ade45ae`](https://github.com/opentargets/genetics_etl_python/commit/ade45ae6bc58981e53755a8dd8f5573575ac18cb))

* Merge pull request #128 from opentargets/ds_clustering_w_carry_over

Window based clustering with carrying over locus around the semi indices ([`11b17bf`](https://github.com/opentargets/genetics_etl_python/commit/11b17bfed5438f659d54a4967348ccd7726f8942))

* Merge branch &#39;ds_clustering_w_carry_over&#39; of https://github.com/opentargets/genetics_etl_python into ds_clustering_w_carry_over ([`48c6aa2`](https://github.com/opentargets/genetics_etl_python/commit/48c6aa2eeef1bd075e6ac4f41e0d23efe39448a7))

* Merge remote-tracking branch &#39;origin&#39; into ds_clustering_w_carry_over ([`96f0adb`](https://github.com/opentargets/genetics_etl_python/commit/96f0adbe84a4ff80040bc3b1d6c392995a2905ec))

* Merge branch &#39;main&#39; into ds_clustering_w_carry_over ([`b9b7bae`](https://github.com/opentargets/genetics_etl_python/commit/b9b7bae3db46611d1d5d6d81fd6b29c87d872e4d))

* Apply suggestions from code review

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`2cfa165`](https://github.com/opentargets/genetics_etl_python/commit/2cfa1651b1c1b5bd99d9f9a893f6c6d9ddf94411))

* Merge branch &#39;main&#39; into il-spark-conf ([`f1901aa`](https://github.com/opentargets/genetics_etl_python/commit/f1901aa47d7eff572eec1d19958786d3f7577414))

* Merge pull request #135 from opentargets/do_camelcase_schemas

feat: ensure schema columns are camelcase ([`edf2ba4`](https://github.com/opentargets/genetics_etl_python/commit/edf2ba490f775c59c775f39bbbc3f7bf54349ece))

* Merge branch &#39;main&#39; into ds_clustering_w_carry_over ([`c83f947`](https://github.com/opentargets/genetics_etl_python/commit/c83f9478b3de08ef4261b9e7322a2b0b8e9a3ed4))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into do_camelcase_schemas ([`63c284a`](https://github.com/opentargets/genetics_etl_python/commit/63c284a7647d1f24314759dff90fdaefc5877d71))

* Merge pull request #139 from opentargets/il-fixes

revert: remove airflow checkpoint ([`35f79c7`](https://github.com/opentargets/genetics_etl_python/commit/35f79c70c6a8423d34dd5baab79983805ddc0a93))

* Revert &#34;chore: work in progress&#34;

This reverts commit 720fff566f6485fea5b97af8117165984e489b7c. ([`5a405cc`](https://github.com/opentargets/genetics_etl_python/commit/5a405cc69582690a1ee2b80720abbe694670b714))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-fixes ([`be80d0e`](https://github.com/opentargets/genetics_etl_python/commit/be80d0efdc072dcfbcaef50abffcb1e546966563))

* Merge remote-tracking branch &#39;origin&#39; into ds_clustering_w_carry_over ([`4742ace`](https://github.com/opentargets/genetics_etl_python/commit/4742ace4419e79dbc2be526c6b141cc53001777e))

* Merge pull request #137 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`cb02bc3`](https://github.com/opentargets/genetics_etl_python/commit/cb02bc3e9a59f2b3f15118e37b414f24e39297a0))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/astral-sh/ruff-pre-commit: v0.0.288 → v0.0.290](https://github.com/astral-sh/ruff-pre-commit/compare/v0.0.288...v0.0.290) ([`0752d12`](https://github.com/opentargets/genetics_etl_python/commit/0752d125598699d38c366f4fceff748456b3d3f1))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-spark-conf ([`e69d12b`](https://github.com/opentargets/genetics_etl_python/commit/e69d12bc74dd82582e364f12a98313db5b06f590))

* Merge pull request #134 from opentargets/do_renametags

Rename locus columns (no tags) ([`46f8fc7`](https://github.com/opentargets/genetics_etl_python/commit/46f8fc72baa5f0d03bc21376706dc1bd1072be9c))

* Merge pull request #133 from opentargets/do_fixdocs

fix: missing dataset schemas do to wrong hook setup ([`f3354a4`](https://github.com/opentargets/genetics_etl_python/commit/f3354a4b2557824d2b5f804154082053c4b59374))

* Merge pull request #132 from opentargets/do_xdist

Pytest parallelisation using pytest-xdist ([`7ce0653`](https://github.com/opentargets/genetics_etl_python/commit/7ce0653242a2fe7f0f53a901201ba8eb31bf155b))

* revert: this can cause the codecov.xml to be removed which is required for certain use cases ([`32cc25a`](https://github.com/opentargets/genetics_etl_python/commit/32cc25a2edb6a039732b3bbeb2cc12552b20dd2e))

* Merge pull request #131 from opentargets/do_rename_credibleSet

This PR describes the initial stages of defining the content of `locus` in our StudyLocus data model. More updates are expected in next iterations. ([`2363290`](https://github.com/opentargets/genetics_etl_python/commit/2363290702d9bb4c2ad4399f368d2cb2f665e936))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`bf301ff`](https://github.com/opentargets/genetics_etl_python/commit/bf301fff67486614c971f5d694771e625c0547d5))

* Merge branch &#39;main&#39; into ds_clustering_w_carry_over ([`0daf461`](https://github.com/opentargets/genetics_etl_python/commit/0daf46114c29e2c945c453d2a89d677e9fce5f1c))

* Merge pull request #120 from opentargets/il-update-cluster

feat: update existing cluster for development ([`cfaf68f`](https://github.com/opentargets/genetics_etl_python/commit/cfaf68f910570871d6f1963237c26818841d2712))

* Merge pull request #115 from opentargets/il-dataset-abc

Set `Dataset` as an abstract class ([`2b39bdb`](https://github.com/opentargets/genetics_etl_python/commit/2b39bdb9c0fb4624113a5ed6f131ff3ce91ec27e))

* Merge pull request #119 from opentargets/il-gwas-studylocusid

Assign final `studyLocusId` to GWASCat data ([`35e463a`](https://github.com/opentargets/genetics_etl_python/commit/35e463a7549bded1b1006ca29695e8270b0ef797))

* Merge pull request #130 from opentargets/do_fixdocs

Docs build is now tested ([`b28668b`](https://github.com/opentargets/genetics_etl_python/commit/b28668bec9cedcf42d43afb952e2ae07648dd260))

* revert: use assign_study_locus_id as a static method ([`6913d4a`](https://github.com/opentargets/genetics_etl_python/commit/6913d4a439775f2e5bd219f93e8258871f2f29ca))

* Merge pull request #129 from opentargets/do_fixdocs

docs: fix incorrect reference ([`b3911c6`](https://github.com/opentargets/genetics_etl_python/commit/b3911c6e981dc1070d0ea0ca87997aaa013047d1))

* Merge pull request #110 from opentargets/il-bm-dump

Important consequences of this PR to keep in mind:
- `StudyLocus` contains an `ldSet` column in addition to the `credibleSet`
- There is a bug on the calculation of PICS that causes most of our `credibleSet` to only contain the lead variant
- Spark session configuration was slightly adjusted to run this step. Changes have not been included in this PR ([`6d13f74`](https://github.com/opentargets/genetics_etl_python/commit/6d13f7410fbf88ef6963703ba1028eb9728e8997))

* Merge pull request #112 from opentargets/il-ld-annotator

Important consequences of this PR to keep in mind:
- `StudyLocus` contains an `ldSet` column in addition to the `credibleSet`
- There is a bug on the calculation of PICS that causes most of our `credibleSet` to only contain the lead variant ([`714e63e`](https://github.com/opentargets/genetics_etl_python/commit/714e63e666a3c172e7924fd9d9fe8dcf2f782b53))

* Merge pull request #127 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`82111eb`](https://github.com/opentargets/genetics_etl_python/commit/82111eba3dc352218cb72943df0bec3cae4b0c70))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/astral-sh/ruff-pre-commit: v0.0.287 → v0.0.288](https://github.com/astral-sh/ruff-pre-commit/compare/v0.0.287...v0.0.288)
- [github.com/psf/black: 23.7.0 → 23.9.1](https://github.com/psf/black/compare/23.7.0...23.9.1) ([`c22550b`](https://github.com/opentargets/genetics_etl_python/commit/c22550bdc9d49a4cf4d1cd1be2f64b0cfbe01531))

* Merge pull request #126 from opentargets/il-ruff

Use Ruff as a linter + minor improvements ([`850026f`](https://github.com/opentargets/genetics_etl_python/commit/850026fbc891bac415cce49f227be5e545edfff6))

* Merge branch &#39;il-ruff&#39; of https://github.com/opentargets/genetics_etl_python into il-ruff ([`2fcad00`](https://github.com/opentargets/genetics_etl_python/commit/2fcad00d329a2484b9d444d3b2a427057a1ca89a))

* Merge branch &#39;il-ruff&#39; of https://github.com/opentargets/genetics_etl_python into il-ruff ([`a1f8209`](https://github.com/opentargets/genetics_etl_python/commit/a1f8209ca5832ab5a83b1dbe4abf2c5a4496ea13))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`d18cd03`](https://github.com/opentargets/genetics_etl_python/commit/d18cd03b82c441a621d3f840d5fa393744f60332))

* Merge pull request #124 from opentargets/il-overlaps-index

feat: add `StudyLocusOverlaps` step to the project ([`99a6561`](https://github.com/opentargets/genetics_etl_python/commit/99a6561b6472126076dba8deb53658134f85ec50))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into do-airflow ([`74a19c8`](https://github.com/opentargets/genetics_etl_python/commit/74a19c8b948ff9bf463ddb5f0ee6242b845279bd))

* Merge pull request #123 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`e13ec2f`](https://github.com/opentargets/genetics_etl_python/commit/e13ec2fdbf5198cce4fb3d4159c623039b96e6e0))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/pre-commit/mirrors-mypy: v1.5.0 → v1.5.1](https://github.com/pre-commit/mirrors-mypy/compare/v1.5.0...v1.5.1) ([`4c71c0c`](https://github.com/opentargets/genetics_etl_python/commit/4c71c0c2a33030982346c3d472207f9d29b0f6f1))

* Revert &#34;feat: first dag&#34;

This reverts commit 6f28714c2848581164740778229f2979660d6857. ([`0a117e5`](https://github.com/opentargets/genetics_etl_python/commit/0a117e5395a4bb1d5bce62abe38204962a8779ba))

* Revert &#34;fix: failed dependency&#34;

This reverts commit 5d1cc823f805aadd9bb67236fa7edf7f3d20a39a. ([`c5bb499`](https://github.com/opentargets/genetics_etl_python/commit/c5bb499d5b879007240e8efa64f37adfe6b7554c))

* Revert &#34;Delete CHANGELOG.md&#34;

This reverts commit 32d7384a789edebd124182e018c6751899283473. ([`13e872a`](https://github.com/opentargets/genetics_etl_python/commit/13e872a0f2030d77871dded3143a0d1161f1f115))

* Delete CHANGELOG.md ([`32d7384`](https://github.com/opentargets/genetics_etl_python/commit/32d7384a789edebd124182e018c6751899283473))

* Merge pull request #118 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`31c24d5`](https://github.com/opentargets/genetics_etl_python/commit/31c24d5d65f72031da91ca9c761a0dce46dcdc0b))

* revert: revert aggregation to multiple columns ([`09a2c6f`](https://github.com/opentargets/genetics_etl_python/commit/09a2c6f91f57a8731da1e9e623280095e1914f9b))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/hadialqattan/pycln: v2.2.1 → v2.2.2](https://github.com/hadialqattan/pycln/compare/v2.2.1...v2.2.2)
- [github.com/pre-commit/mirrors-mypy: v1.4.1 → v1.5.0](https://github.com/pre-commit/mirrors-mypy/compare/v1.4.1...v1.5.0) ([`1ef927d`](https://github.com/opentargets/genetics_etl_python/commit/1ef927dd1fc694db016df62e2972f4b69d579eee))

* Merge pull request #117 from opentargets/il-dataset-feat

Small new features for Dataset ([`c7dcbd8`](https://github.com/opentargets/genetics_etl_python/commit/c7dcbd8f5b882b88a082949a6402ddf6df5e1ccf))

* Merge pull request #116 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`9396369`](https://github.com/opentargets/genetics_etl_python/commit/9396369208661b88611f250473dcaa366b35e4d4))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/hadialqattan/pycln: v2.2.0 → v2.2.1](https://github.com/hadialqattan/pycln/compare/v2.2.0...v2.2.1) ([`2a1c6c6`](https://github.com/opentargets/genetics_etl_python/commit/2a1c6c63515f1a2693df3d520eef6ea2c95c99a6))

* Merge pull request #114 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`8335528`](https://github.com/opentargets/genetics_etl_python/commit/8335528fdf7268ca858d8f6f2266c1a0aab8ae52))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/hadialqattan/pycln: v2.1.6 → v2.2.0](https://github.com/hadialqattan/pycln/compare/v2.1.6...v2.2.0)
- [github.com/pycqa/flake8: 6.0.0 → 6.1.0](https://github.com/pycqa/flake8/compare/6.0.0...6.1.0) ([`9b89b4e`](https://github.com/opentargets/genetics_etl_python/commit/9b89b4e14852cb991c6ee1bf306dd3998e3a45c4))

* Merge pull request #108 from opentargets/il-fix-ld-annotation

Fixes to the `annotate_ld` function ([`536a9d1`](https://github.com/opentargets/genetics_etl_python/commit/536a9d1b9735d36dfe3fb96373a3d7d6b2db5600))

* Merge pull request #111 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`c8079c7`](https://github.com/opentargets/genetics_etl_python/commit/c8079c707e7db7e26e99fa6990fc73e829fad3c8))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/hadialqattan/pycln: v2.1.5 → v2.1.6](https://github.com/hadialqattan/pycln/compare/v2.1.5...v2.1.6) ([`01c1280`](https://github.com/opentargets/genetics_etl_python/commit/01c1280576b3ff7551cc0b121c3ca2621e5efb04))

* Merge branch &#39;il-bm-dump&#39; of https://github.com/opentargets/genetics_etl_python into il-bm-dump ([`98be830`](https://github.com/opentargets/genetics_etl_python/commit/98be8308278e3fc8bac5b19a46146c53675ce9d0))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-bm-dump ([`fa1b0cf`](https://github.com/opentargets/genetics_etl_python/commit/fa1b0cf4ea3c1c37c1a8a022a43cbbd32fff31b5))

* Merge pull request #106 from opentargets/ds_3017_update_sumstats

refactor: some minor issues sorted out around summary statistics ([`0d976ba`](https://github.com/opentargets/genetics_etl_python/commit/0d976ba74ab5e442652518a5db34970e31190037))

* Merge pull request #107 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`883347e`](https://github.com/opentargets/genetics_etl_python/commit/883347e3ba8e66041e79336e1b988f34fd470b2e))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/psf/black: 23.3.0 → 23.7.0](https://github.com/psf/black/compare/23.3.0...23.7.0) ([`3b0cd7b`](https://github.com/opentargets/genetics_etl_python/commit/3b0cd7be1707e5efdd79a6230d22910a5914fb01))

* Merge pull request #105 from opentargets:il-ldindex-duplicates

Remove ambiguous variants from LDIndex ([`0b8b9b7`](https://github.com/opentargets/genetics_etl_python/commit/0b8b9b7ee29637c65bcfb1bf74063e785605da8e))

* Merge pull request #97 from opentargets:ds_sumstats_to_locus

Finding loci via a distance based clumping ([`630d8ef`](https://github.com/opentargets/genetics_etl_python/commit/630d8ef8ec1f5411482f4d48f539e821abd42818))

* Merge pull request #104 from opentargets:il-fix-pics

Add `test__finemap` ([`c2a7513`](https://github.com/opentargets/genetics_etl_python/commit/c2a75136ce9f1c194c84d6a25b6438288c4260d6))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into ds_sumstats_to_locus ([`ec2b229`](https://github.com/opentargets/genetics_etl_python/commit/ec2b22950b5f5b232364185443aeb916aebbd119))

* Merge pull request #103 from opentargets/il-studylocusid-func

feat: add `get_study_locus_id` ([`69e5abf`](https://github.com/opentargets/genetics_etl_python/commit/69e5abf5528e479ff8f908eba9f2911f58046e68))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into ds_sumstats_to_locus ([`c842213`](https://github.com/opentargets/genetics_etl_python/commit/c842213ebf498c281f7beb91ad0d4e0e3a4eeb00))

* Merge pull request #102 from opentargets/il-fix-pics

fix: avoid empty credible set (#3016) ([`8fd06d7`](https://github.com/opentargets/genetics_etl_python/commit/8fd06d734fcb245e69e6197fcea0d6c4994583cf))

* Merge pull request #100 from opentargets/do_mantissa_float

fix: pValueMantissa as float in summary stats ([`dcac9b9`](https://github.com/opentargets/genetics_etl_python/commit/dcac9b9f501d0657c5adb5e5b278716724e3897c))

* Merge pull request #101 from opentargets/il-ld-clumping

feat: fix ldclumping, tests, and docs ([`c9d0bba`](https://github.com/opentargets/genetics_etl_python/commit/c9d0bbaf7d56313cf974bf2df3fccba8a8e606bc))

* Update src/otg/method/window_based_clumping.py ([`cb56d99`](https://github.com/opentargets/genetics_etl_python/commit/cb56d990d8172168a7c6d063c56a9447c9ffdbbc))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`5c5e0cd`](https://github.com/opentargets/genetics_etl_python/commit/5c5e0cd922e8977ed35b660da717d926575e0b2e))

* Update src/otg/method/window_based_clumping.py ([`1e50b57`](https://github.com/opentargets/genetics_etl_python/commit/1e50b57ae382143fdacc63df8d8214afc100ce01))

* Merge pull request #98 from opentargets/do_fixavatars

fix: stacking avatars in docs ([`877194b`](https://github.com/opentargets/genetics_etl_python/commit/877194b63f6054635fccdc3a89f5694eeadeff57))

* Merge pull request #95 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`8840ad5`](https://github.com/opentargets/genetics_etl_python/commit/8840ad5274123348d16412797ed039d15b046299))

* Merge pull request #96 from opentargets/tskir-node-troubleshooting

Thanks, @tskir. This will hopefully save some time for someone in the future. ([`b900e8d`](https://github.com/opentargets/genetics_etl_python/commit/b900e8df7533b6fd96db204ac9ef806c2b5e477c))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/pre-commit/mirrors-mypy: v1.3.0 → v1.4.1](https://github.com/pre-commit/mirrors-mypy/compare/v1.3.0...v1.4.1) ([`65cbe68`](https://github.com/opentargets/genetics_etl_python/commit/65cbe6821133856307405bbfc062e587ca103ac3))

* Merge pull request #88 from opentargets/2955-tskir-finngen-r9

Migrate to FinnGen R9 ([`d3f2e1b`](https://github.com/opentargets/genetics_etl_python/commit/d3f2e1b492ed8dcf402afbb0ead11a36c8cbadfa))

* Merge pull request #92 from opentargets/il-gwas-index-fix

Fix GWASCatalog data issues ([`594f738`](https://github.com/opentargets/genetics_etl_python/commit/594f7380be588488cbb3f29c1a187c27fc81f1dc))

* Merge pull request #86 from opentargets/hn-ukbb-ingest

UKBiobank ingestion ([`ef5afb0`](https://github.com/opentargets/genetics_etl_python/commit/ef5afb085b94b0894a8dcddc6dc33324b1fd9d40))

* Merge pull request #93 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`f816ece`](https://github.com/opentargets/genetics_etl_python/commit/f816ece41c55f96eb9781b1bcdb70b9e7586297a))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/asottile/yesqa: v1.4.0 → v1.5.0](https://github.com/asottile/yesqa/compare/v1.4.0...v1.5.0) ([`e37d247`](https://github.com/opentargets/genetics_etl_python/commit/e37d247124f1104aa54741ea9afce1126a432ad6))

* Merge pull request #91 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`f55fec9`](https://github.com/opentargets/genetics_etl_python/commit/f55fec936a02010bf2739761a806cec20c38d669))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/hadialqattan/pycln: v2.1.3 → v2.1.5](https://github.com/hadialqattan/pycln/compare/v2.1.3...v2.1.5)
- [github.com/pycqa/flake8: 5.0.4 → 6.0.0](https://github.com/pycqa/flake8/compare/5.0.4...6.0.0)
- [github.com/pre-commit/mirrors-mypy: v1.2.0 → v1.3.0](https://github.com/pre-commit/mirrors-mypy/compare/v1.2.0...v1.3.0) ([`c83c992`](https://github.com/opentargets/genetics_etl_python/commit/c83c9924c5ee5bcfdf4411aa378db20ce47d0911))

* Merge pull request #90 from opentargets/buniello-patch-4

Update roadmap.md ([`3afcc35`](https://github.com/opentargets/genetics_etl_python/commit/3afcc35a23c6b40774e49a29de906d80f2ff0831))

* Merge pull request #89 from opentargets/buniello-patch-3

Update roadmap.md ([`66ba355`](https://github.com/opentargets/genetics_etl_python/commit/66ba355bb31510a4e1b4d7c54a4e420ccafda19c))

* Update roadmap.md ([`6e3d6ad`](https://github.com/opentargets/genetics_etl_python/commit/6e3d6ad48156dabdb867d5a65910ae665ff6b22b))

* Update roadmap.md ([`29d3132`](https://github.com/opentargets/genetics_etl_python/commit/29d31328abf55bdb9284c4054550b783d16cdeb3))

* Merge pull request #87 from opentargets/tskir-contributing

Create a contributing checklist for the genetics repository ([`117945b`](https://github.com/opentargets/genetics_etl_python/commit/117945bf83502570217ef444e7bc998a1154e55b))

* Merge pull request #79 from opentargets/do_gcp_image_2_1

This PR&#39;s main feature is upgrading the reference [GCP images to 2.1](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.1). As a consequence, Spark is updated to 3.3.0 and Python to 3.10.

 *BREAKING CHANGE*: This will require updating everyone&#39;s poetry environments. You might need to run `make setup-dev` again on your local machine. ([`579e991`](https://github.com/opentargets/genetics_etl_python/commit/579e991fdd15690bb9f1fec1f10a76fe4ee79798))

* Merge pull request #83 from opentargets/il-study-locus

Debugging GWAS Catalog pipeline ([`c09c1f4`](https://github.com/opentargets/genetics_etl_python/commit/c09c1f4cd5e38f4ec5ef2cdd732913824cb28d7f))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`427535a`](https://github.com/opentargets/genetics_etl_python/commit/427535afb6fb6d86595a14a12afea56531c6410a))

* Merge pull request #85 from opentargets/buniello-patch-2

Update roadmap.md ([`ea7efcb`](https://github.com/opentargets/genetics_etl_python/commit/ea7efcb3aab96799826362253c97c110ce55a8f8))

* Merge pull request #84 from opentargets/buniello-patch-1

docs: update documentation ([`30000e7`](https://github.com/opentargets/genetics_etl_python/commit/30000e755de491b6027c4dde98a8aedf7e6168b4))

* Update roadmap.md ([`c89f768`](https://github.com/opentargets/genetics_etl_python/commit/c89f768d9b9c154b272aa5e9083fcd7f444b4e10))

* Merge branch &#39;do_gcp_image_2_1&#39; into il-study-locus ([`b4020b8`](https://github.com/opentargets/genetics_etl_python/commit/b4020b83b4e763411bffea691f997468f3802019))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into do_gcp_image_2_1 ([`ef18bd1`](https://github.com/opentargets/genetics_etl_python/commit/ef18bd1f7f07a5e73e9062ae44b8e6e050f11ef6))

* Merge pull request #82 from opentargets/2946-tskir-versioning

Implement version aware code deployment ([`f67387e`](https://github.com/opentargets/genetics_etl_python/commit/f67387e614a3a9f527815f6fe2be343d786595c8))

* Fix numbering in README.md ([`5c7bbbc`](https://github.com/opentargets/genetics_etl_python/commit/5c7bbbc31c3c026853223f27249fec57397c1233))

* Merge pull request #80 from opentargets/do-finngen-step-docfix

docs: missing docs for finngen step added ([`7cfb0ca`](https://github.com/opentargets/genetics_etl_python/commit/7cfb0ca90ce9d00448fc263ab29cb2ca45c458d7))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into do_gcp_image_2_1 ([`85aaeb0`](https://github.com/opentargets/genetics_etl_python/commit/85aaeb0ffb43fe242a1869b4bb486ea888fbd185))

* Merge pull request #59 from opentargets/2771-tskir-finngen

FinnGen ingestion ([`9afe9ee`](https://github.com/opentargets/genetics_etl_python/commit/9afe9ee656db51b1c82822e175ec2cac8cecbf39))

* Merge pull request #77 from opentargets/il-credset

Redefinition of credible set annotation ([`d5a51ab`](https://github.com/opentargets/genetics_etl_python/commit/d5a51ab2e45c1f8415d9ac98e2cdb7fc83395a8b))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-credset ([`ccf6ecf`](https://github.com/opentargets/genetics_etl_python/commit/ccf6ecf8f21deca1dd43d25703e36d8461907627))

* Merge pull request #76 from opentargets/il-nested-validation

Improved validation of nested structures ([`bff5c0d`](https://github.com/opentargets/genetics_etl_python/commit/bff5c0d0c2124e7f89ad132d13f375fb891c5bea))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python ([`a0bfd8c`](https://github.com/opentargets/genetics_etl_python/commit/a0bfd8c0338808742d43e4bb83427bf06906115e))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into do_gcp_image_2_1 ([`9ab1c58`](https://github.com/opentargets/genetics_etl_python/commit/9ab1c58078212348f7973457ca8b13d12e4622eb))

* Merge pull request #73 from opentargets/do_va_slim

Do va slim ([`8e095fe`](https://github.com/opentargets/genetics_etl_python/commit/8e095fe1e8cd53146322fcc763ddf1a67ed90235))

* Merge pull request #75 from opentargets/do_test_session_streamline

Test session streamline ([`2a7e1bc`](https://github.com/opentargets/genetics_etl_python/commit/2a7e1bca9553315f63281248c0247634ec92dae6))

* Merge pull request #72 from opentargets/do_test_credset

test: is in credset function ([`b81327a`](https://github.com/opentargets/genetics_etl_python/commit/b81327a3371f8161caaea2f2e9eebdc49fd3d022))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python ([`5bc4b38`](https://github.com/opentargets/genetics_etl_python/commit/5bc4b38673748abd6b9f3e18e274c30e355efa13))

* Merge pull request #71 from opentargets/il-schemas

Validate schemas overlooking the nullability fields ([`660e5d8`](https://github.com/opentargets/genetics_etl_python/commit/660e5d8a352d36b5576b0a877cc053bfcf343ed9))

* Merge branch &#39;il-schemas&#39; of https://github.com/opentargets/genetics_etl_python into il-schemas ([`57310d7`](https://github.com/opentargets/genetics_etl_python/commit/57310d754a32ebda2ed5a37bdab18ffaab76676a))

* Revert &#34;build: update mypy to 1.2.0&#34;

This reverts commit 0d5c6e9e9f6592f4b005e5feef43c550c3535f2d. ([`34f1024`](https://github.com/opentargets/genetics_etl_python/commit/34f1024a4739038e715878070c43453cead03c3d))

* Revert &#34;fix: order ld index by idx and unpersist data&#34;

This reverts commit 5a8e03a7030c88b6b7c39f2ed20123598ff76dea. ([`aba6bd8`](https://github.com/opentargets/genetics_etl_python/commit/aba6bd8e82f8a2f0469b69a37698540b96bdee42))

* Revert &#34;fix: correct attribute names for ld indices&#34;

This reverts commit b1d56c48cac25ca11c587527c32df5ddbeddc004. ([`1f6e389`](https://github.com/opentargets/genetics_etl_python/commit/1f6e389e626c741fce47520bfa9be3bf4482c950))

* Revert &#34;style: rename ld indices location to directory and no extension&#34;

This reverts commit 512c842e7e5ac744820b017fc5640b8bf009ef24. ([`eedf763`](https://github.com/opentargets/genetics_etl_python/commit/eedf76309d45230d82e1e614a99e37504aa32c87))

* Revert &#34;fix: join `_variant_coordinates_in_ldindex` on `variantId`&#34;

This reverts commit 829ef6b982cc7c83c9fa95928140d23987cd67d3. ([`5f9c7c1`](https://github.com/opentargets/genetics_etl_python/commit/5f9c7c1b47cb5c77c2edde75e211e33f24212b12))

* Revert &#34;fix: move rsId and concordance check outside the filter function&#34;

This reverts commit c57919d2d28e0cea8f4eb67f2b2fb2e30d273945. ([`5dda33f`](https://github.com/opentargets/genetics_etl_python/commit/5dda33f80213e3ed15a3c7a9038422c5345d4ab6))

* Revert &#34;fix: update configure and gitignore&#34;

This reverts commit 6cc9af18a6061083abbd403b35034f56233dd226. ([`4051d0d`](https://github.com/opentargets/genetics_etl_python/commit/4051d0dbe733ea0a0d759e469474cea4dc2e5f82))

* Merge branch &#39;il-schemas&#39; of https://github.com/opentargets/genetics_etl_python into il-schemas ([`079ee76`](https://github.com/opentargets/genetics_etl_python/commit/079ee76589d7c02318fd9a1789b8085dd42ecbfd))

* Merge branch &#39;main&#39; into il-schemas ([`27eeb03`](https://github.com/opentargets/genetics_etl_python/commit/27eeb03513401a24e04b5e3f6f9902e6bbbd5597))

* Merge pull request #66 from opentargets/do_hydra

Do hydra ([`52152ff`](https://github.com/opentargets/genetics_etl_python/commit/52152fff02e572672595c68016932bf37036c032))

* Merge pull request #68 from opentargets/ds_summary_stats_ingest

Summary statistics dataset ([`499a780`](https://github.com/opentargets/genetics_etl_python/commit/499a780e83dc07eacd3bb48ba8e5d6c68d8f4b96))

* Update gcp.yaml

Wrong interpolation ([`bb377d5`](https://github.com/opentargets/genetics_etl_python/commit/bb377d5b2133f7f1033a73aa1cf17759c76f307c))

* Merge branch &#39;do_hydra&#39; into ds_summary_stats_ingest ([`b5fbcb0`](https://github.com/opentargets/genetics_etl_python/commit/b5fbcb08aa8acc6d9ffb28c00a94e369ca7e6422))

* Merge branch &#39;do_hydra&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`18fc46e`](https://github.com/opentargets/genetics_etl_python/commit/18fc46e73e93245686da0e87366c0a4dae7a1c15))

* Merge branch &#39;do_hydra&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`20bf3b7`](https://github.com/opentargets/genetics_etl_python/commit/20bf3b796a740a8b37943cfda8ad515fcd49764f))

* Merge branch &#39;do_hydra&#39; of https://github.com/opentargets/genetics_etl_python into ds_summary_stats_ingest ([`172fa25`](https://github.com/opentargets/genetics_etl_python/commit/172fa2563e58892da1a847521fffbd83cd084185))

* Merge branch &#39;do_hydra&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`e6931c6`](https://github.com/opentargets/genetics_etl_python/commit/e6931c6557067179dafc87858ef0c5f1c71e3ab3))

* Merge branch &#39;do_hydra&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`8aa8f99`](https://github.com/opentargets/genetics_etl_python/commit/8aa8f9980d011220664bd8096db3007d817ba2fe))

* Merge pull request #64 from opentargets/ds_clumping_refactor

Clumping refactor ([`2500f7f`](https://github.com/opentargets/genetics_etl_python/commit/2500f7fa84ce27418418d281a7b40ea00b4000c7))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`d232e4e`](https://github.com/opentargets/genetics_etl_python/commit/d232e4ebdc63bfed6249bc4759ede7c2649537a2))

* Update src/etl/gwas_ingest/pics.py ([`7cf8f42`](https://github.com/opentargets/genetics_etl_python/commit/7cf8f4261aa78ca723b3d7032a1e3fe649b0d27d))

* Update src/etl/gwas_ingest/clumping.py ([`2772831`](https://github.com/opentargets/genetics_etl_python/commit/277283103f798c02a7660d10f432505aa53344bd))

* Merge branch &#39;do_hydra&#39; of https://github.com/opentargets/genetics_etl_python into il-l2g-prototype ([`9ccc916`](https://github.com/opentargets/genetics_etl_python/commit/9ccc9163f2514c4e2144067abd983e569da9d31b))

* Merge pull request #46 from opentargets/il-v2g-distance

Extract V2G evidence from a variant distance to a gene TSS ([`ce4fa35`](https://github.com/opentargets/genetics_etl_python/commit/ce4fa3598a9902e0c8883afae311407fee0458d6))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-v2g-distance ([`10b1d7c`](https://github.com/opentargets/genetics_etl_python/commit/10b1d7cf17652321e3187c76e90381a3c43819f4))

* [pre-commit.ci] pre-commit autoupdate (#63)

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/pycqa/isort: 5.11.4 → 5.12.0](https://github.com/pycqa/isort/compare/5.11.4...5.12.0)
- [github.com/psf/black: 22.12.0 → 23.1.0](https://github.com/psf/black/compare/22.12.0...23.1.0)
- [github.com/pycqa/flake8: 5.0.4 → 6.0.0](https://github.com/pycqa/flake8/compare/5.0.4...6.0.0)
- [github.com/pycqa/pydocstyle: 6.2.3 → 6.3.0](https://github.com/pycqa/pydocstyle/compare/6.2.3...6.3.0)

* fix: reverting flake8 version

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci

---------

Co-authored-by: pre-commit-ci[bot] &lt;66853113+pre-commit-ci[bot]@users.noreply.github.com&gt;
Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt; ([`b8b5bb1`](https://github.com/opentargets/genetics_etl_python/commit/b8b5bb1fb97c4b6cfeddd2fe06e3147845dca20a))

* [pre-commit.ci] pre-commit autoupdate (#56)

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/pre-commit/pygrep-hooks: v1.9.0 → v1.10.0](https://github.com/pre-commit/pygrep-hooks/compare/v1.9.0...v1.10.0)
- [github.com/asottile/pyupgrade: v3.3.0 → v3.3.1](https://github.com/asottile/pyupgrade/compare/v3.3.0...v3.3.1)
- [github.com/hadialqattan/pycln: v2.1.2 → v2.1.3](https://github.com/hadialqattan/pycln/compare/v2.1.2...v2.1.3)
- [github.com/pycqa/isort: 5.10.1 → 5.11.4](https://github.com/pycqa/isort/compare/5.10.1...5.11.4)
- [github.com/psf/black: 22.10.0 → 22.12.0](https://github.com/psf/black/compare/22.10.0...22.12.0)
- [github.com/pycqa/flake8: 5.0.4 → 6.0.0](https://github.com/pycqa/flake8/compare/5.0.4...6.0.0)
- [github.com/alessandrojcm/commitlint-pre-commit-hook: v9.3.0 → v9.4.0](https://github.com/alessandrojcm/commitlint-pre-commit-hook/compare/v9.3.0...v9.4.0)
- [github.com/pycqa/pydocstyle: 6.1.1 → 6.2.3](https://github.com/pycqa/pydocstyle/compare/6.1.1...6.2.3)

* regressing flake8

Co-authored-by: pre-commit-ci[bot] &lt;66853113+pre-commit-ci[bot]@users.noreply.github.com&gt;
Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt; ([`e0f02ea`](https://github.com/opentargets/genetics_etl_python/commit/e0f02ea7237d1f047ae1d7b019cf854d19889825))

* Do pics (#57)

* feat: experimental LD using hail

* feat: handling of the lower triangle

* docs: more comments explaining what\&#39;s going on

* feat: non-working implementation of ld information

* feat: ld information based on precomputed index

* fix: no longer neccesary

* feat: missing ld.py added

* docs: intervals functions examples

* fix: typo

* refactor: larger scale update in the GWAS Catalog data ingestion

* feat: precommit updated

* feat: first pics implementation derived from gnomAD LD information

* feat: modularise iterating over populations

* feat: finalizing GWAS Catalog ingestion steps

* fix: test fixed, due to changes in logic

* feat: ignoring .coverage file

* feat: modularised pics method

* feat: integrating pics

* chore: smoothing out bits

* feat: cleanup of the pics logic

No select statements, more concise functions and carrying over all required information

* fix: slight updates

* feat: map gnomad positions to ensembl positions LD

* fix: use cumsum instead of postprob

* feat: update studies schemas

* feat: working on integrating ingestion with pics

* feat: support for hail doctests

* test: _query_block_matrix example

* feat: ignore hail logs for testing

* feat: ingore TYPE_CHECKING blocks from testing

* feat: pics benchmark notebook (co-authored by Irene)

* feat: new finding on r &gt; 1

* docs: Explanation about the coordinate shift

* test: several pics doctests

* test: fixed test for log neg pval

* style: remove variables only used for return

* feat: parametrise liftover chain

* refactor: consolidating some code

* feat: finishing pics

* chore: adding tests to effect harmonization

* chore: adding more tests

* feat: benchmarking new dataset

* fix: resolving minor bugs around pics

* Apply suggestions from code review

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt;

* fix: applying review comments

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci

* fix: addressing some more review comments

* fix: update GnomAD join

* fix: bug sorted out in pics filterin

* feat: abstracting QC flagging

* feat: solving clumping

* fix: clumping is now complete

Co-authored-by: David &lt;ochoa@ebi.ac.uk&gt;
Co-authored-by: David Ochoa &lt;dogcaesar@gmail.com&gt;
Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt;
Co-authored-by: pre-commit-ci[bot] &lt;66853113+pre-commit-ci[bot]@users.noreply.github.com&gt; ([`7e42f94`](https://github.com/opentargets/genetics_etl_python/commit/7e42f947fbbd1c030f166459d082ae7fa70c1592))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`e94adfd`](https://github.com/opentargets/genetics_etl_python/commit/e94adfd9a306580f85e80c6cea0357383393f9f3))

* Merge branch &#39;main&#39; into il-v2g-distance ([`363288e`](https://github.com/opentargets/genetics_etl_python/commit/363288edfec067d5137b61d5bc9500b465340a74))

* Integrate eCAVIAR colocalization (#58)

* refactor: extract method to find overlapping peaks to `find_gwas_vs_all_overlapping_peaks`

* test: add tests for `find_gwas_vs_all_overlapping_peaks`

* fix: add gene_id to the metadata col

* feat: implement working ecaviar

* test: add test for `ecaviar_colocalisation`

* test: added test for _extract_credible_sets

* fix: filter only variants in the 95 credible set

* docs: update coloc docs

* feat: add coloc schema and validation step ([`8c5c532`](https://github.com/opentargets/genetics_etl_python/commit/8c5c532cfc4afb33e536a5acfc663a7a630d6985))

* Delete .DS_Store ([`010447b`](https://github.com/opentargets/genetics_etl_python/commit/010447b9f82c1e390af3d16b1050cd9380dfc5ab))

* Merge pull request #54 from opentargets/il-update-coloc

Update the coloc pipeline to the newer credible sets and study datasets ([`dc0f566`](https://github.com/opentargets/genetics_etl_python/commit/dc0f56682e1b1b658f5ca2b22c0fbc6198646186))

* Merge pull request #55 from opentargets/il-python-update

Update package to Python 3.8.15 ([`ff6cdc3`](https://github.com/opentargets/genetics_etl_python/commit/ff6cdc3032823788b603d18fce60fdef4877daee))

* Merge pull request #53 from opentargets/il-refactor-coloc

Reorganisation of the coloc logic ([`2b7a338`](https://github.com/opentargets/genetics_etl_python/commit/2b7a338f3143c691b4a136c631b78f21a5cbeea1))

* Merge pull request #52 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`d1079a5`](https://github.com/opentargets/genetics_etl_python/commit/d1079a5abe250d378f8e73dbd489697bb9b617b6))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/pre-commit/pre-commit-hooks: v4.3.0 → v4.4.0](https://github.com/pre-commit/pre-commit-hooks/compare/v4.3.0...v4.4.0)
- [github.com/asottile/pyupgrade: v3.2.2 → v3.3.0](https://github.com/asottile/pyupgrade/compare/v3.2.2...v3.3.0)
- [github.com/pycqa/flake8: 5.0.4 → 6.0.0](https://github.com/pycqa/flake8/compare/5.0.4...6.0.0) ([`e1dc17a`](https://github.com/opentargets/genetics_etl_python/commit/e1dc17aad1c403533b77a14ba4236c01a8f4a2f8))

* Merge pull request #41 from opentargets/il-vep

Extract V2G evidence from functional predictions ([`57b0adf`](https://github.com/opentargets/genetics_etl_python/commit/57b0adf9e68e10f4256826ea00f3b7c6de7dc915))

* Merge pull request #51 from opentargets/do_awesome_pages

feat: ignore docstrings from private functions in documentation ([`16adafc`](https://github.com/opentargets/genetics_etl_python/commit/16adafccfca301d250216aa296f47573ddfb7371))

* Merge branch &#39;main&#39; into il-vep ([`21104f3`](https://github.com/opentargets/genetics_etl_python/commit/21104f386597bad1a011166f12ea93a9c1ba16f2))

* Merge branch &#39;main&#39; into il-vep ([`827e244`](https://github.com/opentargets/genetics_etl_python/commit/827e2448a7fc2482d3cf772d6604d097d65c5fb4))

* Merge pull request #50 from opentargets/do_awesome_pages

feat: plugin not to automatically handle required pages ([`7ebe630`](https://github.com/opentargets/genetics_etl_python/commit/7ebe6303cc2de4ce0327a167aab452114feefad1))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/pre-commit/pre-commit-hooks: v4.3.0 → v4.4.0](https://github.com/pre-commit/pre-commit-hooks/compare/v4.3.0...v4.4.0)
- [github.com/pycqa/flake8: 5.0.4 → 6.0.0](https://github.com/pycqa/flake8/compare/5.0.4...6.0.0) ([`1a4082e`](https://github.com/opentargets/genetics_etl_python/commit/1a4082e52621cdcb9acab11af61efacc2d0450b9))

* Add `gnomad3VariantId` to variant annotation schema (#48)

* feat: repartition data on chromosome and sort by position

* feat: index repartitioned on chromosome and sort by position

* fix: reverting some changes before starting other update

* feat: converting gnomad positions to ensembl

* feat: adding Gnomad3 variant identifier

* fix: slight updates

* feat: include gnomad3VariantId in the variant schema

* feat: include gnomad3VariantId in the variant schema

Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt; ([`621dfe2`](https://github.com/opentargets/genetics_etl_python/commit/621dfe28da7bf8e8beb8adf0e0f3675eef0f22d7))

* [pre-commit.ci] pre-commit autoupdate (#42)

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/asottile/pyupgrade: v3.2.0 → v3.2.2](https://github.com/asottile/pyupgrade/compare/v3.2.0...v3.2.2)
- [github.com/hadialqattan/pycln: v2.1.1 → v2.1.2](https://github.com/hadialqattan/pycln/compare/v2.1.1...v2.1.2)
- [github.com/pre-commit/mirrors-mypy: v0.982 → v0.991](https://github.com/pre-commit/mirrors-mypy/compare/v0.982...v0.991)

* chore: separate script for dependency installation (#47)

* Minor improvements to the variant datasets generation (#45)

* feat: repartition data on chromosome and sort by position

* feat: index repartitioned on chromosome and sort by position

* fix: reverting some changes before starting other update

* feat: converting gnomad positions to ensembl

* feat: adding Gnomad3 variant identifier

* fix: slight updates

Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt;

The script runs and generated dataset is agreement with expectation.

* fix: adding __init__.py

Co-authored-by: pre-commit-ci[bot] &lt;66853113+pre-commit-ci[bot]@users.noreply.github.com&gt;
Co-authored-by: Kirill Tsukanov &lt;tskir@users.noreply.github.com&gt;
Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt;
Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt; ([`7fad89c`](https://github.com/opentargets/genetics_etl_python/commit/7fad89cd3eb326f48943b3fa076db214a8cd8a79))

* Minor improvements to the variant datasets generation (#45)

* feat: repartition data on chromosome and sort by position

* feat: index repartitioned on chromosome and sort by position

* fix: reverting some changes before starting other update

* feat: converting gnomad positions to ensembl

* feat: adding Gnomad3 variant identifier

* fix: slight updates

Co-authored-by: Daniel Suveges &lt;daniel.suveges@protonmail.com&gt;

The script runs and generated dataset is agreement with expectation. ([`95b5a88`](https://github.com/opentargets/genetics_etl_python/commit/95b5a88862235729174fa3ce62ee4c83f2993ac6))

* Merge branch &#39;il-vep&#39; of https://github.com/opentargets/genetics_etl_python into il-vep ([`b029e3b`](https://github.com/opentargets/genetics_etl_python/commit/b029e3bf6d3f5b86e0292efd62b5686e6d784aef))

* revert commit to ebe6717 state ([`5c92546`](https://github.com/opentargets/genetics_etl_python/commit/5c92546ce3615a959c73cf7d1c1f23e933184c1f))

* Revert &#34;fix: fix tests data definition&#34;

This reverts commit a382ffaf3876feae152e39cfd010f4062cc61d87. ([`a4a390b`](https://github.com/opentargets/genetics_etl_python/commit/a4a390bda6712573bb04b3dc1504580b27b7f062))

* Merge pull request #40 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`86578da`](https://github.com/opentargets/genetics_etl_python/commit/86578da17fdd58c43d47594fd506fdccdf1eaf4a))

* Merge pull request #39 from opentargets/do_codecov

codecov coverage target ([`17b429c`](https://github.com/opentargets/genetics_etl_python/commit/17b429cc99a7551d55445c48d94ca2c56827725c))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/asottile/pyupgrade: v3.1.0 → v3.2.0](https://github.com/asottile/pyupgrade/compare/v3.1.0...v3.2.0) ([`e7a3840`](https://github.com/opentargets/genetics_etl_python/commit/e7a38404a71e760b2397f3d724493804797dbab0))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-vep ([`194347c`](https://github.com/opentargets/genetics_etl_python/commit/194347c798422762d72fda2ab404c8e224b222e6))

* Add variant index generation module (#33)

* feat: coalesce variant_idx
* feat: remove utils to parse vep and annotate closest genes
* feat: add output validation
* feat: extract `mostSevereConsequence` out of vep


* test: added tests for spark helpers

* docs: remove reference to closest gene
* docs: added missing docs

Co-authored-by: David Ochoa &lt;dogcaesar@gmail.com&gt; ([`bc42bb8`](https://github.com/opentargets/genetics_etl_python/commit/bc42bb817a5a1bb36263e44aa89cc9443138f53a))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into il-vep ([`b58f1dc`](https://github.com/opentargets/genetics_etl_python/commit/b58f1dc05c55c85fc3c7173e6adea5dc00d4a785))

* Merge pull request #38 from opentargets/DSuveges-patch-1

Adding variant pipeline docs ([`23e2228`](https://github.com/opentargets/genetics_etl_python/commit/23e2228386c21e482190028b44357dd20805a049))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`ff494bb`](https://github.com/opentargets/genetics_etl_python/commit/ff494bb0fc8dbc62fcd3663313d54732e39a411a))

* Adding variant pipeline docs

the __init__ script was missing! ([`fc8e6aa`](https://github.com/opentargets/genetics_etl_python/commit/fc8e6aa2eb0dbb0e25cc6e8d5727fc97c93f8ea9))

* pvalue to zscore refactor + refactored function to operate with column ([`31a1556`](https://github.com/opentargets/genetics_etl_python/commit/31a1556ec18f95f30bc6129385d388845ef5a0b0))

* Do_removepandas (#37)

remove pandas dependency + removing old docs ([`5edb5df`](https://github.com/opentargets/genetics_etl_python/commit/5edb5dff0903b5817d110113131e562273fc11a3))

* Modification of the variant annotation schema (#24) ([`6edcdea`](https://github.com/opentargets/genetics_etl_python/commit/6edcdea0a7a84e2f2ae4ba55118747e2e9994bb0))

* Fixing bug in pvalue -&gt; z-score calculation (#34)

* Fixing bug in pvalue -&gt; z-score calculation
* Returning to scipy.stats functions
* Updating examples ([`8c77cd1`](https://github.com/opentargets/genetics_etl_python/commit/8c77cd1617854d493031cfc1997fcdfceb53bc22))

* Merge pull request #32 from opentargets/do_ipython_support

feat: ipython support ([`17c61eb`](https://github.com/opentargets/genetics_etl_python/commit/17c61eb43f59d57ad9742c3365649aa9dabd1027))

* Merge pull request #31 from opentargets/do_docs

feat: token added to codecov ([`bef4e86`](https://github.com/opentargets/genetics_etl_python/commit/bef4e86a3cdddbc02ce73677a3a72fd26d95830a))

* Merge pull request #30 from opentargets/do_docs

feat: codecov integration ([`d169917`](https://github.com/opentargets/genetics_etl_python/commit/d16991767c6c12b6e4f06b7853ac042d6071b0c4))

* Merge pull request #29 from opentargets/do_docs

Support for doctests implemented ([`f9d42d2`](https://github.com/opentargets/genetics_etl_python/commit/f9d42d2b1b81afa5e2f6f420371383504937abb4))

* Merge branch &#39;main&#39; into do_docs ([`0fe439c`](https://github.com/opentargets/genetics_etl_python/commit/0fe439ca8fcd1366db3cb0920ef33bf7f43e7c7d))

* Merge pull request #28 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`fd995f8`](https://github.com/opentargets/genetics_etl_python/commit/fd995f87a7043ab866ab8172f8bc59886fd6de20))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/asottile/pyupgrade: v2.38.2 → v3.1.0](https://github.com/asottile/pyupgrade/compare/v2.38.2...v3.1.0)
- [github.com/psf/black: 22.8.0 → 22.10.0](https://github.com/psf/black/compare/22.8.0...22.10.0)
- [github.com/pre-commit/mirrors-mypy: v0.981 → v0.982](https://github.com/pre-commit/mirrors-mypy/compare/v0.981...v0.982) ([`56103b7`](https://github.com/opentargets/genetics_etl_python/commit/56103b75f5980421aa0f983352be8ef61d7145eb))

* Merge pull request #26 from opentargets/gwas_ingest_improve

Ingesting GWAS Catalog study table ([`4919545`](https://github.com/opentargets/genetics_etl_python/commit/4919545cf6fde1b52b8c57d3b28e3debcb2616dc))

* Merge remote-tracking branch &#39;refs/remotes/origin/gwas_ingest_improve&#39; into gwas_ingest_improve ([`5101a6d`](https://github.com/opentargets/genetics_etl_python/commit/5101a6d7d0017816900ec5d01bfd9b882c87e77a))

* Merge branch &#39;main&#39; into gwas_ingest_improve ([`40d4a03`](https://github.com/opentargets/genetics_etl_python/commit/40d4a033235e032087b17556a1bc68b9219b6fac))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python ([`ac9b7a8`](https://github.com/opentargets/genetics_etl_python/commit/ac9b7a8914925d3c9ec8fc6d3707215ab1e37952))

* Merge pull request #27 from opentargets/do_docs

force merging to checkout if gh action works ([`4f970bb`](https://github.com/opentargets/genetics_etl_python/commit/4f970bb6c2af74216c5f8f9d80532cd4385620f6))

* Merge pull request #25 from opentargets/do_docs

feat: docs support ([`c16f27b`](https://github.com/opentargets/genetics_etl_python/commit/c16f27b4dbbc454592d774833355e786fdfdb7c8))

* Merge branch &#39;main&#39; into do_docs ([`aa614a7`](https://github.com/opentargets/genetics_etl_python/commit/aa614a77726d6920337bf08e37d68a6d616095a9))

* Merge pull request #23 from opentargets/do_schemadoc

docs: schema functions description ([`4d881ab`](https://github.com/opentargets/genetics_etl_python/commit/4d881ab8c478215c20b399b6cda399d1616f066c))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python ([`decf535`](https://github.com/opentargets/genetics_etl_python/commit/decf535a4e6088639c3ff92dbbf4ad7fbca5e18f))

* Merge pull request #15 from opentargets/gwas_ingest_improve

Adding more stuff to GWAS Catalog ingest ([`9cb71cc`](https://github.com/opentargets/genetics_etl_python/commit/9cb71ccfb50d1970635b918218bf2d5d119cc44b))

* Merge branch &#39;main&#39; into gwas_ingest_improve ([`4d0f089`](https://github.com/opentargets/genetics_etl_python/commit/4d0f0896191b12d31dbc477cfb846d7abd4ba9d1))

* Update documentation/GWAS_ingestion.md

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`8b46024`](https://github.com/opentargets/genetics_etl_python/commit/8b460240b3b237fd20a7868471534fb7ebe2b07d))

* Merge pull request #22 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`1716782`](https://github.com/opentargets/genetics_etl_python/commit/171678294d9dcceb198e4821742b0808b03b9256))

* Merge pull request #21 from opentargets/do_readschema

Merging to prevent distractions. We can fix things as we go ([`654a702`](https://github.com/opentargets/genetics_etl_python/commit/654a702d19b6f9209ae378c07f95ee184ad4bd08))

* Apply suggestions from code review

Co-authored-by: Irene López &lt;45119610+ireneisdoomed@users.noreply.github.com&gt; ([`fa863ab`](https://github.com/opentargets/genetics_etl_python/commit/fa863ab80a78f9cba51b94c440a975ff0f70b891))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/asottile/pyupgrade: v2.38.0 → v2.38.2](https://github.com/asottile/pyupgrade/compare/v2.38.0...v2.38.2) ([`66e504a`](https://github.com/opentargets/genetics_etl_python/commit/66e504aeb29d6e5dee8a236b7575cb11464f4527))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`0f39ed6`](https://github.com/opentargets/genetics_etl_python/commit/0f39ed697d6eed2eb813da6696709a4b9cb3abea))

* Merge branch &#39;main&#39; into do_readschema ([`b4ed33d`](https://github.com/opentargets/genetics_etl_python/commit/b4ed33dea6ca1485fd94d41064c37e6bc7419308))

* Merge pull request #20 from opentargets/do_intervals_fromtarget

Intervals from target ([`cd18a05`](https://github.com/opentargets/genetics_etl_python/commit/cd18a05c5ae40a9856e89a51c58f0fc5130a12b4))

* Merge branch &#39;do_intervals_fromtarget&#39; of https://github.com/opentargets/genetics_etl_python into do_intervals_fromtarget ([`a569fa5`](https://github.com/opentargets/genetics_etl_python/commit/a569fa5cf564a54ae4233967d4db510712f75821))

* Merge pull request #18 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`991f78e`](https://github.com/opentargets/genetics_etl_python/commit/991f78e473ae9cac84283d15116f83f67062e9b8))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`f684c9f`](https://github.com/opentargets/genetics_etl_python/commit/f684c9ffe1c9e7d4b6c94bc657fd576f5059ac81))

* Update launch.json
Removing comment lines from the `launch.json` file. ([`01d7fee`](https://github.com/opentargets/genetics_etl_python/commit/01d7fee9903930418e5aaf9519da8020fa213763))

* Update launch.json

Removing comment lines from the `launch.json` file. ([`7fbd510`](https://github.com/opentargets/genetics_etl_python/commit/7fbd510591480ded149a3e25384525f7e08d7f55))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/asottile/pyupgrade: v2.37.3 → v2.38.0](https://github.com/asottile/pyupgrade/compare/v2.37.3...v2.38.0) ([`3e60d53`](https://github.com/opentargets/genetics_etl_python/commit/3e60d532cf29b90f0561c4877e4e062818ce04d8))

* Merge pull request #17 from opentargets/pre-commit-ci-update-config

[pre-commit.ci] pre-commit autoupdate ([`625df4d`](https://github.com/opentargets/genetics_etl_python/commit/625df4dac3884657f1360fb37ee3042f632a7ea1))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`ec08107`](https://github.com/opentargets/genetics_etl_python/commit/ec081074c5df3bc5b9493842e270845e5746f07a))

* [pre-commit.ci] pre-commit autoupdate

updates:
- [github.com/alessandrojcm/commitlint-pre-commit-hook: v9.0.0 → v9.1.0](https://github.com/alessandrojcm/commitlint-pre-commit-hook/compare/v9.0.0...v9.1.0) ([`2fa250b`](https://github.com/opentargets/genetics_etl_python/commit/2fa250bd0b4673e21c225df3e91bdf907cac5f75))

* Merge pull request #14 from opentargets/ds_gwas_ingest ([`468ad1c`](https://github.com/opentargets/genetics_etl_python/commit/468ad1c3bce3b50a9a88a83bba6d64b4265ba5c4))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`3783db8`](https://github.com/opentargets/genetics_etl_python/commit/3783db81986ebe64fc4a58936ce09179b3ac80ee))

* [pre-commit.ci] auto fixes from pre-commit.com hooks

for more information, see https://pre-commit.ci ([`e9702a7`](https://github.com/opentargets/genetics_etl_python/commit/e9702a73864bf8226583b085cb9732fa20853e3a))

* Merge pull request #13 from opentargets/variant_annot

Variant annotation included with new build system ([`4132dcb`](https://github.com/opentargets/genetics_etl_python/commit/4132dcbe308ef676a9a1d34e655afd4923088fc4))

* Merge branch &#39;main&#39; of https://github.com/opentargets/genetics_etl_python into variant_annot ([`01e4ae4`](https://github.com/opentargets/genetics_etl_python/commit/01e4ae43e763ae3b963e5e097bfd7e699a28be7b))

* Merge pull request #12 from opentargets/do_config

feat: modular hydra configs and other enhancements ([`764efa0`](https://github.com/opentargets/genetics_etl_python/commit/764efa0d5dc0ac84ecb425408045b44d1a05d96c))

* Merge pull request #10 from opentargets/ds_intervals

The main interval script is added to the repo ([`ba08567`](https://github.com/opentargets/genetics_etl_python/commit/ba08567a6500f55ecc47cb36b86636b690d09aab))

* Merge pull request #9 from opentargets/ds_intervals

feat: Adding parsers for intervals dataset. ([`483edf7`](https://github.com/opentargets/genetics_etl_python/commit/483edf72c2bc6f20ef76ec0088a91e30b45cfee6))

* Echo removed. ([`972e6bc`](https://github.com/opentargets/genetics_etl_python/commit/972e6bc3feef0793df995d2e2a92e71e1f643ceb))

* Black sç ([`e30e036`](https://github.com/opentargets/genetics_etl_python/commit/e30e03692290d28dcbc61293a8ccc4383ffabb96))

* Merge pull request #8 from opentargets/do_setupenvironment

Do setupenvironment ([`8f59dc8`](https://github.com/opentargets/genetics_etl_python/commit/8f59dc813338666a44a1dfbdfbbc532a5bd572d8))

* Instructions to setup dev environment ([`804ae79`](https://github.com/opentargets/genetics_etl_python/commit/804ae79bf924acbafe344d01cc9010a37ab3d77c))

* black was missing from dependencies ([`1a2a800`](https://github.com/opentargets/genetics_etl_python/commit/1a2a80019bee2343c32b918e884bcf7b03ad9bbd))

* Merge pull request #7 from opentargets/do_project_rename

generalising project ([`d54414b`](https://github.com/opentargets/genetics_etl_python/commit/d54414b0ebbebda330d53856599cdc130ed8355c))

* generalising project ([`e765611`](https://github.com/opentargets/genetics_etl_python/commit/e7656111b67051b203b7ef905b651c56d2322a3e))

* Merge pull request #4 from opentargets/do_poetry

Managing build and dependencies using poetry + makefile ([`3835fc8`](https://github.com/opentargets/genetics_etl_python/commit/3835fc8710bf5753babba70078f81fbd85e64aea))

* Action no longer runs ([`44f9c6d`](https://github.com/opentargets/genetics_etl_python/commit/44f9c6d134a3c14e26b056428803fb806ac22df9))

* Merge pull request #6 from opentargets/do_all_lints

pre-commit replaces GitHub actions for all code quality checks ([`2074a0c`](https://github.com/opentargets/genetics_etl_python/commit/2074a0c934e998714bf30dbe9363f9b11b0509a6))

* pre-commit including and code adjusted to new changes ([`80b4aa2`](https://github.com/opentargets/genetics_etl_python/commit/80b4aa212554b4b9029a13cc21ed191be1f091fb))

* Trying single action ([`d44ca2f`](https://github.com/opentargets/genetics_etl_python/commit/d44ca2f58f97634b9b11b3da16bec1d7fd90132e))

* poetry version added for caching purposes ([`991234f`](https://github.com/opentargets/genetics_etl_python/commit/991234fd6b1916f15287346bf36cbeb0ed717d59))

* more indentation problems ([`36e4560`](https://github.com/opentargets/genetics_etl_python/commit/36e4560d76c8ac95347cea67698ca7b28741e0a6))

* indentation problem ([`3f9fd30`](https://github.com/opentargets/genetics_etl_python/commit/3f9fd30544210e761c694580582f9057062a7fba))

* pylint workflow updated ([`45adf32`](https://github.com/opentargets/genetics_etl_python/commit/45adf325ca106bdbf8531237a40d1f6b57ae7796))

* unnecessary code ([`72d4dc4`](https://github.com/opentargets/genetics_etl_python/commit/72d4dc4dc19f1a4fcbe497c115286a39bc5ca755))

* changes on dependencies ([`74d6947`](https://github.com/opentargets/genetics_etl_python/commit/74d694717d41068df0a73b9d5140ca6898a91f25))

* spaces over tabs ([`f1d36c0`](https://github.com/opentargets/genetics_etl_python/commit/f1d36c0194cccc3903510b36b806505fe5d76714))

* Problem renaming variables ([`138508a`](https://github.com/opentargets/genetics_etl_python/commit/138508a8dd86769ccfa45c27d067294b0a35dcc4))

* Minimum python version downgraded ([`a5477b4`](https://github.com/opentargets/genetics_etl_python/commit/a5477b4078372191d6ae37969eef17839959786e))

* add init ([`035f45d`](https://github.com/opentargets/genetics_etl_python/commit/035f45d6d919d19c4d1e1a2a4f67d785c842c555))

* updating pylint test to work with poetry ([`6f17ea3`](https://github.com/opentargets/genetics_etl_python/commit/6f17ea30ffd843fe9aa25a0ef0222613baf2fe6c))

* More meaningful help ([`5c8e23e`](https://github.com/opentargets/genetics_etl_python/commit/5c8e23e9646151dcc01a049531db49bc679d99e7))

* more isort changes ([`fe46a77`](https://github.com/opentargets/genetics_etl_python/commit/fe46a7738aed135dacd3d4adb7a32361b16f0158))

* Changes to comply with isort ([`1155e96`](https://github.com/opentargets/genetics_etl_python/commit/1155e96373013643d14ddc8ec37e1fab73044032))

* Unnecessary comments ([`c94fcef`](https://github.com/opentargets/genetics_etl_python/commit/c94fceff13c67ea750e17e8f89bdde62abaa0eb4))

* unused import ([`53632c3`](https://github.com/opentargets/genetics_etl_python/commit/53632c3065f5803de40757daaff7749541899b99))

* build version works ([`be23652`](https://github.com/opentargets/genetics_etl_python/commit/be236526e19328ddb8700831eac5a3e684ba6981))

* unnecessary ([`3943ba8`](https://github.com/opentargets/genetics_etl_python/commit/3943ba88ccded27a20ec2af7a85734b5557d90b0))

* renamed config file ([`24f0167`](https://github.com/opentargets/genetics_etl_python/commit/24f01677125a5226f073d4ee3db787f4c2a37398))

* right project dependencies structure ([`eb63079`](https://github.com/opentargets/genetics_etl_python/commit/eb63079784f723c1be2735b94554013f879ec61b))

* updates to gitignore ([`f36b993`](https://github.com/opentargets/genetics_etl_python/commit/f36b9930e4632f684310c3fc04232a9d08c3e682))

* changes to gitignore ([`83e2854`](https://github.com/opentargets/genetics_etl_python/commit/83e2854145bcee3d6d98d3cfe5bd4ff9e1d7d215))

* Adding gitignore ([`8fddac8`](https://github.com/opentargets/genetics_etl_python/commit/8fddac8e291b6db4ddec9ed62dee2d927ce01992))

* pyspark as development dependency ([`e3cbe1d`](https://github.com/opentargets/genetics_etl_python/commit/e3cbe1d9be15d70e934ab4228418f06efc380e2f))

* No longer required with Poetry ([`895f3df`](https://github.com/opentargets/genetics_etl_python/commit/895f3df3d32c3c88c13feb294f159a42465475c8))

* Fixing imports based on new strucure ([`d3e21d3`](https://github.com/opentargets/genetics_etl_python/commit/d3e21d3b87571a36f9395b014d7031f97546a54e))

* Update pylint action to use poetry ([`4300678`](https://github.com/opentargets/genetics_etl_python/commit/43006780a114402bc29ad9b1eb41d05dacf707d9))

* Update dev dependencies ([`7404401`](https://github.com/opentargets/genetics_etl_python/commit/74044014b992cc820503e7f83b2685312d56f75d))

* New structure and poetry lock ([`89aaf46`](https://github.com/opentargets/genetics_etl_python/commit/89aaf46702e7bd08e8a6bf0e8ef51313ea58bf76))

* No longer necessary ([`2d318f0`](https://github.com/opentargets/genetics_etl_python/commit/2d318f03eb7d5773c28fc5142cb09f5377a53628))

* simplify name ([`94deb08`](https://github.com/opentargets/genetics_etl_python/commit/94deb0840c4532ebfb38200c367ab76e56bcf662))

* simplify name ([`ab8f63b`](https://github.com/opentargets/genetics_etl_python/commit/ab8f63b36b1489e4750a6d05ee42546b6e64a3ae))

* Too many badges ([`df0fdb2`](https://github.com/opentargets/genetics_etl_python/commit/df0fdb2e45d422b5144f8893f0b2df270d5e9bd2))

* Merge pull request #2 from opentargets/do_black_action

Do black action ([`fcd611a`](https://github.com/opentargets/genetics_etl_python/commit/fcd611aca54a296442cae54247c2f80b7e3cbe28))

* pylinting and badges ([`9cb1966`](https://github.com/opentargets/genetics_etl_python/commit/9cb196611474508ff763c2dd3b98b60768ee3251))

* missing docstrings ([`7132519`](https://github.com/opentargets/genetics_etl_python/commit/7132519b9f432e5cc679da31aa80934a4a703865))

* Improving dependencies ([`ab879a5`](https://github.com/opentargets/genetics_etl_python/commit/ab879a58e8156fb6b329279b4d50cb6bbd01356b))

* add requirements install to pylint ([`ab28c94`](https://github.com/opentargets/genetics_etl_python/commit/ab28c9466f702ccfed0ec6a618238e1832c935b6))

* Merge pull request #3 from opentargets:do_lintchanges

Do_lintchanges ([`556b0bb`](https://github.com/opentargets/genetics_etl_python/commit/556b0bbc9abf44e32f49750149c8a4bef5bbf1bd))

* lint compliance ([`1f0e584`](https://github.com/opentargets/genetics_etl_python/commit/1f0e5840a37ab24785debbe7703aaa6936d03362))

* adjusting lint ([`1b123f6`](https://github.com/opentargets/genetics_etl_python/commit/1b123f682e3d63eeacf6ac825354558d04888257))

* Black and pylint added ([`43f47ae`](https://github.com/opentargets/genetics_etl_python/commit/43f47aecf1494821a8b6d0f99216fe1ae8459ae3))

* pylint action ([`9478db8`](https://github.com/opentargets/genetics_etl_python/commit/9478db813e2f4aef06dfda54048a0e75bceea820))

* Black fixed ([`dfc4cc1`](https://github.com/opentargets/genetics_etl_python/commit/dfc4cc19dd6c0f6d7aad625883f840bfe491854a))

* Checking gh action works III ([`ed0edc2`](https://github.com/opentargets/genetics_etl_python/commit/ed0edc2087ae785c68a12e63b3258c7c01fd3e7f))

* Checking gh action works II ([`e84f4df`](https://github.com/opentargets/genetics_etl_python/commit/e84f4df077e3a57b3deee037746a9a712f3667a7))

* Merge branch &#39;do_black_action&#39; of https://github.com/opentargets/genetics_spark_coloc into do_black_action ([`b2b85d2`](https://github.com/opentargets/genetics_etl_python/commit/b2b85d2734a80fd1495265a0fc2cfb56f952bb5e))

* Checking gh action works ([`e057b23`](https://github.com/opentargets/genetics_etl_python/commit/e057b23c4bf7d42a18ce37c44ee0acc52d842955))

* Remove options ([`6790860`](https://github.com/opentargets/genetics_etl_python/commit/67908609e7586dd0cc3c9f3496fb6d40bcae42a5))

* Trying black integration ([`6776309`](https://github.com/opentargets/genetics_etl_python/commit/677630916a93c6e85a050e4add333bf7ecf96af2))

* unnecessary code removed ([`1d2d971`](https://github.com/opentargets/genetics_etl_python/commit/1d2d971c6dd3b98dfc126a5260e38aa221ad0d5e))

* working version scaled to 64 cores ([`7072332`](https://github.com/opentargets/genetics_etl_python/commit/7072332f7b34c878f0af0f6beeadea8fd1feff67))

* Rename function ([`00ee91a`](https://github.com/opentargets/genetics_etl_python/commit/00ee91a19f52a3c8485ef3e4f12bd838cd8fef96))

* TODO resolved ([`27f0f73`](https://github.com/opentargets/genetics_etl_python/commit/27f0f7315b8de6224543ff4f090de1a565a97354))

* Merge pull request #1 from mkarmona:mkarmona-patch-1

improve join conditions and better hash function ([`e30e811`](https://github.com/opentargets/genetics_etl_python/commit/e30e8115d1f51a74b8a873b640663b556c300f85))

* including chromosome in join ([`b9a4589`](https://github.com/opentargets/genetics_etl_python/commit/b9a45890aed8c2f335e52b7fa2ffbdb57fa9c4f3))

* Comment out filtering by chromosome ([`7c1d2e2`](https://github.com/opentargets/genetics_etl_python/commit/7c1d2e28abdc8094c96173011aba31b6c1f413ff))

* improve join conditions and better hash function ([`3088c37`](https://github.com/opentargets/genetics_etl_python/commit/3088c370081c8583d95806682a7e2b58e9afce45))

* duplicated column ([`477239c`](https://github.com/opentargets/genetics_etl_python/commit/477239c7af835c9e38027b03dca0bba590ac20ca))

* simpler execution ([`d7f27a1`](https://github.com/opentargets/genetics_etl_python/commit/d7f27a1026580938e6922989153163ee166803bd))

* Merge branch &#39;main&#39; of https://github.com/d0choa/genetics_spark_coloc ([`2c1de88`](https://github.com/opentargets/genetics_etl_python/commit/2c1de88fc8803e103020c82f535ef5722e56c63c))

* Feedback from mk ([`10f09b8`](https://github.com/opentargets/genetics_etl_python/commit/10f09b8c8e999f6df8848f09556e1a31c3c8c79b))

* duplicated row ([`693e32d`](https://github.com/opentargets/genetics_etl_python/commit/693e32dfcd3c60dc69c958e99b3d63b0b13e7c43))

* Bug fixing ([`5b6217c`](https://github.com/opentargets/genetics_etl_python/commit/5b6217c6d978490227fae6ad958eeb0ff4f72c05))

* ammend comment ([`ea7adf5`](https://github.com/opentargets/genetics_etl_python/commit/ea7adf567a7ae73a274298985d86ac5986613f9c))

* Clean up (not tested) ([`e99ba28`](https://github.com/opentargets/genetics_etl_python/commit/e99ba28dde19e41fd9603ff2fb4001e51ec42e22))

* Working version of self-join ([`8f41180`](https://github.com/opentargets/genetics_etl_python/commit/8f41180332ee9bace346a1ffd5eadeb3deb8316e))

* indentation fixed ([`f420aa1`](https://github.com/opentargets/genetics_etl_python/commit/f420aa10785a0addb6f666b4b539bc3f8a216716))

* add expected results ([`751c4a9`](https://github.com/opentargets/genetics_etl_python/commit/751c4a98795beef51877858f5a04bce7914d4010))

* reverting changes to work with vectors ([`b4eb54e`](https://github.com/opentargets/genetics_etl_python/commit/b4eb54e74b71180cb87c97811010b9059523e4ee))

* coloc running version ([`9042b69`](https://github.com/opentargets/genetics_etl_python/commit/9042b69e92bfbdb07a86cc31c498fa597808885e))

* Not tested version ([`f620947`](https://github.com/opentargets/genetics_etl_python/commit/f620947ad268371f215bdce10414b9107360766c))

* unnecessary on clause removed ([`36a5798`](https://github.com/opentargets/genetics_etl_python/commit/36a5798b2953f6ce1bebcd3840b0a2d88ca6e595))

* New approach: self-join, using the complex “on” clause, creating the nested object with all the tag variants ([`13580ca`](https://github.com/opentargets/genetics_etl_python/commit/13580ca00c9fdfaae8221714d5ad9f9665ea3fc2))

* Coloc using windows instead of groupby ([`955e30c`](https://github.com/opentargets/genetics_etl_python/commit/955e30c56a77075358ad45141272a930b11af628))

* distinct is not necessary ([`fd6a639`](https://github.com/opentargets/genetics_etl_python/commit/fd6a63951485830b039b58d871f1687290e4700d))

* config based on chr22 partitiiooned dataset ([`31f2d24`](https://github.com/opentargets/genetics_etl_python/commit/31f2d245af0658849f77a78bd81327a7cc5c440e))

* More clear to select than to drop ([`dbb88ba`](https://github.com/opentargets/genetics_etl_python/commit/dbb88bace89987caf8283d9546906c75d669b92c))

* separation between py files and yaml ([`220c613`](https://github.com/opentargets/genetics_etl_python/commit/220c613f2ae501d26a36d860ff89e3befbc41855))

* more leeway before destroying machine ([`02f59cb`](https://github.com/opentargets/genetics_etl_python/commit/02f59cbfd2ea55e90a74ca6689ccac1711b677c2))

* to allow spark debugging interfaces ([`0c51c7f`](https://github.com/opentargets/genetics_etl_python/commit/0c51c7f6bc4fe3ab8f432a6c74e87350e56e1a61))

* partial improvements ([`9484c45`](https://github.com/opentargets/genetics_etl_python/commit/9484c45026b8e4ffff4218e3ed360e58700bf082))

* incomplete on in join ([`8834a44`](https://github.com/opentargets/genetics_etl_python/commit/8834a44079205c03380d4dd06555acd701914ff7))

* improvements on carrying over info from credset ([`909a17f`](https://github.com/opentargets/genetics_etl_python/commit/909a17fe6563bf6a0c45926bb3ae854ec9e63287))

* unstable: playing with structure ([`90eb673`](https://github.com/opentargets/genetics_etl_python/commit/90eb6730023637606a86f159990edf554148eef8))

* Trying to carry variant/study metadata ([`fc9197c`](https://github.com/opentargets/genetics_etl_python/commit/fc9197c7f9db06157720e1204f11b595871a390e))

* comment ([`a77e808`](https://github.com/opentargets/genetics_etl_python/commit/a77e808ef98bed167a9785cc029b22baca431f51))

* more readable ([`1610438`](https://github.com/opentargets/genetics_etl_python/commit/1610438407b869f2b38dfcb50399785de19da3cd))

* Restructuring the project ([`77cf939`](https://github.com/opentargets/genetics_etl_python/commit/77cf9395756a84e42ecfcc0804a15d1041ace049))

* typo fixed ([`9c0e3f6`](https://github.com/opentargets/genetics_etl_python/commit/9c0e3f61130f54029bcd6aad84b49715068b6a53))

* 32 cores is more realistic at the moment ([`0b70026`](https://github.com/opentargets/genetics_etl_python/commit/0b70026c77e24cbfe4a12031c0894e0b03276b18))

* unnecessary nesting removed ([`b8e63f7`](https://github.com/opentargets/genetics_etl_python/commit/b8e63f7c9bcfc9ccb027be19507fcd4fbd15efaf))

* avoiding crossJoin to best manage partitions ([`440f11d`](https://github.com/opentargets/genetics_etl_python/commit/440f11d927a3e34ec983a631f938e500adb89419))

* explain ([`f03e2be`](https://github.com/opentargets/genetics_etl_python/commit/f03e2beb648056c03c738255cbf01d1096c6da68))

* removing unnecesary persist ([`6310944`](https://github.com/opentargets/genetics_etl_python/commit/6310944dded491d4359825542592366a37f35019))

* machine definition and submission now work ([`f3f96da`](https://github.com/opentargets/genetics_etl_python/commit/f3f96da1d426990bccc35e45c358c36e4c4bfccf))

* Bug writing file ([`10840dc`](https://github.com/opentargets/genetics_etl_python/commit/10840dc53a3c3cf5f20267a2a1ababb9f560a160))

* Slimmer version to check performance ([`5ed86b2`](https://github.com/opentargets/genetics_etl_python/commit/5ed86b2e9fd946043775d56d16d2151dcc68ff47))

* trying to make hydra fly ([`bc14e71`](https://github.com/opentargets/genetics_etl_python/commit/bc14e71bf1750b52a3245636949bb7ba670d248e))

* Still not working version ([`b312857`](https://github.com/opentargets/genetics_etl_python/commit/b3128576969811f51b3906cbabd57fea6e37d31c))

* Initial commit ([`8a29b09`](https://github.com/opentargets/genetics_etl_python/commit/8a29b09a42e32a7a64b6eb09f6a5b5a4dbe19298))
