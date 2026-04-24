# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**gentropy** is Open Targets' Python framework for post-GWAS analysis. It harmonizes, statistically analyzes, and prioritizes genetic signals to assist drug discovery. The codebase is built on **PySpark** with **Hail** for genomic data processing, using **Hydra** for configuration management.

## Key Architecture

### Core Abstractions

- **`Dataset`** (`src/gentropy/dataset/dataset.py`) - Abstract base class wrapping a PySpark DataFrame with schema validation. All dataset types inherit from this. Provides `read()`, `from_parquet()`, `filter()`, `validate_schema()`, `valid_rows()` (QC filtering), and partitioning utilities.

- **`Session`** (`src/gentropy/common/session.py`) - SparkSession wrapper with custom config (write mode, output partitions, Hail setup, BGZIP codec, dynamic allocation). Use `Session.find()` to get the active session.

- **`config.py`** (`src/gentropy/config.py`) - Hydra config store with dataclass-based step configurations. All steps are registered via `register_config()`. Run `gentropy.cli:main` via Hydra to execute steps.

- **CLI** (`src/gentropy/cli.py`) - Entry point `gentropy` command. Uses `@hydra.main` decorator with `instantiate(cfg.step)` to run the configured pipeline step.

### Directory Structure

```
src/gentropy/
  cli.py                    # CLI entry point (Hydra)
  config.py                 # All step configurations (dataclasses)
  common/
    session.py              # SparkSession wrapper (core runtime)
    schemas.py              # Schema validation utilities
    spark.py, udf.py, stats.py, genomic_region.py
  dataset/                  # Dataset types (DataFrame wrappers)
    dataset.py              # Base Dataset ABC
    study_locus.py, summary_statistics.py, variant_index.py
    study_index.py, colocalisation.py, l2g_*.py, molecular_complex.py
    l2g_features/           # Locus-to-gene feature engineering
  datasource/               # External data sources (FINNGEN, gnomAD, GWAS Catalog, etc.)
  method/                   # Algorithms (finemapping, clumping, QC, colocalisation)
    colocalisation/, l2g/   # Subdirectories for complex methods
  external/                 # Cloud storage (GCS, S3)
  assets/                   # Schemas, data files, log4j config
```

Top-level Python files (`pics.py`, `finngen_studies.py`, `susie_finemapper.py`, etc.) are **Step classes** - each orchestrates a pipeline stage by reading datasets, applying methods, and writing results.

### Locus-to-Gene (L2G)

The L2G system (`src/gentropy/l2g.py`, `src/gentropy/method/l2g/`) predicts gene-causal relationships using XGBoost models. Key files:
- `LocusToGeneFeatureMatrixStep` - builds feature matrices from credible sets
- `LocusToGeneStep` - trains/evaluates models
- `LocusToGeneEvidenceStep` / `LocusToGeneAssociationsStep` - generates evidence/associations output

## Development Commands

### Setup
```bash
uv sync                    # Install dependencies
uv run pre-commit install  # Install pre-commit hooks
```

### Linting & Formatting
```bash
make check                 # Run ruff + pydoclint
uv run ruff check src/gentropy .   # Lint only
uv run ruff format src/gentropy .  # Format only
uv run pydoclint --config=pyproject.toml src  # Docstring lint
```

### Type Checking
```bash
uv run mypy src/gentropy   # Semistrict mypy (see pyproject.toml [tool.mypy])
```

### Tests
```bash
make test                  # Full test suite (combined coverage)
uv run pytest -m 'not download_jars_from_web and not no_shared_spark'  # Default run
uv run pytest tests/gentropy/dataset/ -v --no-cov  # Single module, no coverage
uv run pytest tests/gentropy/dataset/test_study_locus.py -v --no-cov -k test_valid_rows  # Single test
make test-no-shared-spark-session   # Tests isolated from shared SparkSession
```

Test markers: `step_test`, `download_jars_from_web`, `no_shared_spark`

### Build & Documentation
```bash
make build                 # Build Python package (uv build)
make build-documentation   # Start local mkdocs server (uv run mkdocs serve)
make build-docker          # Build Docker image
```

### Pre-commit Hooks
The repo uses pre-commit with: ruff, ruff-format, mypy, interrogate, pydocstyle, pydoclint, yamllint, commitlint (conventional commits), uv-lock check. Run `pre-commit run --all-files` to validate.

### Post-Change Verification
After any changes that touch multiple files or cross-references, always run both:
```bash
make test                  # Full test suite
uv run pre-commit run --all-files  # All pre-commit hooks
```
This ensures no import breakage, linting issues, or type errors are introduced.

## Coding Conventions

- **Python 3.11-3.13**, Google-style docstrings (pydocstyle convention)
- **Type annotations** required everywhere (mypy semistrict: no implicit optional, no re-export, disallow generics, etc.)
- **Schema validation** at Dataset construction time via `validate_schema()` and `compare_struct_schemas()`
- **QC flags** use `@qc_test` decorator on methods; datasets expose `get_QC_mappings()` and `get_QC_column_name()`
- **Hydra configs** use `_target_` field for instantiation; MISSING fields are required at runtime
- **All external data sources** live under `datasource/` with subpackages per source (finngen, gnomad, gwas_catalog, etc.)
