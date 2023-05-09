[![status: experimental](https://github.com/GIScience/badges/raw/master/status/experimental.svg)](https://github.com/GIScience/badges#experimental)
[![docs](https://github.com/opentargets/genetics_etl_python/actions/workflows/docs.yaml/badge.svg)](https://opentargets.github.io/genetics_etl_python/)
[![codecov](https://codecov.io/gh/opentargets/genetics_etl_python/branch/main/graph/badge.svg?token=5ixzgu8KFP)](https://codecov.io/gh/opentargets/genetics_etl_python)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/opentargets/genetics_etl_python/main.svg)](https://results.pre-commit.ci/badge/github/opentargets/genetics_etl_python)

# Genetics Portal Data Pipeline (experimental)

- [Documentation](https://opentargets.github.io/genetics_etl_python/)

## Development

### Requirements

- [pyenv](https://github.com/pyenv/pyenv)
- [Poetry](https://python-poetry.org/docs/)
- gcloud installed and authorised to your GCP Project
- gsutil
- [make](https://www.gnu.org/software/make/) build tool
- OpenBLAS and LAPACK libraries (for scipy). [more info](https://stackoverflow.com/questions/69954587/no-blas-lapack-libraries-found-when-installing-scipy)

### Setup development environment

Run `make setup-dev`; it will install or update the necessary packages and activate the environment.

Then run VS Code (`code`) and select the interpreter.

In some cases, Pyenv and Poetry may cause various exotic errors which are hard to diagnose and get rid of. In this case, it helps to remove them from the system completely before running the `make setup-dev` command. See instructions in [utils/remove_pyenv_poetry.md](utils/remove_pyenv_poetry.md).

### Configuration

We use [hydra](https://hydra.cc) for managing the ETL configuration. The `configs` directory contains the source YAMLs neccessary to produce an instance of the configuration. To manually run an instance of the configuration run:

```bash
poetry run python ./utils/configure.py --cfg job # add --resolve to resolve interpolations
```

A local instance of the configuration file can be used in `src/config.yaml` for debugging purposes (gitignored).

Configurations can be modified using hydra options.

```bash
poetry run python ./utils/configure.py environment=local
```

### Build

Use `make build` to create a bundle that will contain the neccessary code, configuration and dependencies to run the ETL pipeline. The build is stored in `dist/` (gitignored).

### GCP Dataproc workflow

A full dataproc workflow DAG can be triggered using the `workflow/workflow_template.py`
