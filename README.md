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

Ensure python version described in `.python-version` is available

```bash
pyenv versions
```

Otherwise, install

```bash
pyenv install 3.10.8
```

Make sure you are using the local Python version

``` bash
python -V
poetry env use 3.10.8
```

``` bash
make setup-dev

#VS-code
code . #...and select interpreter
```

### Build

Use `make build` to create a bundle that will contain the neccessary code, configuration and dependencies to run the ETL pipeline. The build is stored in `dist/` (gitignored).

### GCP Dataproc workflow

A full dataproc workflow DAG can be triggered using the `workflow/workflow_template.py`
