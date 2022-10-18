[![status: experimental](https://github.com/GIScience/badges/raw/master/status/experimental.svg)](https://github.com/GIScience/badges#experimental)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![codecov](https://codecov.io/gh/opentargets/genetics_etl_python/branch/main/graph/badge.svg?token=5ixzgu8KFP)](https://codecov.io/gh/opentargets/genetics_etl_python)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/opentargets/genetics_etl_python/main.svg)](https://results.pre-commit.ci/badge/github/opentargets/genetics_etl_python)

# Genetics Portal Python ETL

Genetics portal ETL steps (Python)

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
pyenv install 3.8.13
```

Make sure you are using the local Python version

``` bash
python -V
#Python 3.8.13
poetry env use 3.8
```

``` bash
make setup-dev

#VS-code
code . #...and select interpreter
```

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

### Development guidelines

- DataFrame schemas must be validated when reading (`etl.read_parquet`) and writing (`validate_df_schema`).
- ...
### More help

```bash
make help
```
