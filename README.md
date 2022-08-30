[![status: experimental](https://github.com/GIScience/badges/raw/master/status/experimental.svg)](https://github.com/GIScience/badges#experimental)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# Genetics Portal Python ETL

Genetics portal ETL steps (Python)

### Requirements

- [Poetry](https://python-poetry.org/docs/)
- gcloud installed and authorised to your GCP Project
- gsutil
- [make](https://www.gnu.org/software/make/) build tool

### Setup development environment

```
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


### More help

```bash
make help
```
