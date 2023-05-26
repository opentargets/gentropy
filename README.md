[![status: experimental](https://github.com/GIScience/badges/raw/master/status/experimental.svg)](https://github.com/GIScience/badges#experimental)
[![docs](https://github.com/opentargets/genetics_etl_python/actions/workflows/docs.yaml/badge.svg)](https://opentargets.github.io/genetics_etl_python/)
[![codecov](https://codecov.io/gh/opentargets/genetics_etl_python/branch/main/graph/badge.svg?token=5ixzgu8KFP)](https://codecov.io/gh/opentargets/genetics_etl_python)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/opentargets/genetics_etl_python/main.svg)](https://results.pre-commit.ci/badge/github/opentargets/genetics_etl_python)

# Genetics Portal Data Pipeline (experimental)
- [Documentation](https://opentargets.github.io/genetics_etl_python/)

## One-time configuration
The steps in this section only ever need to be done once on any particular system.

Google Cloud configuration:
1. Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install.
1. Log in to your work Google Account: run `gcloud auth login` and follow instructions.
1. Obtain Google application credentials: run `gcloud auth application-default login` and follow instructions.

Check that you have the `make` utility installed, and if not (which is unlikely), install it using your system package manager.

Ensure that you have `java` version 8 installed on your system (also known as java 1.8).

## Environment configuration
Run `make setup-dev` to install/update the necessary packages and activate the development environment. You need to do this every time you open a new shell.

It is recommended to use VS Code as an IDE for development.

## How to run the code
All pipelines in this repository are intended to be run in Google Dataproc. Running them locally is not currently supported.

In order to run the code:
1. Manually edit your local [`workflow/dag.yaml`](workflow/dag.yaml) file and comment out the steps you do not want to run.
2. Manually edit your local [`pyproject.toml`](pyproject.toml) file and modify the version of the code.
  - This must be different from the version used by any other people working on the repository to avoid any deployment conflicts, so it's a good idea to use your name, for example: `1.2.3+jdoe`.
  - You can also add a brief branch description, for example: `1.2.3+jdoe.myfeature`.
  - Note that the version must comply with [PEP440 conventions](https://peps.python.org/pep-0440/#normalization), otherwise Poetry will not allow it to be deployed.
3. Run `make build`.
  - This will create a bundle containing the neccessary code, configuration and dependencies to run the ETL pipeline, and then upload this bundle to Google Cloud.
  - A version specific subpath is used, so uploading the code will not affect any branches but your own.
  - If there was already a code bundle uploaded with the same version number, it will be replaced.
4. Submit the Dataproc job with `poetry run python workflow/workflow_template.py`
  - You will need to specify additional parameters, some are mandatory and some are optional. Run with `--help` to see usage.
  - The script will provision the cluster and submit the job.
  - The cluster will take a few minutes to get provisioned and running, during which the script will not output anything, this is normal.
  - Once submitted, you can monitor the progress of your job on this page: https://console.cloud.google.com/dataproc/jobs?project=open-targets-genetics-dev.
  - On completion (whether successful or a failure), the cluster will be automatically removed, so you don't have to worry about shutting it down to avoid incurring charges.

## Troubleshooting
In some cases, Pyenv and Poetry may cause various exotic errors which are hard to diagnose and get rid of. In this case, it helps to remove them from the system completely before running the `make setup-dev` command. See instructions in [utils/remove_pyenv_poetry.md](utils/remove_pyenv_poetry.md).

If you see errors related to BLAS/LAPACK libraries, see [this StackOverflow post](https://stackoverflow.com/questions/69954587/no-blas-lapack-libraries-found-when-installing-scipy) for more info.
