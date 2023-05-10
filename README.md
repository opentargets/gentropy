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

## Environment configuration
Run `make setup-dev` to install/update the necessary packages and activate the development environment. You need to do this every time you open a new shell.

It is recommended to use VS Code as an IDE for development.

## How to run the code
All pipelines in this repository are intended to be run in Google Dataproc. Running them locally is not currently supported.

In order to run the code:
1. Manually edit your local [`workflow/dag.yaml`](workflow/dag.yaml) file and comment out the steps you do not want to run.
2. Manually edit your local [`workflow/workflow_template.py`](workflow/workflow_template.py) file and change the following parameters:
  - `cluster_name` and `template_id` to reflect your own name/initials
  - `machine_type` to set the VM size suitable for your workflow
3. Run `make build`. This will create a bundle that will contain the neccessary code, configuration and dependencies to run the ETL pipeline, and then upload this bundle to Google Cloud. **Note:** currently, there is just one upload path which is shared by all users. So when you upload the code, it replaces a version uploaded previously by anyone else.
4. Submit the Dataproc job: `poetry run python workflow/workflow_template.py`. You can then monitor the progress of your job on this page: https://console.cloud.google.com/dataproc/jobs?project=open-targets-genetics-dev.

## Troubleshooting
In some cases, Pyenv and Poetry may cause various exotic errors which are hard to diagnose and get rid of. In this case, it helps to remove them from the system completely before running the `make setup-dev` command. See instructions in [utils/remove_pyenv_poetry.md](utils/remove_pyenv_poetry.md).

If you see errors related to BLAS/LAPACK libraries, see [this StackOverflow post](https://stackoverflow.com/questions/69954587/no-blas-lapack-libraries-found-when-installing-scipy) for more info.
