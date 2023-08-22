# Environment configuration and contributing changes

## One-time configuration
The steps in this section only ever need to be done once on any particular system.

Google Cloud configuration:
1. Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install.
1. Log in to your work Google Account: run `gcloud auth login` and follow instructions.
1. Obtain Google application credentials: run `gcloud auth application-default login` and follow instructions.

Check that you have the `make` utility installed, and if not (which is unlikely), install it using your system package manager.

Check that you have `java` installed.

## Environment configuration
Run `make setup-dev` to install/update the necessary packages and activate the development environment. You need to do this every time you open a new shell.

It is recommended to use VS Code as an IDE for development.

## How to run the code
All pipelines in this repository are intended to be run in Google Dataproc. Running them locally is not currently supported.

In order to run the code:

1. Manually edit your local [`workflow/dag.yaml`](../workflow/dag.yaml) file and comment out the steps you do not want to run.

2. Manually edit your local [`pyproject.toml`](../pyproject.toml) file and modify the version of the code.
    - This must be different from the version used by any other people working on the repository to avoid any deployment conflicts, so it's a good idea to use your name, for example: `1.2.3+jdoe`.
    - You can also add a brief branch description, for example: `1.2.3+jdoe.myfeature`.
    - Note that the version must comply with [PEP440 conventions](https://peps.python.org/pep-0440/#normalization), otherwise Poetry will not allow it to be deployed.
    - Do not use underscores or hyphens in your version name. When building the WHL file, they will be automatically converted to dots, which means the file name will no longer match the version and the build will fail. Use dots instead.

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

## How to generate a local copy of the documentation
Run `poetry run mkdocs serve`. This will generate the local copy of the documentation and will start a local server to browse it (URL will be printed, usually http://127.0.0.1:8000/).

## How to run the tests
Run `poetry run pytest`.

## Contributing checklist
When making changes, and especially when implementing a new module or feature, it's essential to ensure that all relevant sections of the code base are modified.

### Documentation
* If during development you had a question which wasn't covered in the documentation, and someone explained it to you, add it to the documentation. The same applies if you encountered any instructions in the documentation which were obsolete or incorrect.
* Documentation autogeneration expressions start with `:::`. They will automatically generate sections of the documentation based on class and method docstrings. Be sure to update them for:
  + Dataset definitions in `docs/reference/dataset` (example: `docs/reference/dataset/study_index/study_index_finngen.md`)
  + Step definitions in `docs/reference/step` (example: `docs/reference/step/finngen.md`)

### Configuration
* Input and output paths in `config/datasets/gcp.yaml`
* Step configuration in `config/step/my_STEP.yaml` (example: `config/step/my_finngen.yaml`)

### Classes
* Step configuration class in `src/org/config.py` (example: `FinnGenStepConfig` class in that module)
* Dataset class in `src/org/dataset/` (example: `src/otg/dataset/study_index.py` → `StudyIndexFinnGen`)
* Step main running class in `src/org/STEP.py` (example: `src/org/finngen.py`)

### Tests
* Test study fixture in `tests/conftest.py` (example: `mock_study_index_finngen` in that module)
* Test sample data in `tests/data_samples` (example: `tests/data_samples/finngen_studies_sample.json`)
* Test definition in `tests/` (example: `tests/dataset/test_study_index.py` → `test_study_index_finngen_creation`)
