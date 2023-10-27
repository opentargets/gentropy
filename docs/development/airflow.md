# Airflow workflow

The next section describes how to run Airflow workflows locally while performing computation in Google Cloud Platform. This is useful for testing and debugging, but for production use, we recommend running Airflow on a dedicated server.

## Pre-requisites

- [Docker](https://docs.docker.com/get-docker/)
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)

!!!warning macOS Docker memory allocation
    If you are working on a macOS, the default amount of memory available for Docker might not bet enough to get Airflow up and running. You should allocate at least 4GB of memory for the Docker Engine (ideally 8GB). [More info](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#)


## Configure Airflow access to Google Cloud Platform

Run the next command with the appropriate <PROJECT_ID> to ensure you have Google default application credentials set up:

```bash
gcloud auth application-default login --project=<PROJECT_ID>
```

Next, create the service account key file that will be used by Airflow to access Google Cloud Platform resources. The next command will create a file at `~/.config/gcloud/service_account_credentials.json` using the specified IAM account which needs to have the required priviledges to access the required GCP resources.

```bash
gcloud iam service-accounts keys create ~/.config/gcloud/service_account_credentials.json --iam-account=open-targets-genetics-dev@appspot.gserviceaccount.com
```

## Airflow

All subsequent steps are relative to the `src/airflow` folder:

```bash
cd src/airflow
```

###  Build Docker image

To extend the default airflow image with the required libraries and credentials.

!!!note "Note"
    The Dockerfile extends the official [Airflow Docker Compose YAML](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml). We add support for the Google Cloud SDK, Google Airflow operators and access to GCP credentials.

```bash
# Build the image in the Dockerfile and name it extending_airflow and version it as latest
docker build . --tag extending_airflow:latest
```

###  Initialise

Before starting Airflow, we need to initialise the database:

```bash
docker compose up airflow-init
```

Now you can start all services:

```bash
docker compose up -d
```

Airflow UI will now be available at `http://localhost:8080/`. Default username and password are both `airflow`.
For additional information on how to use Airflow visit the [official documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html).


### Cleaning up

At any time, you can check the status of your containers with:

```bash
docker ps
```

To stop Airflow, run:

```bash
docker compose down
```

To cleanup the Airflow database, run:

```bash
docker compose down --volumes --remove-orphans
```

### Advanced configuration

More information on running Airflow with Docker Compose can be found in the [official docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

1. **Increase Airflow concurrency**. Modify the `docker-compose.yaml` and add the following to the x-airflow-common → environment section:

```yaml
AIRFLOW__CELERY__WORKER_CONCURRENCY: 32
AIRFLOW__CORE__PARALLELISM: 32
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: 32
AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY: 16
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 1
```

1. **Additional pip packages**. They can be added to the `requirements.txt` file.


## Troubleshooting

Note that when you a a new workflow under `dags/`, Airflow will not pick that up immediately. By default the filesystem is only scanned for new DAGs every 300s. However, once the DAG is added, updates are applied nearly instantaneously.

Also, if you edit the DAG while an instance of it is running, it might cause problems with the run, as Airflow will try to update the tasks and their properties in DAG according to the file changes.
We will be running a local Airflow setup using Docker Compose. First, make sure it is installed (this and subsequent commands are tested on Ubuntu).
