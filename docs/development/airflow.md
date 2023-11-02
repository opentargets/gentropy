# Airflow workflow

This section describes how to set up a local Airflow server which will orchestrate running workflows in Google Cloud Platform. This is useful for testing and debugging, but for production use, it is recommended to run Airflow on a dedicated server.

## Install pre-requisites

- [Docker](https://docs.docker.com/get-docker/)
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)

!!!warning macOS Docker memory allocation
    On macOS, the default amount of memory available for Docker might not be enough to get Airflow up and running. Allocate at least 4GB of memory for the Docker Engine (ideally 8GB). [More info](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#)


## Configure Airflow access to Google Cloud Platform

!!!warning Specifying Google Cloud parameters
    Run the next two command with the appropriate project ID and service account name to ensure the correct Google default application credentials are set up.

Authenticate to Google Cloud:

```bash
gcloud auth application-default login --project=open-targets-eu-dev
```

Create the service account key file that will be used by Airflow to access Google Cloud Platform resources:

```bash
gcloud iam service-accounts keys create ~/.config/gcloud/service_account_credentials.json --iam-account=open-targets-genetics-dev@appspot.gserviceaccount.com
```


## Set up Airflow

Change the working directory so that all subsequent commands will work:

```bash
cd src/airflow
```

### Build Docker image

!!!note Custom Docker image for Airflow
    The custom Dockerfile built by the command below extends the official [Airflow Docker Compose YAML](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml). We add support for Google Cloud SDK, Google Dataproc operators, and access to GCP credentials.

```bash
docker build . --tag extending_airflow:latest
```

### Initialise

Before starting Airflow, initialise the database:

```bash
docker compose up airflow-init
```

Now start all services:

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
