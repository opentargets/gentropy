# Running Airflow workflows

This documentation section is currently under development. Once finished, it will be moved into `docs/`.

## Set up Docker

We will be running a local Airflow setup using Docker Compose. First, make sure it is installed. On Ubuntu it can be done with:

```bash
sudo apt install docker-compose
```

Next, verify that you can run Docker. This should say "Hello from Docker":

```bash
docker run hello-world
```

If the command above raises a permission error, do these steps:

```bash
sudo usermod -a -G docker $USER
newgrp docker
```

## Set up Airflow

Based on instructions from https://airflow.apache.org/docs/apache-airflow/stable/tutorial/pipeline.html. Make sure to run all commands from the `airflow` directory, where this README file is located.

```bash
# Download the docker-compose.yaml file.
curl -sLfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

# Make expected directories.
mkdir -p ./dags ./logs ./plugins


# Construct the modified Docker image with additional PIP dependencies.
docker build . --tag opentargets-airflow:2.7.1

# Set environment variables.
echo -e "AIRFLOW_UID=$(id -u)" > .env
echo "AIRFLOW_IMAGE_NAME=opentargets-airflow:2.7.1." >> .env
echo "GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/application_default_credentials.json" >> .env
```

## Temporary
Here are the variables to try to increase concurrency:
```
    AIRFLOW__CELERY__WORKER_CONCURRENCY: 256
    AIRFLOW__CORE__PARALLELISM: 256
    AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: 256
    AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 256
    AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY: 256
```

## Start Airflow

```bash
docker-compose up
```

Airflow UI will now be available at http://localhost:8080/home. Default username and password are both `airflow`.

## Configure Google Cloud access

In order to be able to access Google Cloud and do work with Dataproc, Airflow will need to be configured. First, obtain Google default application credentials by running this command and following the instructions:

```bash
gcloud auth application-default login
```

Next, copy the file into the `config/` subdirectory:

```bash
cp ~/.config/gcloud/application_default_credentials.json config/
```

Now open the Airflow UI and:

* Navigate to Admin → Connections.
* Click on "Add new record".
* Set "Connection type" to `Google Cloud``.
* Set "Connection ID" to `google_cloud_default`.
* Set "Credential Configuration File" to `/opt/airflow/config/application_default_credentials.json`.
* Click on "Save".

## Run a workflow

Workflows, which must be placed under the `dags/` directory, will appear in the "DAGs" section of the UI, which is also the main page. They can be triggered manually by opening a workflow and clicking on the "Play" button in the upper right corner.

In order to restart a failed task, click on it and then click on "Clear task".
