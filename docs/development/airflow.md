# Running Airflow workflows

Airflow code is located in `src/airflow`. Make sure to execute all of the instructions from that directory, unless stated otherwise.

## Set up Docker

We will be running a local Airflow setup using Docker Compose. First, make sure it is installed (this and subsequent commands are tested on Ubuntu):

```bash
sudo apt install docker-compose
```

Next, verify that you can run Docker. This should say "Hello from Docker":

```bash
docker run hello-world
```

If the command above raises a permission error, fix it and reboot:

```bash
sudo usermod -a -G docker $USER
newgrp docker
```

## Set up Airflow

This section is adapted from instructions from https://airflow.apache.org/docs/apache-airflow/stable/tutorial/pipeline.html. When you run the commands, make sure your current working directory is `src/airflow`.

```bash
# Download the latest docker-compose.yaml file.
curl -sLfO https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml

# Make expected directories.
mkdir -p ./config ./dags ./logs ./plugins

# Construct the modified Docker image with additional PIP dependencies.
docker build . --tag opentargets-airflow:2.7.1

# Set environment variables.
cat << EOF > .env
AIRFLOW_UID=$(id -u)
AIRFLOW_IMAGE_NAME=opentargets-airflow:2.7.1
EOF
```

Now modify `docker-compose.yaml` and add the following to the x-airflow-common → environment section:
```
GOOGLE_APPLICATION_CREDENTIALS: '/opt/airflow/config/application_default_credentials.json'
AIRFLOW__CELERY__WORKER_CONCURRENCY: 32
AIRFLOW__CORE__PARALLELISM: 32
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: 32
AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY: 16
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 1
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

Next, copy the file into the `config/` subdirectory which we created above:

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

## Troubleshooting

Note that when you a a new workflow under `dags/`, Airflow will not pick that up immediately. By default the filesystem is only scanned for new DAGs every 300s. However, once the DAG is added, updates are applied nearly instantaneously.

Also, if you edit the DAG while an instance of it is running, it might cause problems with the run, as Airflow will try to update the tasks and their properties in DAG according to the file changes.
