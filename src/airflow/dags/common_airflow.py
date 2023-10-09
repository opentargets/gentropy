"""Airflow boilerplate code which can be shared by several DAGs."""

from __future__ import annotations

import os

import pendulum
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# Code version. It has to be repeated here as well as in `pyproject.toml`, because Airflow isn't able to look at files outside of its `dags/` directory.
otg_version = "1.0.0"


# Cloud configuration.
project_id = "open-targets-genetics-dev"
region = "europe-west1"
zone = "europe-west1-d"
image_version = "2.1"
google_application_credentials = (
    "/opt/airflow/config/application_default_credentials.json"
)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_application_credentials
os.environ["GOOGLE_CLOUD_PROJECT"] = project_id


# Executable configuration.
initialisation_base_path = (
    f"gs://genetics_etl_python_playground/initialisation/{otg_version}"
)
python_cli = f"{initialisation_base_path}/cli.py"
config_tar = f"{initialisation_base_path}/config.tar.gz"
package_wheel = f"{initialisation_base_path}/otgenetics-{otg_version}-py3-none-any.whl"
initialisation_executable_file = [
    f"{initialisation_base_path}/install_dependencies_on_cluster.sh"
]


# Input/output file configuration.
version = "XX.XX"
inputs = "gs://genetics_etl_python_playground/input"
outputs = f"gs://genetics_etl_python_playground/output/python_etl/parquet/{version}"
spark_write_mode = "overwrite"


# Shared DAG construction parameters.
shared_dag_kwargs = dict(
    tags=["genetics_etl", "experimental"],
    start_date=pendulum.now(tz="Europe/London").subtract(days=1),
    schedule_interval="@once",
    catchup=False,
)


def create_cluster(
    cluster_name,
    master_machine_type="n1-standard-4",
    worker_machine_type="n1-standard-16",
    num_workers=0,
):
    """Generate an Airflow task to create a Dataproc cluster. Common parameters are reused, and varying parameters can be specified as needed."""
    cluster_generator_config = ClusterGenerator(
        project_id=project_id,
        zone=zone,
        master_machine_type=master_machine_type,
        worker_machine_type=worker_machine_type,
        master_disk_size=1000,
        worker_disk_size=500,
        num_workers=num_workers,
        num_local_ssds=1,
        image_version=image_version,
        enable_component_gateway=True,
        init_actions_uris=initialisation_executable_file,
        metadata={
            "CONFIGTAR": config_tar,
            "PACKAGE": package_wheel,
        },
        idle_delete_ttl=300,
    ).make()
    return DataprocCreateClusterOperator(
        task_id=f"create_cluster_{cluster_name}",
        project_id=project_id,
        cluster_config=cluster_generator_config,
        region=region,
        cluster_name=cluster_name,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )


def submit_pyspark_job(cluster_name, task_id, python_module_path, args):
    """Generate an Airflow task to run a PySpark job on a Dataproc cluster."""
    if isinstance(args, dict):
        args = [f"--{arg}={val}" for arg, val in args.items()]
    return DataprocSubmitJobOperator(
        task_id=task_id,
        region=region,
        project_id=project_id,
        job={
            "job_uuid": f"airflow-{task_id}",
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": {
                "main_python_file_uri": f"{initialisation_base_path}/{python_module_path}",
                "args": args,
                "properties": {
                    "spark.jars": "/opt/conda/miniconda3/lib/python3.10/site-packages/hail/backend/hail-all-spark.jar",
                    "spark.driver.extraClassPath": "/opt/conda/miniconda3/lib/python3.10/site-packages/hail/backend/hail-all-spark.jar",
                    "spark.executor.extraClassPath": "./hail-all-spark.jar",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                    "spark.kryo.registrator": "is.hail.kryo.HailKryoRegistrator",
                },
            },
        },
    )


def delete_cluster(cluster_name):
    """Generate an Airflow task to delete a Dataproc cluster."""
    return DataprocDeleteClusterOperator(
        task_id=f"delete_cluster_{cluster_name}",
        project_id=project_id,
        cluster_name=cluster_name,
        region=region,
        trigger_rule=TriggerRule.ALL_DONE,
        deferrable=True,
    )
