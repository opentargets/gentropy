"""Airflow boilerplate code which can be shared by several DAGs."""

from __future__ import annotations

from typing import Any

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


# Shared DAG construction parameters.
shared_dag_args = dict(
    owner="Open Targets Data Team",
    retries=3,
)
shared_dag_kwargs = dict(
    tags=["genetics_etl", "experimental"],
    start_date=pendulum.now(tz="Europe/London").subtract(days=1),
    schedule_interval="@once",
    catchup=False,
)


def create_cluster(
    cluster_name: str,
    master_machine_type: str = "n1-standard-4",
    worker_machine_type: str = "n1-standard-16",
    num_workers: int = 0,
) -> DataprocCreateClusterOperator:
    """Generate an Airflow task to create a Dataproc cluster. Common parameters are reused, and varying parameters can be specified as needed.

    Args:
        cluster_name (str): Name of the cluster.
        master_machine_type (str): Machine type for the master node. Defaults to "n1-standard-4".
        worker_machine_type (str): Machine type for the worker nodes. Defaults to "n1-standard-16".
        num_workers (int): Number of worker nodes. Defaults to 0.

    Returns:
        DataprocCreateClusterOperator: Airflow task to create a Dataproc cluster.
    """
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
        idle_delete_ttl=None,
    ).make()
    return DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=project_id,
        cluster_config=cluster_generator_config,
        region=region,
        cluster_name=cluster_name,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )


def submit_job(
    cluster_name: str, task_id: str, job_type: str, job_specification: dict[str, Any]
) -> DataprocSubmitJobOperator:
    """Submit an arbitrary job to a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.
        task_id (str): Name of the task.
        job_type (str): Type of the job to submit.
        job_specification (dict[str, Any]): Specification of the job to submit.

    Returns:
        DataprocSubmitJobOperator: Airflow task to submit an arbitrary job to a Dataproc cluster.
    """
    return DataprocSubmitJobOperator(
        task_id=task_id,
        region=region,
        project_id=project_id,
        job={
            "job_uuid": f"airflow-{task_id}",
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            job_type: job_specification,
        },
    )


def submit_pyspark_job(
    cluster_name: str, task_id: str, python_module_path: str, args: dict[str, Any]
) -> DataprocSubmitJobOperator:
    """Submit a PySpark job to a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.
        task_id (str): Name of the task.
        python_module_path (str): Path to the Python module to run.
        args (dict[str, Any]): Arguments to pass to the Python module.

    Returns:
        DataprocSubmitJobOperator: Airflow task to submit a PySpark job to a Dataproc cluster.
    """
    formatted_args = []
    if isinstance(args, dict):
        formatted_args = [f"--{arg}={val}" for arg, val in args.items()]
    return submit_job(
        cluster_name=cluster_name,
        task_id=task_id,
        job_type="pyspark_job",
        job_specification={
            "main_python_file_uri": f"{initialisation_base_path}/{python_module_path}",
            "args": formatted_args,
            "properties": {
                "spark.jars": "/opt/conda/miniconda3/lib/python3.10/site-packages/hail/backend/hail-all-spark.jar",
                "spark.driver.extraClassPath": "/opt/conda/miniconda3/lib/python3.10/site-packages/hail/backend/hail-all-spark.jar",
                "spark.executor.extraClassPath": "./hail-all-spark.jar",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.kryo.registrator": "is.hail.kryo.HailKryoRegistrator",
            },
        },
    )


def install_dependencies(cluster_name: str) -> DataprocSubmitJobOperator:
    """Install dependencies on a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.

    Returns:
        DataprocSubmitJobOperator: Airflow task to install dependencies on a Dataproc cluster.
    """
    return submit_job(
        cluster_name=cluster_name,
        task_id="install_dependencies",
        job_type="pig_job",
        job_specification={
            "jar_file_uris": [
                f"gs://genetics_etl_python_playground/initialisation/{otg_version}/install_dependencies_on_cluster.sh"
            ],
            "query_list": {
                "queries": [
                    "sh chmod 750 ${PWD}/install_dependencies_on_cluster.sh",
                    "sh ${PWD}/install_dependencies_on_cluster.sh",
                ]
            },
        },
    )


def delete_cluster(cluster_name: str) -> DataprocDeleteClusterOperator:
    """Generate an Airflow task to delete a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.

    Returns:
        DataprocDeleteClusterOperator: Airflow task to delete a Dataproc cluster.
    """
    return DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=project_id,
        cluster_name=cluster_name,
        region=region,
        trigger_rule=TriggerRule.ALL_DONE,
        deferrable=True,
    )
