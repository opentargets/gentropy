"""Airflow boilerplate code which can be shared by several DAGs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pendulum
import yaml
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

if TYPE_CHECKING:
    from pathlib import Path

# Code version. It has to be repeated here as well as in `pyproject.toml`, because Airflow isn't able to look at files outside of its `dags/` directory.
OTG_VERSION = "1.0.0"


# Cloud configuration.
GCP_PROJECT = "open-targets-genetics-dev"
GCP_REGION = "europe-west1"
GCP_ZONE = "europe-west1-d"
GCP_DATAPROC_IMAGE = "2.1"
GCP_AUTOSCALING_POLICY = "otg-etl"


# Cluster init configuration.
INITIALISATION_BASE_PATH = (
    f"gs://genetics_etl_python_playground/initialisation/{OTG_VERSION}"
)
CONFIG_TAG = f"{INITIALISATION_BASE_PATH}/config.tar.gz"
PACKAGE_WHEEL = f"{INITIALISATION_BASE_PATH}/otgenetics-{OTG_VERSION}-py3-none-any.whl"
INITIALISATION_EXECUTABLE_FILE = [
    f"{INITIALISATION_BASE_PATH}/install_dependencies_on_cluster.sh"
]


# CLI configuration.
CLUSTER_CONFIG_DIR = "/config"
CONFIG_NAME = "config"
PYTHON_CLI = "cli.py"


# Shared DAG construction parameters.
shared_dag_args = dict(
    owner="Open Targets Data Team",
    retries=1,
)
shared_dag_kwargs = dict(
    tags=["genetics_etl", "experimental"],
    start_date=pendulum.now(tz="Europe/London").subtract(days=1),
    schedule_interval="@once",
    catchup=False,
)


def create_cluster(
    cluster_name: str,
    master_machine_type: str = "n1-highmem-8",
    worker_machine_type: str = "n1-standard-16",
    num_workers: int = 2,
    num_local_ssds: int = 1,
) -> DataprocCreateClusterOperator:
    """Generate an Airflow task to create a Dataproc cluster. Common parameters are reused, and varying parameters can be specified as needed.

    Args:
        cluster_name (str): Name of the cluster.
        master_machine_type (str): Machine type for the master node. Defaults to "n1-highmem-8".
        worker_machine_type (str): Machine type for the worker nodes. Defaults to "n1-standard-16".
        num_workers (int): Number of worker nodes. Defaults to 2.
        num_local_ssds (int): How many local SSDs to attach to each worker node, both primary and secondary. Defaults to 1.

    Returns:
        DataprocCreateClusterOperator: Airflow task to create a Dataproc cluster.
    """
    # Create base cluster configuration.
    cluster_config = ClusterGenerator(
        project_id=GCP_PROJECT,
        zone=GCP_ZONE,
        master_machine_type=master_machine_type,
        worker_machine_type=worker_machine_type,
        master_disk_size=500,
        worker_disk_size=500,
        num_workers=num_workers,
        image_version=GCP_DATAPROC_IMAGE,
        enable_component_gateway=True,
        init_actions_uris=INITIALISATION_EXECUTABLE_FILE,
        metadata={
            "CONFIGTAR": CONFIG_TAG,
            "PACKAGE": PACKAGE_WHEEL,
        },
        idle_delete_ttl=None,
        autoscaling_policy=f"projects/{GCP_PROJECT}/regions/{GCP_REGION}/autoscalingPolicies/{GCP_AUTOSCALING_POLICY}",
    ).make()

    # If specified, amend the configuration to include local SSDs for worker nodes.
    if num_local_ssds:
        for worker_section in ("worker_config", "secondary_worker_config"):
            # Create a disk config section if it does not exist.
            cluster_config[worker_section].setdefault("disk_config", dict())
            # Specify the number of local SSDs.
            cluster_config[worker_section]["num_local_ssds"] = num_local_ssds

    # Return the cluster creation operator.
    return DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=GCP_PROJECT,
        cluster_config=cluster_config,
        region=GCP_REGION,
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
        region=GCP_REGION,
        project_id=GCP_PROJECT,
        job={
            "job_uuid": f"airflow-{task_id}",
            "reference": {"project_id": GCP_PROJECT},
            "placement": {"cluster_name": cluster_name},
            job_type: job_specification,
        },
    )


def submit_pyspark_job(
    cluster_name: str, task_id: str, python_module_path: str, args: list[str]
) -> DataprocSubmitJobOperator:
    """Submit a PySpark job to a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.
        task_id (str): Name of the task.
        python_module_path (str): Path to the Python module to run.
        args (list[str]): Arguments to pass to the Python module.

    Returns:
        DataprocSubmitJobOperator: Airflow task to submit a PySpark job to a Dataproc cluster.
    """
    return submit_job(
        cluster_name=cluster_name,
        task_id=task_id,
        job_type="pyspark_job",
        job_specification={
            "main_python_file_uri": python_module_path,
            "args": args,
            "properties": {
                "spark.jars": "/opt/conda/miniconda3/lib/python3.10/site-packages/hail/backend/hail-all-spark.jar",
                "spark.driver.extraClassPath": "/opt/conda/miniconda3/lib/python3.10/site-packages/hail/backend/hail-all-spark.jar",
                "spark.executor.extraClassPath": "./hail-all-spark.jar",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.kryo.registrator": "is.hail.kryo.HailKryoRegistrator",
            },
        },
    )


def submit_step(cluster_name: str, step_id: str) -> DataprocSubmitJobOperator:
    """Submit a PySpark job to execute a specific CLI step.

    Args:
        cluster_name (str): Name of the cluster.
        step_id (str): Name of the step.

    Returns:
        DataprocSubmitJobOperator: Airflow task to submit a PySpark job to execute a specific CLI step.
    """
    return submit_pyspark_job(
        cluster_name=cluster_name,
        task_id=step_id,
        python_module_path=f"{INITIALISATION_BASE_PATH}/{PYTHON_CLI}",
        args=[
            f"step={step_id}",
            f"--config-dir={CLUSTER_CONFIG_DIR}",
            f"--config-name={CONFIG_NAME}",
        ],
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
                f"gs://genetics_etl_python_playground/initialisation/{OTG_VERSION}/install_dependencies_on_cluster.sh"
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
        project_id=GCP_PROJECT,
        cluster_name=cluster_name,
        region=GCP_REGION,
        trigger_rule=TriggerRule.ALL_DONE,
        deferrable=True,
    )


def read_yaml_config(config_path: Path) -> Any:
    """Parse a YAMl config file and do all necessary checks.

    Args:
        config_path (Path): Path to the YAML config file.

    Returns:
        Any: Parsed YAML config file.
    """
    assert config_path.exists(), f"YAML config path {config_path} does not exist."
    with open(config_path, "r") as config_file:
        return yaml.safe_load(config_file)


def generate_dag(cluster_name: str, tasks: list[DataprocSubmitJobOperator]) -> Any:
    """For a list of tasks, generate a complete DAG.

    Args:
        cluster_name (str): Name of the cluster.
        tasks (list[DataprocSubmitJobOperator]): List of tasks to execute.

    Returns:
        Any: Airflow DAG.
    """
    return (
        create_cluster(cluster_name)
        >> install_dependencies(cluster_name)
        >> tasks
        >> delete_cluster(cluster_name)
    )
