"""Airflow boilerplate code which can be shared by several DAGs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

import pendulum
import yaml
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import batch_v1, dataproc_v1, storage

if TYPE_CHECKING:
    from pathlib import Path

# Code version. It has to be repeated here as well as in `pyproject.toml`, because Airflow isn't able to look at files outside of its `dags/` directory.
GENTROPY_VERSION = "0.0.0"

# Cloud configuration.
GCP_PROJECT = "open-targets-genetics-dev"
GCP_REGION = "europe-west1"
GCP_ZONE = "europe-west1-d"
GCP_DATAPROC_IMAGE = "2.1"
GCP_AUTOSCALING_POLICY = "otg-etl"

# Cluster init configuration.
INITIALISATION_BASE_PATH = (
    f"gs://genetics_etl_python_playground/initialisation/{GENTROPY_VERSION}"
)
CONFIG_TAG = f"{INITIALISATION_BASE_PATH}/config.tar.gz"
PACKAGE_WHEEL = (
    f"{INITIALISATION_BASE_PATH}/gentropy-{GENTROPY_VERSION}-py3-none-any.whl"
)
INITIALISATION_EXECUTABLE_FILE = [
    f"{INITIALISATION_BASE_PATH}/install_dependencies_on_cluster.sh"
]

# CLI configuration.
CLUSTER_CONFIG_DIR = "/config"
CONFIG_NAME = "ot_config"
PYTHON_CLI = "cli.py"

# Shared DAG construction parameters.
shared_dag_args = {
    "owner": "Open Targets Data Team",
    "retries": 0,
}

shared_dag_kwargs = {
    "tags": ["genetics_etl", "experimental"],
    "start_date": pendulum.now(tz="Europe/London").subtract(days=1),
    "schedule": "@once",
    "catchup": False,
}

MACHINES = {
    "VEPMACHINE": {
        "machine_type": "e2-standard-4",
        "cpu_milli": 2000,
        "memory_mib": 2000,
        "boot_disk_mib": 10000,
    },
}


def check_gcp_folder_exists(bucket_name: str, folder_path: str) -> bool:
    """Check if a folder exists in a Google Cloud bucket.

    Args:
        bucket_name (str): The name of the Google Cloud bucket.
        folder_path (str): The path of the folder to check.

    Returns:
        bool: True if the folder exists, False otherwise.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    return any(blobs)


def create_cluster(
    cluster_name: str,
    master_machine_type: str = "n1-highmem-16",
    worker_machine_type: str = "n1-standard-16",
    num_workers: int = 2,
    num_preemptible_workers: int = 0,
    num_local_ssds: int = 1,
    autoscaling_policy: str = GCP_AUTOSCALING_POLICY,
    master_disk_size: int = 500,
) -> DataprocCreateClusterOperator:
    """Generate an Airflow task to create a Dataproc cluster. Common parameters are reused, and varying parameters can be specified as needed.

    Args:
        cluster_name (str): Name of the cluster.
        master_machine_type (str): Machine type for the master node. Defaults to "n1-highmem-8".
        worker_machine_type (str): Machine type for the worker nodes. Defaults to "n1-standard-16".
        num_workers (int): Number of worker nodes. Defaults to 2.
        num_preemptible_workers (int): Number of preemptible worker nodes. Defaults to 0.
        num_local_ssds (int): How many local SSDs to attach to each worker node, both primary and secondary. Defaults to 1.
        autoscaling_policy (str): Name of the autoscaling policy to use. Defaults to GCP_AUTOSCALING_POLICY.
        master_disk_size (int): Size of the master node's boot disk in GB. Defaults to 500.

    Returns:
        DataprocCreateClusterOperator: Airflow task to create a Dataproc cluster.
    """
    # Create base cluster configuration.
    cluster_config = ClusterGenerator(
        project_id=GCP_PROJECT,
        zone=GCP_ZONE,
        master_machine_type=master_machine_type,
        worker_machine_type=worker_machine_type,
        master_disk_size=master_disk_size,
        worker_disk_size=500,
        num_preemptible_workers=num_preemptible_workers,
        num_workers=num_workers,
        image_version=GCP_DATAPROC_IMAGE,
        enable_component_gateway=True,
        optional_components=["JUPYTER"],
        init_actions_uris=INITIALISATION_EXECUTABLE_FILE,
        metadata={
            "CONFIGTAR": CONFIG_TAG,
            "PACKAGE": PACKAGE_WHEEL,
        },
        idle_delete_ttl=30 * 60,  # In seconds.
        autoscaling_policy=f"projects/{GCP_PROJECT}/regions/{GCP_REGION}/autoscalingPolicies/{autoscaling_policy}",
    ).make()

    # If specified, amend the configuration to include local SSDs for worker nodes.
    if num_local_ssds:
        for worker_section in ("worker_config", "secondary_worker_config"):
            # Create a disk config section if it does not exist.
            cluster_config[worker_section].setdefault("disk_config", {})
            # Specify the number of local SSDs.
            cluster_config[worker_section]["disk_config"]["num_local_ssds"] = (
                num_local_ssds
            )

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
    cluster_name: str,
    task_id: str,
    job_type: str,
    job_specification: dict[str, Any],
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
) -> DataprocSubmitJobOperator:
    """Submit an arbitrary job to a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.
        task_id (str): Name of the task.
        job_type (str): Type of the job to submit.
        job_specification (dict[str, Any]): Specification of the job to submit.
        trigger_rule (TriggerRule): Trigger rule for the task. Defaults to TriggerRule.ALL_SUCCESS.

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
        trigger_rule=trigger_rule,
    )


def submit_pyspark_job(
    cluster_name: str,
    task_id: str,
    python_module_path: str,
    args: list[str],
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
) -> DataprocSubmitJobOperator:
    """Submit a PySpark job to a Dataproc cluster.

    Args:
        cluster_name (str): Name of the cluster.
        task_id (str): Name of the task.
        python_module_path (str): Path to the Python module to run.
        args (list[str]): Arguments to pass to the Python module.
        trigger_rule (TriggerRule): Trigger rule for the task. Defaults to TriggerRule.ALL_SUCCESS.

    Returns:
        DataprocSubmitJobOperator: Airflow task to submit a PySpark job to a Dataproc cluster.
    """
    return submit_job(
        cluster_name=cluster_name,
        task_id=task_id,
        job_type="pyspark_job",
        trigger_rule=trigger_rule,
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


def submit_step(
    cluster_name: str,
    step_id: str,
    task_id: str = "",
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
    other_args: Optional[list[str]] = None,
) -> DataprocSubmitJobOperator:
    """Submit a PySpark job to execute a specific CLI step.

    Args:
        cluster_name (str): Name of the cluster.
        step_id (str): Name of the step in gentropy.
        task_id (str): Name of the task. Defaults to step_id.
        trigger_rule (TriggerRule): Trigger rule for the task. Defaults to TriggerRule.ALL_SUCCESS.
        other_args (Optional[list[str]]): Other arguments to pass to the CLI step. Defaults to None.

    Returns:
        DataprocSubmitJobOperator: Airflow task to submit a PySpark job to execute a specific CLI step.
    """
    if task_id == "":
        task_id = step_id
    return submit_pyspark_job(
        cluster_name=cluster_name,
        task_id=task_id,
        python_module_path=f"{INITIALISATION_BASE_PATH}/{PYTHON_CLI}",
        trigger_rule=trigger_rule,
        args=[f"step={step_id}"]
        + (other_args if other_args is not None else [])
        + [
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
                f"gs://genetics_etl_python_playground/initialisation/{GENTROPY_VERSION}/install_dependencies_on_cluster.sh"
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
    )


def read_yaml_config(config_path: Path) -> Any:
    """Parse a YAMl config file and do all necessary checks.

    Args:
        config_path (Path): Path to the YAML config file.

    Returns:
        Any: Parsed YAML config file.
    """
    assert config_path.exists(), f"YAML config path {config_path} does not exist."
    with open(config_path) as config_file:
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


def submit_pyspark_job_no_operator(
    cluster_name: str,
    step_id: str,
    other_args: Optional[list[str]] = None,
) -> None:
    """Submits the Pyspark job to the cluster.

    Args:
        cluster_name (str): Cluster name
        step_id (str): Step id
        other_args (Optional[list[str]]): Other arguments to pass to the CLI step. Defaults to None.
    """
    # Create the job client.
    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{GCP_REGION}-dataproc.googleapis.com:443"}
    )

    python_uri = f"{INITIALISATION_BASE_PATH}/{PYTHON_CLI}"
    # Create the job config. 'main_jar_file_uri' can also be a
    # Google Cloud Storage URL.
    job_description = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": python_uri,
            "args": [f"step={step_id}"]
            + (other_args if other_args is not None else [])
            + [
                f"--config-dir={CLUSTER_CONFIG_DIR}",
                f"--config-name={CONFIG_NAME}",
            ],
            "properties": {
                "spark.jars": "/opt/conda/miniconda3/lib/python3.10/site-packages/hail/backend/hail-all-spark.jar",
                "spark.driver.extraClassPath": "/opt/conda/miniconda3/lib/python3.10/site-packages/hail/backend/hail-all-spark.jar",
                "spark.executor.extraClassPath": "./hail-all-spark.jar",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.kryo.registrator": "is.hail.kryo.HailKryoRegistrator",
            },
        },
    }
    job_client.submit_job(
        project_id=GCP_PROJECT, region=GCP_REGION, job=job_description
    )


def create_container_runnable(
    image: str, commands: list[str], **kwargs: Any
) -> batch_v1.Runnable:
    """Create a container runnable for a Batch job with additional optional parameters.

    Args:
        image (str): The Docker image to use.
        commands (list[str]): The commands to run in the container.
        **kwargs (Any): Additional optional parameters to set on the container.

    Returns:
        batch_v1.Runnable: The container runnable.
    """
    container = batch_v1.Runnable.Container(
        image_uri=image, entrypoint="/bin/sh", commands=commands, **kwargs
    )
    return batch_v1.Runnable(container=container)


def create_task_spec(
    image: str, commands: list[str], **kwargs: Any
) -> batch_v1.TaskSpec:
    """Create a task for a Batch job.

    Args:
        image (str): The Docker image to use.
        commands (list[str]): The commands to run in the container.
        **kwargs (Any): Any additional parameter to pass to the container runnable

    Returns:
        batch_v1.TaskSpec: The task specification.
    """
    task = batch_v1.TaskSpec()
    task.runnables = [create_container_runnable(image, commands, **kwargs)]
    return task


def set_up_mounting_points(
    mounting_points: list[dict[str, str]],
) -> list[batch_v1.Volume]:
    """Set up the mounting points for the container.

    Args:
        mounting_points (list[dict[str, str]]): The mounting points.

    Returns:
        list[batch_v1.Volume]: The volumes.
    """
    volumes = []
    for mount in mounting_points:
        gcs_bucket = batch_v1.GCS()
        gcs_bucket.remote_path = mount["remote_path"]
        gcs_volume = batch_v1.Volume()
        gcs_volume.gcs = gcs_bucket
        gcs_volume.mount_path = mount["mount_point"]
        volumes.append(gcs_volume)
    return volumes


def create_batch_job(
    task: batch_v1.TaskSpec,
    machine: str,
    task_env: list[batch_v1.Environment],
    mounting_points: list[dict[str, str]] | None = None,
) -> batch_v1.Job:
    """Create a Google Batch job.

    Args:
        task (batch_v1.TaskSpec): The task specification.
        machine (str): The machine type to use.
        task_env (list[batch_v1.Environment]): The environment variables for the task.
        mounting_points (list[dict[str, str]] | None): List of mounting points.

    Returns:
        batch_v1.Job: The Batch job.
    """
    resources = batch_v1.ComputeResource()
    resources.cpu_milli = MACHINES[machine]["cpu_milli"]
    resources.memory_mib = MACHINES[machine]["memory_mib"]
    resources.boot_disk_mib = MACHINES[machine]["boot_disk_mib"]
    task.compute_resource = resources

    task.max_retry_count = 3
    task.max_run_duration = "43200s"

    # The mounting points are set up and assigned to the task:
    task.volumes = set_up_mounting_points(mounting_points) if mounting_points else None

    group = batch_v1.TaskGroup()
    group.task_spec = task
    group.task_environments = task_env

    policy = batch_v1.AllocationPolicy.InstancePolicy()
    policy.machine_type = MACHINES[machine]["machine_type"]
    policy.provisioning_model = "SPOT"

    instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
    instances.policy = policy
    allocation_policy = batch_v1.AllocationPolicy()
    allocation_policy.instances = [instances]

    job = batch_v1.Job()
    job.task_groups = [group]
    job.allocation_policy = allocation_policy
    job.logs_policy = batch_v1.LogsPolicy()
    job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

    return job
