"""Airflow DAG for the harmonisation part of the pipeline."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, List

import common_airflow as common
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)
from google.cloud import batch_v1

PROJECT_ID = "open-targets-genetics-dev"
REGION = "europe-west1"

VEP_DOCKER_IMAGE = "europe-west1-docker.pkg.dev/open-targets-genetics-dev/gentropy-app/custom_ensembl_vep:dev"
MOUNT_DIR = "/mnt/disks/share"  # inside the image
BUCKET_NAME = "genetics_etl_python_playground/vep/"

MACHINES = {
    "VEPMACHINE": {
        "machine_type": "e2-standard-4",
        "cpu_milli": 2000,
        "memory_mib": 8000,
        "boot_disk_mib": 10000,
    },
}

INPUT_FILE = "clinvar_subset.vcf"
OUTPUT_FILE = "clinvar_output.json"


def create_container_runnable(image: str, commands: List[str]) -> batch_v1.Runnable:
    """Create a container runnable for a Batch job.

    Args:
        image (str): The Docker image to use.
        commands (List[str]): The commands to run in the container.

    Returns:
        batch_v1.Runnable: The container runnable.
    """
    runnable = batch_v1.Runnable()
    runnable.container = batch_v1.Runnable.Container()
    runnable.container.image_uri = image
    runnable.container.entrypoint = "/bin/sh"
    runnable.container.commands = commands
    return runnable


def create_task_spec(image: str, commands: List[str]) -> batch_v1.TaskSpec:
    """Create a task for a Batch job.

    Args:
        image (str): The Docker image to use.
        commands (List[str]): The commands to run in the container.

    Returns:
        batch_v1.TaskSpec: The task specification.
    """
    task = batch_v1.TaskSpec()
    task.runnables = [
        create_container_runnable(image, commands)
        # msg_runnable()
    ]
    return task


def create_batch_job(
    task: batch_v1.TaskSpec, task_count: int, machine: str
) -> batch_v1.Job:
    """Create a Google Batch job.

    Args:
        task (batch_v1.TaskSpec): The task specification.
        task_count (int): The number of tasks to run.
        machine (str): The machine type to use.

    Returns:
        batch_v1.Job: The Batch job.
    """
    resources = batch_v1.ComputeResource()
    resources.cpu_milli = MACHINES[machine]["cpu_milli"]
    resources.memory_mib = MACHINES[machine]["memory_mib"]
    resources.boot_disk_mib = MACHINES[machine]["boot_disk_mib"]
    task.compute_resource = resources

    task.max_retry_count = 0
    task.max_run_duration = "43200s"

    gcs_bucket = batch_v1.GCS()
    gcs_bucket.remote_path = BUCKET_NAME
    gcs_volume = batch_v1.Volume()
    gcs_volume.gcs = gcs_bucket
    gcs_volume.mount_path = MOUNT_DIR

    task.volumes = [gcs_volume]

    group = batch_v1.TaskGroup()
    group.task_spec = task
    group.task_count_per_node = 1
    group.task_count = task_count
    group.parallelism = task_count

    policy = batch_v1.AllocationPolicy.InstancePolicy()
    policy.machine_type = MACHINES[machine]["machine_type"]

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


@task(task_id="vep_cache")
def vep_task(**kwargs: Any) -> None:
    """Submit a Batch job to download cache for VEP.

    Args:
        **kwargs (Any): Keyword arguments.
    """
    # ti = kwargs["ti"]
    # pulled_study_loci_ids = ti.xcom_pull(
    #     task_ids="get_study_loci_to_finemap", key="study_loci_ids"
    # )
    command = [
        "-c",
        rf"vep --cache --offline --format vcf --force_overwrite \
            --dir_cache {MOUNT_DIR} \
            --input_file {MOUNT_DIR}/{INPUT_FILE} \
            --output_file {MOUNT_DIR}/{OUTPUT_FILE} --json \
            --dir_plugins {MOUNT_DIR}/VEP_plugins \
            --sift b \
            --polyphen b \
            --uniprot \
            --check_existing \
            --exclude_null_alleles \
            --canonical \
            --plugin LoF,loftee_path:{MOUNT_DIR}/VEP_plugins,gerp_bigwig:{MOUNT_DIR}/gerp_conservation_scores.homo_sapiens.GRCh38.bw,human_ancestor_fa:{MOUNT_DIR}/human_ancestor.fa.gz,conservation_file:{MOUNT_DIR}/loftee.sql \
            --plugin AlphaMissense,file={MOUNT_DIR}/AlphaMissense_hg38.tsv.gz,transcript_match=1",
    ]
    task = create_task_spec(VEP_DOCKER_IMAGE, command)
    batch_task = CloudBatchSubmitJobOperator(
        task_id="vep_cache_batch_job",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=f"vep-cache-job-{time.strftime('%Y%m%d-%H%M%S')}",
        job=create_batch_job(task, 1, "VEPMACHINE"),
        deferrable=False,
    )
    batch_task.execute(context=kwargs)


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Ensembl VEP",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    vep_task()
