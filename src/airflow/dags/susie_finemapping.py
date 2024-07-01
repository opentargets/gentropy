"""Airflow DAG that uses Google Cloud Batch to run the SuSie Finemapper step."""

from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any

import common_airflow as common
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from cloudpathlib import GSPath
from google.cloud import batch_v1

PROJECT_ID = "open-targets-genetics-dev"
REGION = "europe-west1"
GENTROPY_DOCKER_IMAGE = (
    "europe-west1-docker.pkg.dev/open-targets-genetics-dev/gentropy-app/gentropy:dev"
)
STUDY_LOCUS_PATH = (
    "gs://genetics-portal-dev-analysis/irene/toy_studdy_locus_alzheimer_partitioned"
)
STUDY_INDEX_PATH = "gs://gwas_catalog_data/study_index"
OUTPUT_PATH = "gs://genetics-portal-dev-analysis/irene/tmp_to_delete"


@task(task_id="get_study_loci_to_finemap")
def get_study_loci_to_finemap(**kwargs: Any) -> None:
    """Get the list of study loci to include in the fine-mapping job.

    Args:
        **kwargs (Any): Keyword arguments.
    """
    ti = kwargs["ti"]
    all_study_locus_ids = [
        re.search(r"studyLocusId=(-?\d+)", path)[1]  # type: ignore
        for path in ti.xcom_pull(task_ids="get_all_study_locus_ids", key="return_value")
        if re.search(r"studyLocusId=(-?\d+)", path)
    ]
    finemapped_study_locus_ids = [
        path.strip("/_SUCCESS").split("/")[-1]
        for path in ti.xcom_pull(task_ids="get_finemapped_paths", key="return_value")
    ]

    ids_to_finemap = list(set(all_study_locus_ids) - set(finemapped_study_locus_ids))
    ti.xcom_push(key="study_loci_ids", value=ids_to_finemap)


@task(task_id="finemapping_task")
def finemapping_task(**kwargs: Any) -> None:
    """Submit a Batch job to run fine-mapping on a list of study loci.

    Args:
        **kwargs (Any): Keyword arguments.
    """
    ti = kwargs["ti"]
    pulled_study_loci_ids = ti.xcom_pull(
        task_ids="get_study_loci_to_finemap", key="study_loci_ids"
    )
    batch_task = CloudBatchSubmitJobOperator(
        task_id="finemapping_batch_job",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=f"finemapping-job-{time.strftime('%Y%m%d-%H%M%S')}",
        job=_finemapping_batch_job(
            pulled_study_loci_ids,
            GENTROPY_DOCKER_IMAGE,
            STUDY_LOCUS_PATH,
            STUDY_INDEX_PATH,
            OUTPUT_PATH,
            1000000,
            10,
        ),
        deferrable=False,
    )
    batch_task.execute(context=kwargs)


def _finemapping_batch_job(
    study_loci_ids: list[str],
    docker_image_url: str,
    study_locus_collected_path: str,
    study_index_path: str,
    output_path: str,
    locus_radius: int,
    max_causal_snps: int,
) -> batch_v1.Job:
    """Create a Batch job to run fine-mapping on a list of study loci.

    Args:
        study_loci_ids (list[str]): The list of study loci to fine-map.
        docker_image_url (str): The URL of the Docker image to use for the job.
        study_locus_collected_path (str): The path to the study locus to fine-map.
        study_index_path (str): The path to the study index.
        output_path (str): The path to store the output.
        locus_radius (int): The radius around the study locus to consider for fine-mapping.
        max_causal_snps (int): The maximum number of causal SNPs to consider.

    Returns:
        batch_v1.Job: A Batch job to run fine-mapping on the given study loci.
    """
    runnable = batch_v1.Runnable()
    runnable.container = batch_v1.Runnable.Container()
    runnable.container.image_uri = docker_image_url
    runnable.container.entrypoint = "/bin/sh"
    runnable.container.commands = [
        "-c",
        rf"poetry run gentropy step=susie_finemapping step.study_locus_to_finemap=$STUDYLOCUSID step.study_locus_collected_path={study_locus_collected_path} step.study_index_path={study_index_path} step.output_path={output_path} step.locus_radius={locus_radius} step.max_causal_snps={max_causal_snps} step.session.extended_spark_conf={{spark.jars:https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar\}}",
    ]
    runnable.container.options = "-e HYDRA_FULL_ERROR=1"

    task = batch_v1.TaskSpec()
    task.runnables = [runnable]

    resources = batch_v1.ComputeResource()
    resources.cpu_milli = 2000
    resources.memory_mib = 16
    task.compute_resource = resources
    task.max_retry_count = 0

    group = batch_v1.TaskGroup()

    task_env = [
        batch_v1.Environment(variables={"STUDYLOCUSID": study_locus_id})
        for study_locus_id in study_loci_ids
    ]
    group.task_environments = task_env
    group.task_spec = task
    policy = batch_v1.AllocationPolicy.InstancePolicy()
    policy.machine_type = "e2-standard-4"
    instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
    instances.policy = policy
    allocation_policy = batch_v1.AllocationPolicy()
    allocation_policy.instances = [instances]

    job = batch_v1.Job()
    job.task_groups = [group]
    job.allocation_policy = allocation_policy
    job.labels = {"env": "testing", "type": "container"}

    job.logs_policy = batch_v1.LogsPolicy()
    job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

    return job


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — finemap study loci with SuSie",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
) as dag:
    get_all_study_locus_ids = GCSListObjectsOperator(
        task_id="get_all_study_locus_ids",
        bucket=GSPath(STUDY_LOCUS_PATH).bucket,
        prefix=GSPath(STUDY_LOCUS_PATH)._url.path[1:],  # remove first slash from path
        match_glob="**/**",
    )
    get_finemapped_paths = GCSListObjectsOperator(
        task_id="get_finemapped_paths",
        bucket=GSPath(OUTPUT_PATH).bucket,
        prefix=GSPath(OUTPUT_PATH)._url.path[1:],  # remove first slash from path
        match_glob="**/_SUCCESS",
    )

    (
        [get_all_study_locus_ids, get_finemapped_paths]
        >> get_study_loci_to_finemap()
        >> finemapping_task()
    )
