"""Airflow DAG that uses Google Cloud Batch to run the SuSie Finemapper step."""

from __future__ import annotations

import time
from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)
from google.cloud import batch_v1

PROJECT_ID = "open-targets-genetics-dev"
REGION = "europe-west1"
GENTROPY_DOCKER_IMAGE = "europe-west1-docker.pkg.dev/open-targets-genetics-dev/gentropy-app-dev/gentropy_test:latest"
STUDY_LOCUS_PATH = "gs://genetics-portal-dev-analysis/yt4/toy_studdy_locus_alzheimer"
STUDY_INDEX_PATH = "gs://gwas_catalog_data/study_index"
OUTPUT_PATH = "gs://genetics-portal-dev-analysis/irene/tmp_to_delete"
CREDENTIALS_REMOTE_LOCATION = (
    "gs://genetics-portal-dev-analysis/irene/service_account_credentials.json"
)


def _finemapping_batch_job(
    studyloci_ids: list[str],
    docker_image_url: str,
    study_locus_collected_path: str,
    study_index_path: str,
    output_path: str,
    locus_radius: int,
    max_causal_snps: int,
) -> batch_v1.Job:
    """Create a Batch job to run fine-mapping on a list of study loci.

    Args:
        studyloci_ids (list[str]): A list of study loci IDs to run fine-mapping on.
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
        for study_locus_id in studyloci_ids
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
    description="Open Targets Genetics — eQTL preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
) as dag:
    finemapping_job = CloudBatchSubmitJobOperator(
        task_id="finemapping_batch_job",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=f"finemapping-job-{time.strftime('%Y%m%d-%H%M%S')}",
        job=_finemapping_batch_job(
            ["6109438569946056978", "6388992474978589194"],
            GENTROPY_DOCKER_IMAGE,
            STUDY_LOCUS_PATH,
            STUDY_INDEX_PATH,
            OUTPUT_PATH,
            1000000,
            10,
        ),
        dag=dag,
        deferrable=False,
    )

    (finemapping_job)
