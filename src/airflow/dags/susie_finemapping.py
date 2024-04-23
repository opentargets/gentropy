"""Example Airflow DAG that uses Google Cloud Batch Operators."""

from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)
from google.cloud import batch_v1

PROJECT_ID = "open-targets-genetics-dev"
REGION = "europe-west1"


def _finemapping_batch_job(studyloci_ids: list[str]) -> batch_v1.Job:
    """Create a Batch job to run fine-mapping on a list of study loci.

    Args:
        studyloci_ids (list[str]): A list of study loci IDs to run fine-mapping on.

    Returns:
        batch_v1.Job: A Batch job to run fine-mapping on the given study loci.
    """
    runnable = batch_v1.Runnable()
    runnable.container = batch_v1.Runnable.Container()
    runnable.container.image_uri = "europe-west1-docker.pkg.dev/open-targets-genetics-dev/gentropy-app-dev/gentropy_test:0.1"
    runnable.container.entrypoint = "/bin/sh"
    runnable.container.commands = [
        "-c",
        "echo We are going to finemap this studyLocusId $STUDYLOCUSID",
    ]

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
    study_loci_ids = ["1"]

    finemapping_job = CloudBatchSubmitJobOperator(
        task_id="finemapping_batch_job",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="finemapping-job",
        job=_finemapping_batch_job(study_loci_ids),
        dag=dag,
        deferrable=False,
    )

    (finemapping_job)
