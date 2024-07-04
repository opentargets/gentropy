"""DAG that generates a variant index dataset based on several sources."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)
from common_airflow import (
    create_batch_job,
    create_task_spec,
    read_yaml_config,
    shared_dag_args,
    shared_dag_kwargs,
)
from google.cloud import batch_v1

PROJECT_ID = "open-targets-genetics-dev"
REGION = "europe-west1"
CONFIG_FILE_PATH = Path(__file__).parent / "configs" / "variant_sources.yaml"
GENTROPY_DOCKER_IMAGE = "europe-west1-docker.pkg.dev/open-targets-genetics-dev/gentropy-app/gentropy:il-3333"
DST_PATH = "gs://ot-team/irene/il-3333"


@task(task_id="vcf_creation")
def create_vcf(**kwargs: Any) -> None:
    """Task that sends the ConvertToVcfStep job to Google Batch.

    Args:
        **kwargs (Any): Keyword arguments
    """
    sources = read_yaml_config(CONFIG_FILE_PATH)
    task_env = [
        batch_v1.Environment(
            variables={
                "SOURCE_NAME": source["name"],
                "SOURCE_PATH": source["location"],
                "SOURCE_FORMAT": source["format"],
            }
        )
        for source in sources["sources_inclusion_list"]
    ]

    commands = [
        "-c",
        rf"poetry run gentropy step=variant_to_vcf step.source_path=$SOURCE_PATH step.source_format=$SOURCE_FORMAT step.vcf_path={DST_PATH}/$SOURCE_NAME.vcf +step.session.extended_spark_conf={{spark.jars:https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar}}",
    ]
    task = create_task_spec(
        GENTROPY_DOCKER_IMAGE, commands, options="-e HYDRA_FULL_ERROR=1"
    )

    batch_task = CloudBatchSubmitJobOperator(
        task_id="vep_batch_job",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=f"vcf-job-{time.strftime('%Y%m%d-%H%M%S')}",
        job=create_batch_job(
            task,
            "VEPMACHINE",
            task_env,
        ),
        deferrable=False,
    )

    batch_task.execute(context=kwargs)


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — create VCF file from datasets that contain variant information",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as dag:
    (
        create_vcf()
        # create cluster and variant index step (daniel's changes)
    )
