"""Airflow DAG for the Preprocess part of the pipeline."""
from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)

# CLUSTER_NAME = "otg-preprocess-finngen"
# AUTOSCALING = "finngen-preprocess"

EQTL_CATALOG_SUSIE_LOCATION = "gs://eqtl_catalog_data/ebi_ftp/susie"
TEMP_DECOMPRESS_LOCATION = "gs://eqtl_catalog_data/tmp_susie_decompressed"
DECOMPRESS_FAILED_LOG = "gs://eqtl_catalog_data/tmp_susie_decompressed.log"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — eQTL preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    start_template_job = DataflowTemplatedJobStartOperator(
        task_id="decompress_sussie_outputs",
        template="gs://dataflow-templates/latest/Bulk_Decompress_GCS_Files",
        location="europe-west1",
        project_id="open-targets-genetics-dev",
        parameters={
            "inputFilePattern": f"{EQTL_CATALOG_SUSIE_LOCATION}/**/*.gz",
            "outputDirectory": TEMP_DECOMPRESS_LOCATION,
            "outputFailureFile": DECOMPRESS_FAILED_LOG,
        },
    )

    # (
    #     common.create_cluster(
    #         CLUSTER_NAME,
    #         autoscaling_policy=AUTOSCALING,
    #         master_disk_size=2000,
    #         num_workers=6,
    #     )
    #     >> common.install_dependencies(CLUSTER_NAME)
    #     >> [finngen_summary_stats_preprocess, finngen_finemapping_ingestion]
    #     >> common.delete_cluster(CLUSTER_NAME)
    # )

    start_template_job
