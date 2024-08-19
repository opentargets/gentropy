"""Airflow DAG for the Preprocess part of the pipeline."""

from __future__ import annotations

from pathlib import Path

import common_airflow as common

from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

CLUSTER_NAME = "otg-preprocess-finngen"
AUTOSCALING = "finngen-preprocess"


STUDY_INDEX_OUT = "gs://finngen_data/r11/study_index"
CREDIBLE_SETS_SUMMARY_IN = "gs://finngen-public-data-r11/finemap/full/susie/*.cred.bgz"
SNP_IN = "gs://finngen-public-data-r11/finemap/full/susie/*.snp.bgz"
FINNGEN_PREFIX = "FINNGEN_R11_"
FINEMAPPING_OUT = "gs://finngen_data/r11/finemapping"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Finngen preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    # list susie finemapping snp files

    finngen_finemapping_ingestion = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="finngen_finemapping_ingestion",
        task_id="finngen_finemapping_ingestion",
        other_args=[
            f"step.finngen_finemapping_out={FINEMAPPING_OUT}",
            f"step.finngen_release_prefix={FINNGEN_PREFIX}",
            f"step.finngen_susie_finemapping_snp_files={SNP_IN}",
            f"step.finngen_susie_finemapping_cs_summary_files={CREDIBLE_SETS_SUMMARY_IN}",
        ],
        # This allows to attempt running the task when above step fails do to failifexists
        trigger_rule=TriggerRule.ALL_DONE,
    )
    with TaskGroup(
        group_id="finngen_summary_stats_preprocess"
    ) as finngen_summary_stats_preprocess:
        study_index = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="finngen_studies",
            task_id="finngen_studies",
            other_args=[
                f"step.finngen_study_index_out={STUDY_INDEX_OUT}",
            ],
        )

        # Define order of steps:
        (study_index)
    (
        common.create_cluster(
            CLUSTER_NAME,
            autoscaling_policy=AUTOSCALING,
            master_disk_size=2000,
            num_workers=6,
        )
        >> common.install_dependencies(CLUSTER_NAME)
        >> [finngen_summary_stats_preprocess, finngen_finemapping_ingestion]
        >> common.delete_cluster(CLUSTER_NAME)
    )
