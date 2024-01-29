"""Airflow DAG for the Preprocess part of the pipeline."""
from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

CLUSTER_NAME = "otg-preprocess-finngen"
AUTOSCALING = "finngen-preprocess"

# Get all parameters for the DAG:
FINNGEN_VERSION = "r10"
FINNGEN_BUCKET = f"gs://finngen_data/{FINNGEN_VERSION}"

STUDY_INDEX = f"{FINNGEN_BUCKET}/study_index"
SUMMARY_STATISTICS = f"{FINNGEN_BUCKET}/harmonised_summary_statistics"
WINDOW_BASED_CLUMPED = f"{FINNGEN_BUCKET}/study_locus_datasets/finngen_window_clumped"
LD_CLUMPED = f"{FINNGEN_BUCKET}/study_locus_datasets/finngen_ld_clumped"
PICSED_CREDIBLE_SET = f"{FINNGEN_BUCKET}/credible_set_datasets/finngen_pics"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Finngen preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    finngen_finemapping_ingestion = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="ot_finngen_finemapping_ingestion",
        task_id="finngen_finemapping_ingestion",
        # This allows to attempt running the task when above step fails do to failifexists
        trigger_rule=TriggerRule.ALL_DONE,
    )
    with TaskGroup(
        group_id="finngen_summary_stats_preprocess"
    ) as finngen_summary_stats_preprocess:
        study_index = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="ot_finngen_studies",
            task_id="finngen_studies",
            other_args=[
                f"step.finngen_study_index_out={STUDY_INDEX}",
            ],
        )

        window_based_clumping = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="window_based_clumping",
            task_id="finngen_window_based_clumping",
            other_args=[
                f"step.summary_statistics_input_path={SUMMARY_STATISTICS}",
                f"step.study_locus_output_path={WINDOW_BASED_CLUMPED}",
            ],
        )
        ld_clumping = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="ot_ld_based_clumping",
            task_id="finngen_ld_clumping",
            other_args=[
                f"step.study_locus_input_path={WINDOW_BASED_CLUMPED}",
                f"step.study_index_path={STUDY_INDEX}",
                f"step.clumped_study_locus_output_path={LD_CLUMPED}",
            ],
            trigger_rule=TriggerRule.ALL_DONE,
        )
        pics = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="ot_pics",
            task_id="finngen_pics",
            other_args=[
                f"step.study_locus_ld_annotated_in={LD_CLUMPED}",
                f"step.picsed_study_locus_out={PICSED_CREDIBLE_SET}",
            ],
            # This allows to attempt running the task when above step fails do to failifexists
            trigger_rule=TriggerRule.ALL_DONE,
        )
        # Define order of steps:
        (study_index >> window_based_clumping >> ld_clumping >> pics)
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
