"""Airflow DAG for the preprocessing of eQTL Catalogue's harmonised summary statistics and study table."""
from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.utils.trigger_rule import TriggerRule

CLUSTER_NAME = "otg-preprocess-eqtl-catalogue"
AUTOSCALING = "otg-etl"

SUMSTATS = "gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/preprocess/eqtl_catalogue/summary_stats"
RELEASEBUCKET = "gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — eQTL Catalogue preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    parse_study_and_sumstats = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="eqtl_catalogue_ingestion",
        task_id="eqtl_catalogue_ingestion",
    )
    summary_stats_window_clumping = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="clump",
        task_id="sumstats_window_clumping",
        other_args=[
            f"step.input_path={SUMSTATS}",
            f"step.clumped_study_locus_path={RELEASEBUCKET}/study_locus/window_clumped/from_sumstats/eqtl_catalogue",
        ],
    )
    ld_clumping = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="clump",
        task_id="ld_clumping",
        other_args=[
            "step.input_path={RELEASEBUCKET}/study_locus/window_clumped/from_sumstats/eqtl_catalogue",
            "step.ld_index_path={RELEASEBUCKET}/ld_index",
            "step.study_index_path={RELEASEBUCKET}/study_index/eqtl_catalogue",
            "step.clumped_study_locus_path={RELEASEBUCKET}/study_locus/ld_clumped/from_sumstats/eqtl_catalogue",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    pics = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="pics",
        task_id="pics",
        other_args=[
            "step.study_locus_ld_annotated_in={RELEASEBUCKET}/study_locus/ld_clumped/from_sumstats/eqtl_catalogue",
            "step.picsed_study_locus_out={RELEASEBUCKET}/credible_set/from_sumstats_study_locus/eqtl_catalogue",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        common.create_cluster(
            CLUSTER_NAME, autoscaling_policy=AUTOSCALING, num_workers=5
        )
        >> common.install_dependencies(CLUSTER_NAME)
        >> parse_study_and_sumstats
        >> summary_stats_window_clumping
        >> ld_clumping
        >> pics
        >> common.delete_cluster(CLUSTER_NAME)
    )
