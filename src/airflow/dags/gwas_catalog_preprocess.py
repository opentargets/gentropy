"""Airflow DAG for the preprocessing of GWAS Catalog's harmonised summary statistics and curated associations."""
from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

CLUSTER_NAME = "otg-preprocess-gwascatalog"
AUTOSCALING = "otg-preprocess-gwascatalog"

SUMSTATS = "gs://open-targets-gwas-summary-stats/harmonised"
RELEASEBUCKET = "gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    with TaskGroup(group_id="summary_stats_preprocessing") as summary_stats_group:
        summary_stats_window_clumping = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="clump",
            task_id="catalog_sumstats_window_clumping",
            other_args=[
                f"step.input_path={SUMSTATS}",
                f"step.clumped_study_locus_path={RELEASEBUCKET}/study_locus/window_clumped/from_sumstats/catalog",
            ],
        )
        summary_stats_ld_clumping = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="clump",
            task_id="catalog_sumstats_ld_clumping",
            other_args=[
                f"step.input_path={RELEASEBUCKET}/study_locus/window_clumped/from_sumstats/catalog",
                "step.ld_index_path={RELEASEBUCKET}/ld_index",
                "step.study_index_path={RELEASEBUCKET}/study_index/catalog",
                "step.clumped_study_locus_path={RELEASEBUCKET}/study_locus/ld_clumped/from_sumstats/catalog",
            ],
            trigger_rule=TriggerRule.ALL_DONE,
        )
        summary_stats_pics = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="pics",
            task_id="catalog_sumstats_pics",
            other_args=[
                "step.study_locus_ld_annotated_in={RELEASEBUCKET}/study_locus/ld_clumped/from_sumstats/catalog",
                "step.picsed_study_locus_out={RELEASEBUCKET}/credible_set/from_sumstats/catalog",
            ],
            trigger_rule=TriggerRule.ALL_DONE,
        )
        summary_stats_window_clumping >> summary_stats_ld_clumping >> summary_stats_pics

    with TaskGroup(group_id="curation_preprocessing") as curation_group:
        parse_study_and_curated_assocs = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="gwas_catalog_ingestion",
            task_id="catalog_ingestion",
        )

        curation_ld_clumping = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="clump",
            task_id="catalog_curation_ld_clumping",
            other_args=[
                "step.input_path={RELEASEBUCKET}/study_locus/catalog_curated",
                "step.ld_index_path={RELEASEBUCKET}/ld_index",
                "step.study_index_path={RELEASEBUCKET}/study_index/catalog",
                "step.clumped_study_locus_path={RELEASEBUCKET}/study_locus/ld_clumped/catalog_curated",
            ],
            trigger_rule=TriggerRule.ALL_DONE,
        )

        curation_pics = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="pics",
            task_id="catalog_curation_pics",
            other_args=[
                "step.study_locus_ld_annotated_in={RELEASEBUCKET}/study_locus/ld_clumped/catalog_curated",
                "step.picsed_study_locus_out={RELEASEBUCKET}/credible_set/catalog_curated",
            ],
            trigger_rule=TriggerRule.ALL_DONE,
        )
        parse_study_and_curated_assocs >> curation_ld_clumping >> curation_pics

    (
        common.create_cluster(
            CLUSTER_NAME, autoscaling_policy=AUTOSCALING, num_workers=5
        )
        >> common.install_dependencies(CLUSTER_NAME)
        >> [summary_stats_group, curation_group]
        >> common.delete_cluster(CLUSTER_NAME)
    )
