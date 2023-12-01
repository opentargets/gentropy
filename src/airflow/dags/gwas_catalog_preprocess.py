"""Airflow DAG for the preprocessing of GWAS Catalog's harmonised summary statistics and curated associations."""
from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.utils.trigger_rule import TriggerRule

CLUSTER_NAME = "otg-preprocess-gwascatalog"
AUTOSCALING = "otg-preprocess-gwascatalog"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    summary_stats_window_clumping = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="clump",
        task_id="gwas_catalog_summary_stats_window_clumping",
        other_args=[
            "step.input_path=gs://open-targets-gwas-summary-stats/harmonised",
            "step.clumped_study_locus_path=gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/study_locus/from_sumstats_study_locus_window_clumped",
        ],
    )
    summary_stats_ld_clumping = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="clump",
        task_id="gwas_catalog_summary_stats_ld_clumping",
        other_args=[
            "step.input_path=gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/study_locus/from_sumstats_study_locus_window_clumped",
            "step.clumped_study_locus_path=gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/study_locus/from_sumstats_study_locus_ld_clumped",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )
    summary_stats_pics = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="pics",
        task_id="gwas_catalogs_pics",
        other_args=[
            "step.study_locus_ld_annotated_in=gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/study_locus/from_sumstats_study_locus_ld_clumped",
            "step.picsed_study_locus_out=gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/credible_set/from_sumstats_study_locus/gwas_catalog",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    parse_study_and_curated_assocs = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="gwas_catalog_ingestion",
        task_id="gwas_catalog_ingestion",
    )

    curation_ld_clumping = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="clump",
        task_id="gwas_catalog_curation_ld_clumping",
        other_args=[
            "step.input_path=gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/study_locus/catalog_study_locus",
            "step.clumped_study_locus_path=gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/study_locus/catalog_study_locus_ld_clumped",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    curation_pics = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="pics",
        task_id="gwas_catalogs_curation_pics",
        other_args=[
            "step.study_locus_ld_annotated_in=gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/study_locus/catalog_study_locus_ld_clumped",
            "step.picsed_study_locus_out=gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/credible_set/catalog_curated",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        common.create_cluster(CLUSTER_NAME, autoscaling_policy=AUTOSCALING)
        >> common.install_dependencies(CLUSTER_NAME)
        >> summary_stats_window_clumping
        >> summary_stats_ld_clumping
        >> summary_stats_pics
        >> parse_study_and_curated_assocs
        >> curation_ld_clumping
        >> curation_pics
        >> common.delete_cluster(CLUSTER_NAME)
    )
