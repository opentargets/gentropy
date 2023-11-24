"""Airflow DAG for the preprocessing of GWAS Catalog's harmonised summary statistics and curated associations."""
from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.utils.trigger_rule import TriggerRule

CLUSTER_NAME = "otg-preprocess-gwascatalog"
AUTOSCALING = "gwascatalog-preprocess"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    study_and_curated_assocs = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="gwas_catalog",  # TODO: change name
        task_id="gwas_catalog_study_and_curated_assocs",
    )

    summary_stats = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="gwas_catalog_process_summary_stats",
        task_id="gwas_catalog_summary_stats",
    )

    ld_clumping = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="clump",
        task_id="gwas_catalog_clump",
        other_args=[
            "step.study_locus_in=gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/study_locus/from_sumstats_study_locus/gwas_catalog",  # TODO: update pattern to match curated and from_sumstats
            "step.clumped_study_locus_out=gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/study_locus/from_sumstats_study_locus/gwas_catalog",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    pics = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="pics",
        task_id="gwas_catalogs_pics",
        other_args=[
            "step.study_locus_ld_annotated_in=gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/study_locus/from_sumstats_study_locus/finngen",  # TODO: update pattern to match curated and from_sumstats
            "step.picsed_study_locus_out=gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX/credible_set/from_sumstats_study_locus/finngen",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        common.create_cluster(CLUSTER_NAME, autoscaling_policy=AUTOSCALING)
        >> common.install_dependencies(CLUSTER_NAME)
        >> study_and_curated_assocs
        >> summary_stats
        >> ld_clumping
        >> pics
        >> common.delete_cluster(CLUSTER_NAME)
    )
