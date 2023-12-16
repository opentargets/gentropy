"""Airflow DAG for the preprocessing of GWAS Catalog's harmonised summary statistics and curated associations."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import common_airflow as common
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

CLUSTER_NAME = "otg-preprocess-gwascatalog"
AUTOSCALING = "otg-preprocess-gwascatalog"

RELEASEBUCKET = "gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX"
SUMMARY_STATS_BUCKET_NAME = "open-targets-gwas-summary-stats"
SUMSTATS = "gs://open-targets-gwas-summary-stats/harmonised"
MANIFESTS_PATH = f"{RELEASEBUCKET}/manifests/"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    list_harmonised_sumstats = GCSListObjectsOperator(
        task_id="list_harmonised_parquet",
        bucket=SUMMARY_STATS_BUCKET_NAME,
        prefix="harmonised",
        match_glob="**/_SUCCESS",
    )

    with TaskGroup(
        group_id="summary_stats_preprocessing"
    ) as summary_stats_preprocessing_group:

        @task(task_id="calculate_inclusion_list")
        def calculate_inclusion_list_sumstats(
            **kwargs: Any,
        ) -> DataprocSubmitJobOperator:
            """Calculate the inclusion list for GWAS catalog studies.

            Args:
                **kwargs (Any): The context passed by Airflow.

            Returns:
                DataprocSubmitJobOperator: Task operator.
            """
            ti = kwargs["ti"]
            context = kwargs["context"]
            # Pull the data from XCom.
            harmonised_list = ti.xcom_pull(
                key="list_harmonised_parquet", task_ids="return_value"
            )
            return common.submit_step(
                cluster_name=CLUSTER_NAME,
                step_id="inclusion_list",
                task_id="catalog_sumstats_inclusion_list",
                other_args=[
                    "step.criteria=sumstats",
                    f"step.harmonised_list={harmonised_list}",
                    f"step.inclusion_list_out={ MANIFESTS_PATH }/manifest_sumstats_{context['dag_run'].run_id}.txt",
                    f"step.exclusion_list_out={ MANIFESTS_PATH }/exclusion_sumstats_{context['dag_run'].run_id}.txt",
                ],
            )

        @task(task_id="summary_stats_window_clumping")
        def summary_stats_window_clumping(**kwargs: Any) -> DataprocSubmitJobOperator:
            """Window-based clumping of summary statistics.

            Args:
                **kwargs(Any): The context passed by Airflow.

            Returns:
                DataprocSubmitJobOperator: Task operator.
            """
            context = kwargs["context"]
            return common.submit_step(
                cluster_name=CLUSTER_NAME,
                step_id="clump",
                task_id="catalog_sumstats_window_clumping",
                other_args=[
                    f"step.manifest={ MANIFESTS_PATH }/manifest_{context['dag_run'].run_id}.txt",
                    f"step.clumped_study_locus_path={RELEASEBUCKET}/study_locus/window_clumped/from_sumstats/catalog",
                ],
            )

        (calculate_inclusion_list_sumstats() >> summary_stats_window_clumping())

    with TaskGroup(
        group_id="sumstats_postprocessing_group"
    ) as sumstats_postprocessing_group:
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

        summary_stats_ld_clumping >> summary_stats_pics

    parse_study_and_curated_assocs = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="gwas_catalog_ingestion",
        task_id="catalog_ingestion",
    )

    @task(task_id="calculate_inclusion_list_curated")
    def calculate_inclusion_list_curated(
        **kwargs: Any,
    ) -> DataprocSubmitJobOperator:
        """Calculate the inclusion list of for GWAS Catalog curated studies.

        Args:
            **kwargs(Any): The context passed by Airflow.

        Returns:
                DataprocSubmitJobOperator: Task operator.
        """
        ti = kwargs["ti"]
        context = kwargs["context"]
        # Pull the data from XCom.
        harmonised_list = ti.xcom_pull(
            key="list_harmonised_parquet", task_ids="return_value"
        )
        return common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="inclusion_list",
            task_id="catalog_curated_inclusion_list",
            other_args=[
                "step.criteria=curated",
                f"step.harmonised_list={harmonised_list}",
                f"step.inclusion_list_out={ MANIFESTS_PATH }/manifest_curated_{context['dag_run'].run_id}.txt",
                f"step.exclusion_list_out={ MANIFESTS_PATH }/exclusion_curated_{context['dag_run'].run_id}.txt",
            ],
        )

    with TaskGroup(
        group_id="curation_postprocessing_group"
    ) as curation_postprocessing_group:
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
        curation_ld_clumping >> curation_pics

    (
        common.create_cluster(
            CLUSTER_NAME, autoscaling_policy=AUTOSCALING, num_workers=5
        )
        >> common.install_dependencies(CLUSTER_NAME)
        >> list_harmonised_sumstats
        >> [summary_stats_preprocessing_group, calculate_inclusion_list_curated()]
        >> parse_study_and_curated_assocs
        >> [curation_postprocessing_group, sumstats_postprocessing_group]
        >> common.delete_cluster(CLUSTER_NAME)
    )
