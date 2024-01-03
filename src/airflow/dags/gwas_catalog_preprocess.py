"""Airflow DAG for the preprocessing of GWAS Catalog's harmonised summary statistics and curated associations."""
from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.utils.task_group import TaskGroup

CLUSTER_NAME = "otg-preprocess-gwascatalog"
AUTOSCALING = "otg-preprocess-gwascatalog"

RELEASEBUCKET = "gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX"
RELEASEBUCKET_NAME = "genetics_etl_python_playground"
SUMMARY_STATS_BUCKET_NAME = "open-targets-gwas-summary-stats"
SUMSTATS = "gs://open-targets-gwas-summary-stats/harmonised"
MANIFESTS_PATH = f"{RELEASEBUCKET}/manifests/"


def upload_harmonized_study_list(
    concatenated_studies: str, bucket_name: str, object_name: str
) -> None:
    """This function uploads file to GCP.

    Args:
        concatenated_studies (str): Concatenated list of harmonized summary statistics.
        bucket_name (str): Bucket name
        object_name (str): Name of the object
    """
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    hook.upload(
        bucket_name=bucket_name,
        object_name=object_name,
        data=concatenated_studies,
        encoding="utf-8",
    )


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    # Getting list of folders (each a gwas study with summary statistics)
    list_harmonised_sumstats = GCSListObjectsOperator(
        task_id="list_harmonised_parquet",
        bucket=SUMMARY_STATS_BUCKET_NAME,
        prefix="harmonised",
        match_glob="**/_SUCCESS",
    )

    # Upload resuling list to a bucket:
    upload_task = PythonOperator(
        task_id="uploader",
        python_callable=upload_harmonized_study_list,
        op_kwargs={
            "concatenated_studies": '{{ "\n".join(ti.xcom_pull( key="return_value", task_ids="list_harmonised_parquet")) }}',
            "bucket_name": RELEASEBUCKET_NAME,
            "object_name": "output/python_etl/parquet/XX.XX/manifests/harmonised_sumstats.txt",
        },
    )

    # Processing curated GWAS Catalog top-bottom:
    with TaskGroup(group_id="curation_processing") as curation_processing:
        # Generate inclusion list:
        curation_calculate_inclusion_list = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="gwas_study_inclusion",
            task_id="catalog_curation_inclusion_list",
            other_args=[
                "step.criteria=curation",
                f"step.inclusion_list_path={MANIFESTS_PATH}manifest_curation",
                f"step.exclusion_list_path={MANIFESTS_PATH}exclusion_curation",
                f"step.harmonised_study_file={MANIFESTS_PATH}harmonised_sumstats.txt",
            ],
        )

        # Ingest curated associations from GWAS Catalog:
        curation_ingest_data = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="gwas_catalog_ingestion",
            task_id="ingest_curated_gwas_catalog_data",
            other_args=[f"step.inclusion_list_path={MANIFESTS_PATH}manifest_curation"],
        )

        # Run LD-annotation and clumping on curated data:
        curation_ld_clumping = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="ld_based_clumping",
            task_id="catalog_curation_ld_clumping",
            other_args=[
                f"step.study_locus_input_path={RELEASEBUCKET}/study_locus/catalog_curated",
                f"step.ld_index_path={RELEASEBUCKET}/ld_index",
                f"step.study_index_path={RELEASEBUCKET}/study_index/catalog",
                f"step.clumped_study_locus_output_path={RELEASEBUCKET}/study_locus/ld_clumped/catalog_curated",
            ],
        )

        # Do PICS based finemapping:
        curation_pics = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="pics",
            task_id="catalog_curation_pics",
            other_args=[
                f"step.study_locus_ld_annotated_in={RELEASEBUCKET}/study_locus/ld_clumped/catalog_curated",
                f"step.picsed_study_locus_out={RELEASEBUCKET}/credible_set/catalog_curated",
            ],
        )

        # Define order of steps:
        (
            curation_calculate_inclusion_list
            >> curation_ingest_data
            >> curation_ld_clumping
            >> curation_pics
        )

    # Processing summary statistics from GWAS Catalog:
    with TaskGroup(
        group_id="summary_satistics_processing"
    ) as summary_satistics_processing:
        # Generate inclusion study lists:
        summary_stats_calculate_inclusion_list = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="gwas_study_inclusion",
            task_id="catalog_sumstats_inclusion_list",
            other_args=[
                "step.criteria=summary_stats",
                f"step.inclusion_list_path={MANIFESTS_PATH}manifest_sumstats",
                f"step.exclusion_list_path={MANIFESTS_PATH}exclusion_sumstats",
                f"step.harmonised_study_file={MANIFESTS_PATH}harmonised_sumstats.txt",
            ],
        )

        # Run window-based clumping:
        summary_stats_window_based_clumping = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="window_based_clumping",
            task_id="catalog_sumstats_window_clumping",
            other_args=[
                f"step.summary_statistics_input_path={SUMSTATS}",
                f"step.study_locus_output_path={RELEASEBUCKET}/study_locus/window_clumped/from_sumstats/catalog",
                f"step.inclusion_list_path={MANIFESTS_PATH}manifest_sumstats",
            ],
        )

        # Run LD based clumping:
        summary_stats_ld_clumping = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="ld_based_clumping",
            task_id="catalog_sumstats_ld_clumping",
            other_args=[
                f"step.study_locus_input_path={RELEASEBUCKET}/study_locus/window_clumped/from_sumstats/catalog",
                f"step.ld_index_path={RELEASEBUCKET}/ld_index",
                f"step.study_index_path={RELEASEBUCKET}/study_index/catalog",
                f"step.clumped_study_locus_output_path={RELEASEBUCKET}/study_locus/ld_clumped/from_sumstats/catalog",
            ],
        )

        # Run PICS finemapping:
        summary_stats_pics = common.submit_step(
            cluster_name=CLUSTER_NAME,
            step_id="pics",
            task_id="catalog_sumstats_pics",
            other_args=[
                f"step.study_locus_ld_annotated_in={RELEASEBUCKET}/study_locus/ld_clumped/from_sumstats/catalog",
                f"step.picsed_study_locus_out={RELEASEBUCKET}/credible_set/from_sumstats/catalog",
            ],
        )

        # Order of steps within the group:
        (
            summary_stats_calculate_inclusion_list
            >> summary_stats_window_based_clumping
            >> summary_stats_ld_clumping
            >> summary_stats_pics
        )

    # DAG description:
    (
        # common.create_cluster(
        #     CLUSTER_NAME, autoscaling_policy=AUTOSCALING, num_workers=5
        # ) >>
        common.install_dependencies(CLUSTER_NAME)
        >> list_harmonised_sumstats
        >> upload_task
        >> curation_processing
        >> summary_satistics_processing
    )
