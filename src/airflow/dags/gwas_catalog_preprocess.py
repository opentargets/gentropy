"""Airflow DAG for the preprocessing of GWAS Catalog's harmonised summary statistics and curated associations."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import common_airflow as common
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

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


@task(task_id="summary_stats_window_clumping")
def summary_stats_window_clumping(**kwargs: Any) -> DataprocSubmitJobOperator:
    """Window-based clumping of summary statistics.

    Args:
        **kwargs(Any): The context passed by Airflow.

    Returns:
        DataprocSubmitJobOperator: Task operator.
    """
    return common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="clump",
        task_id="catalog_sumstats_window_clumping",
        other_args=[
            f"step.inclusion_list_path={MANIFESTS_PATH}/manifest_sumstats",
            f"step.clumped_study_locus_path={RELEASEBUCKET}/study_locus/window_clumped/from_sumstats/catalog",
        ],
    )


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
    upload_task = PythonOperator(
        task_id="uploader",
        python_callable=upload_harmonized_study_list,
        op_kwargs={
            "concatenated_studies": '{{ "\n".join(ti.xcom_pull( key="return_value", task_ids="list_harmonised_parquet")) }}',
            "bucket_name": RELEASEBUCKET_NAME,
            "object_name": "output/python_etl/parquet/XX.XX/manifests/harmonised_sumstats.txt",
        },
    )

    calculate_inclusion_list_sumstats = common.submit_step(
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

    (
        # common.create_cluster(
        #     CLUSTER_NAME, autoscaling_policy=AUTOSCALING, num_workers=5
        # ) >>
        common.install_dependencies(CLUSTER_NAME)
        >> list_harmonised_sumstats
        >> upload_task
        >> calculate_inclusion_list_sumstats
    )
