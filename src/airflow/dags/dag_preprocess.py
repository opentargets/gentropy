"""Apache Airflow workflow to run the Preprocess part of the pipeline."""

from __future__ import annotations

from functools import partial

from airflow.models.dag import DAG
from common_airflow import (
    create_cluster,
    delete_cluster,
    outputs,
    shared_dag_args,
    shared_dag_kwargs,
    submit_pyspark_job,
)

# Workflow specific configuration.
CLUSTER_NAME = "otg-preprocess"
SPARK_WRITE_MODE = "append"
submit_pyspark_job_partial = partial(submit_pyspark_job, CLUSTER_NAME)


with DAG(
    dag_id="otg-preprocess",
    description="Open Targets Genetics — Preprocess Workflow",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
):
    # Ingest FinnGen.
    ingest_finngen = submit_pyspark_job_partial(
        task_id="ingest-finngen",
        python_module_path="finngen.py",
        args=dict(
            finngen_phenotype_table_url="https://r9.finngen.fi/api/phenos",
            finngen_release_prefix="FINNGEN_R9",
            finngen_summary_stats_url_prefix="gs://finngen-public-data-r9/summary_stats/finngen_R9_",
            finngen_summary_stats_url_suffix=".gz",
            finngen_study_index_out=f"{outputs}/preprocess/finngen/study_index",
            finngen_summary_stats_out=f"{outputs}/preprocess/finngen/summary_stats",
            spark_write_mode=SPARK_WRITE_MODE,
        ),
    )

    # Assemble the ingestion actions into DAG.
    (create_cluster(CLUSTER_NAME) >> [ingest_finngen] >> delete_cluster(CLUSTER_NAME))
