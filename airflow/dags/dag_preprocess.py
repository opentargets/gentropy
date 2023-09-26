"""Apache Airflow workflow to run the Preprocess part of the pipeline."""

from __future__ import annotations

from functools import partial

from airflow.decorators import dag
from common import (
    default_dag_args,
    generate_create_cluster_task,
    generate_delete_cluster_task,
    generate_pyspark_job,
    outputs,
    spark_write_mode,
)

# Workflow specific configuration.
cluster_name = "otg-preprocess"
generate_pyspark_job_partial = partial(generate_pyspark_job, cluster_name)


@dag(
    dag_id="otg-preprocess",
    default_args=default_dag_args,
    description="Open Targets Genetics - Preprocess Workflow",
    tags=["genetics_etl", "experimental"],
)
def create_dag() -> None:
    """Preprocess DAG definition."""
    create_cluster = generate_create_cluster_task(cluster_name)

    ingest_finngen = generate_pyspark_job_partial(
        "ingest_finngen",
        finngen_phenotype_table_url="https://r9.finngen.fi/api/phenos",
        finngen_release_prefix="FINNGEN_R9",
        finngen_summary_stats_url_prefix="https://storage.googleapis.com/finngen-public-data-r9/summary_stats/finngen_R9_",
        finngen_summary_stats_url_suffix=".gz",
        finngen_study_index_out=f"{outputs}/preprocess/finngen/study_index",
        finngen_summary_stats_out=f"{outputs}/preprocess/finngen/summary_stats",
        spark_write_mode=spark_write_mode,
    )

    delete_cluster = generate_delete_cluster_task(cluster_name)

    create_cluster >> [ingest_finngen] >> delete_cluster


dag = create_dag()
