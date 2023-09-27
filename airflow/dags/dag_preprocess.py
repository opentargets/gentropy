"""Apache Airflow workflow to run the Preprocess part of the pipeline."""

from __future__ import annotations

from functools import partial

import pandas as pd
from airflow.decorators import dag, task
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
    # Common operations.
    create_cluster = generate_create_cluster_task(cluster_name)
    delete_cluster = generate_delete_cluster_task(cluster_name)

    # FinnGen ingestion.
    finngen_study_index = f"{outputs}/preprocess/finngen/study_index"

    ingest_finngen_study_index = generate_pyspark_job_partial(
        "finngen/study_index",
        finngen_phenotype_table_url="https://r9.finngen.fi/api/phenos",
        finngen_release_prefix="FINNGEN_R9",
        finngen_summary_stats_url_prefix="gs://finngen-public-data-r9/summary_stats/finngen_R9_",
        finngen_summary_stats_url_suffix=".gz",
        finngen_study_index_out=finngen_study_index,
        spark_write_mode=spark_write_mode,
    )

    @task
    def list_studies_for_summary_stats_ingestion():
        """Returns pairs of (studyId, summarystatsLocation) fields for all studies to be ingested."""
        df = pd.read_parquet(finngen_study_index)
        selected_columns = df[["studyId", "summarystatsLocation"]]
        result_list = [list(x) for x in selected_columns.to_records(index=False)]
        return result_list[:10]

    @task
    def ingest_finngen_summary_stats(args):
        """Submits a PySpark job to ingest summary stats for a single designated FinnGen study."""
        finngen_study_id, finngen_summary_stats_location = args
        return generate_pyspark_job_partial(
            "finngen/summary_stats",
            finngen_study_id=finngen_study_id,
            finngen_summary_stats_location=finngen_summary_stats_location,
            finngen_summary_stats_out=f"{outputs}/preprocess/finngen/summary_stats/{finngen_study_id}",
            spark_write_mode=spark_write_mode,
        )

    # Chaining the tasks.
    (
        create_cluster
        >> ingest_finngen_study_index
        >> ingest_finngen_summary_stats.expand(
            args=list_studies_for_summary_stats_ingestion()
        )
        >> delete_cluster
    )


dag = create_dag()
