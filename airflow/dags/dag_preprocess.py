"""Apache Airflow workflow to run the Preprocess part of the pipeline."""

from __future__ import annotations

from functools import partial

import pendulum
from airflow.decorators import dag, task, task_group
from common import (
    generate_create_cluster_task,
    generate_dataproc_submit_job_operator,
    generate_delete_cluster_task,
    generate_pyspark_job_params,
    outputs,
    read_parquet_from_path,
    spark_write_mode,
)

# Workflow specific configuration.
cluster_name = "otg-preprocess"
generate_pyspark_job_params_partial = partial(generate_pyspark_job_params, cluster_name)


@dag(
    dag_id="otg-preprocess",
    description="Open Targets Genetics - Preprocess Workflow",
    tags=["genetics_etl", "experimental"],
    start_date=pendulum.now(tz="Europe/London").subtract(days=1),
    schedule_interval="@once",
    catchup=False,
    max_active_tasks=32,
    concurrency=32,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(seconds=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=3),
    },
)
def create_dag() -> None:
    """Preprocess DAG definition."""
    # Common operations.
    create_cluster = generate_create_cluster_task(cluster_name)
    delete_cluster = generate_delete_cluster_task(cluster_name)

    # FinnGen ingestion.
    finngen_study_index = f"{outputs}/preprocess/finngen/study_index"

    ingest_finngen_study_index = generate_dataproc_submit_job_operator(
        task_id="ingest_study_index",
        job=generate_pyspark_job_params_partial(
            "finngen/study_index.py",
            finngen_phenotype_table_url="https://r9.finngen.fi/api/phenos",
            finngen_release_prefix="FINNGEN_R9",
            finngen_summary_stats_url_prefix="gs://finngen-public-data-r9/summary_stats/finngen_R9_",
            finngen_summary_stats_url_suffix=".gz",
            finngen_study_index_out=finngen_study_index,
            spark_write_mode=spark_write_mode,
        ),
    )

    @task_group(group_id="finngen_summary_stats")
    def ingest_finngen_summary_stats():
        """Summary stats ingestion for FinnGen. Defined as a task group because we must wait for the study index ingestion first."""

        @task
        def job_list_studies_for_summary_stats_ingestion():
            """Generates summary stats ingestion jobs based on the study index."""
            df = read_parquet_from_path(finngen_study_index)
            selected_columns = df[["studyId", "summarystatsLocation"]]
            result_list = [list(x) for x in selected_columns.to_records(index=False)]
            job_list = [
                generate_pyspark_job_params_partial(
                    "finngen/summary_stats.py",
                    finngen_study_id=study_id,
                    finngen_summary_stats_location=summary_stats_location,
                    finngen_summary_stats_out=f"{outputs}/preprocess/finngen/summary_stats/{study_id}",
                    spark_write_mode="overwrite",
                )
                for study_id, summary_stats_location in result_list
            ]
            return job_list

        # Run FinnGen summary statistics ingestion on all studies.
        generate_dataproc_submit_job_operator(task_id="finngen_sumstats").expand(
            job=job_list_studies_for_summary_stats_ingestion()
        )

    (
        create_cluster
        >> ingest_finngen_study_index
        >> ingest_finngen_summary_stats()
        >> delete_cluster
    )


create_dag()
