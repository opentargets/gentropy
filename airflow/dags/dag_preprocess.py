"""Apache Airflow workflow to run the Preprocess part of the pipeline."""

from __future__ import annotations

from functools import partial

import pendulum
from airflow.decorators import dag
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from common import (
    generate_create_cluster_task,
    generate_pyspark_job,
    outputs,
    read_parquet_from_path,
)

# Workflow specific configuration.
cluster_name = "otg-preprocess"
generate_pyspark_job_partial = partial(generate_pyspark_job, cluster_name)


expandable_operator = DataprocSubmitJobOperator.partial(
    task_id="sumstats", region="europe-west1", project_id="open-targets-genetics-dev"
)


# Temporary duplicated
otg_version = "0.2.0+tskir"
initialisation_base_path = (
    f"gs://genetics_etl_python_playground/initialisation/{otg_version}"
)
python_cli = f"{initialisation_base_path}/cli.py"
config_tar = f"{initialisation_base_path}/config.tar.gz"
package_wheel = f"{initialisation_base_path}/otgenetics-{otg_version}-py3-none-any.whl"
initialisation_executable_file = [
    f"{initialisation_base_path}/install_dependencies_on_cluster.sh"
]


@dag(
    dag_id="otg-preprocess",
    # default_args=default_dag_args,
    description="Open Targets Genetics - Preprocess Workflow",
    tags=["genetics_etl", "experimental"],
    start_date=pendulum.now(tz="Europe/London").subtract(days=1),
    schedule_interval="@once",
    catchup=False,
)
def create_dag() -> None:
    """Preprocess DAG definition."""
    # Common operations.
    create_cluster = generate_create_cluster_task(cluster_name)
    # # delete_cluster = generate_delete_cluster_task(cluster_name)

    # FinnGen ingestion.
    finngen_study_index = f"{outputs}/preprocess/finngen/study_index"

    # ingest_finngen_study_index = generate_pyspark_job_partial(
    #     "finngen/study_index",
    #     finngen_phenotype_table_url="https://r9.finngen.fi/api/phenos",
    #     finngen_release_prefix="FINNGEN_R9",
    #     finngen_summary_stats_url_prefix="gs://finngen-public-data-r9/summary_stats/finngen_R9_",
    #     finngen_summary_stats_url_suffix=".gz",
    #     finngen_study_index_out=finngen_study_index,
    #     spark_write_mode=spark_write_mode,
    # )

    # @task
    # def list_studies_for_summary_stats_ingestion():
    #     """Returns pairs of (studyId, summarystatsLocation) fields for all studies to be ingested."""
    #     df = read_parquet_from_path(finngen_study_index)
    #     selected_columns = df[["studyId", "summarystatsLocation"]]
    #     result_list = [list(x) for x in selected_columns.to_records(index=False)]
    #     return result_list[:2]

    def job_list_studies_for_summary_stats_ingestion():
        """Returns pairs of (studyId, summarystatsLocation) fields for all studies to be ingested."""
        df = read_parquet_from_path(finngen_study_index)
        selected_columns = df[["studyId", "summarystatsLocation"]]
        result_list = [list(x) for x in selected_columns.to_records(index=False)]
        job_list = []
        for study_id, summary_stats_location in result_list:
            d = {
                "job_uuid": f"airflow-ingest-{study_id}",
                "reference": {"project_id": "open-targets-genetics-dev"},
                "placement": {"cluster_name": "otg-preprocess"},
                "pyspark_job": {
                    "main_python_file_uri": f"{initialisation_base_path}/preprocess/finngen/summary_stats.py",
                    "args": [
                        study_id,
                        summary_stats_location,
                        f"{outputs}/preprocess/finngen/summary_stats/{study_id}",
                        "overwrite",
                    ],
                },
            }
            job_list.append(d)

        return job_list[:2]

    # @task
    # def ingest_finngen_summary_stats(arg):
    #     """Submits a PySpark job to ingest summary stats for a single designated FinnGen study."""
    #     print("NE44455555")
    #     finngen_study_id, finngen_summary_stats_location = arg
    #     return generate_pyspark_job_partial(
    #         "finngen/summary_stats",
    #         finngen_study_id=finngen_study_id,
    #         finngen_summary_stats_location=finngen_summary_stats_location,
    #         finngen_summary_stats_out=f"{outputs}/preprocess/finngen/summary_stats/{finngen_study_id}",
    #         spark_write_mode=spark_write_mode,
    #     )

    # This assumes that the study index is already ingested
    # all_summary_stats_operators = []
    # for (
    #     finngen_study_id,
    #     finngen_summary_stats_location,
    # ) in list_studies_for_summary_stats_ingestion():
    #     all_summary_stats_operators.append(
    #         generate_pyspark_job_partial(
    #             f"summary-stats-finngen-{finngen_study_id}",
    #             "finngen/summary_stats.py",
    #             finngen_study_id=finngen_study_id,
    #             finngen_summary_stats_location=finngen_summary_stats_location,
    #             finngen_summary_stats_out=f"{outputs}/preprocess/finngen/summary_stats/{finngen_study_id}",
    #             spark_write_mode=spark_write_mode,
    #         )
    #     )

    # Chaining the tasks.
    # (create_cluster >> ingest_finngen_study_index >> list_studies_for_summary_stats_ingestion())

    # Initial crude version which kinda works
    # create_cluster >> all_summary_stats_operators

    #
    create_cluster >> expandable_operator.expand(
        job=job_list_studies_for_summary_stats_ingestion()
    )


create_dag()
