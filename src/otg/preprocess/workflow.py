"""Apache Airflow workflow to run the Preprocess part of the pipeline."""

from __future__ import annotations

from airflow.decorators import task

from otg.preprocess.ingest_finngen import ingest_finngen

# Common configuration.
version = "XX.XX"
inputs = "gs://genetics_etl_python_playground/input"
outputs = f"gs://genetics_etl_python_playground/output/python_etl/parquet/{version}"
spark_write_mode = "overwrite"


@task
def task_ingest_finngen():
    """Airflow task to ingest FinnGen."""
    ingest_finngen(
        finngen_phenotype_table_url="https://r9.finngen.fi/api/phenos",
        finngen_release_prefix="FINNGEN_R9",
        finngen_sumstat_url_prefix="https://storage.googleapis.com/finngen-public-data-r9/summary_stats/finngen_R9_",
        finngen_sumstat_url_suffix=".gz",
        finngen_study_index_out=f"{outputs}/finngen_study_index",
        spark_write_mode=spark_write_mode,
    )
