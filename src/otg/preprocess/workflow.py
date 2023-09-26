"""Apache Airflow workflow to run the Preprocess part of the pipeline."""

from __future__ import annotations

from airflow.decorators import task
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from otg.preprocess.ingest_finngen import ingest_finngen

# Cloud configuration.
project_id = "open-targets-genetics-dev"
region = "europe-west1"
zone = "europe-west1-d"
image_version = "2.1"
cluster_name = "otg-preprocess"

# Executable configuration.
otg_version = "0.1.4"
initialisation_base_path = (
    f"gs://genetics_etl_python_playground/initialisation/{otg_version}"
)
python_cli = f"{initialisation_base_path}/cli.py"
package_wheel = f"{initialisation_base_path}/otgenetics-{otg_version}-py3-none-any.whl"
initialisation_executable_file = [f"{initialisation_base_path}/initialise_cluster.sh"]

# File path configuration.
version = "XX.XX"
inputs = "gs://genetics_etl_python_playground/input"
outputs = f"gs://genetics_etl_python_playground/output/python_etl/parquet/{version}"
spark_write_mode = "overwrite"


# Setting up Dataproc cluster.
cluster_generator_config = ClusterGenerator(
    project_id=project_id,
    zone=zone,
    master_machine_type="n1-standard-2",
    master_disk_size=100,
    worker_machine_type="n1-standard-16",
    worker_disk_size=200,
    num_workers=1,
    image_version=image_version,
    enable_component_gateway=True,
    init_actions_uris=initialisation_executable_file,
    metadata={
        "PACKAGE": package_wheel,
    },
).make()
create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=project_id,
    cluster_config=cluster_generator_config,
    region=region,
    cluster_name=cluster_name,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)


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
