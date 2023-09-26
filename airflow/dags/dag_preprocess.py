"""Apache Airflow workflow to run the Preprocess part of the pipeline."""

from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# Cloud configuration.
project_id = "open-targets-genetics-dev"
region = "europe-west1"
zone = "europe-west1-d"
image_version = "2.1"
cluster_name = "otg-preprocess"

# Executable configuration.
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

# File path configuration.
version = "XX.XX"
inputs = "gs://genetics_etl_python_playground/input"
outputs = f"gs://genetics_etl_python_playground/output/python_etl/parquet/{version}"
spark_write_mode = "overwrite"

# Dataproc cluster configuration.
cluster_generator_config = ClusterGenerator(
    project_id=project_id,
    zone=zone,
    master_machine_type="n1-standard-2",
    master_disk_size=100,
    worker_machine_type="n1-standard-16",
    worker_disk_size=200,
    num_workers=2,
    image_version=image_version,
    enable_component_gateway=True,
    init_actions_uris=initialisation_executable_file,
    metadata={
        "CONFIGTAR": config_tar,
        "PACKAGE": package_wheel,
    },
).make()


def generate_pyspark_job(step: str, **kwargs) -> DataprocSubmitJobOperator:
    """Generates a PySpark Dataproc job given step name and its parameters."""
    return DataprocSubmitJobOperator(
        task_id=f"job-{step}",
        region=region,
        project_id=project_id,
        job={
            "job_uuid": f"airflow-{step}",
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": {
                "main_python_file_uri": f"{initialisation_base_path}/preprocess/{step}.py",
                "args": list(map(str, kwargs.values())),
                "properties": {
                    "spark.jars": "/opt/conda/miniconda3/lib/python3.10/site-packages/hail/backend/hail-all-spark.jar",
                    "spark.driver.extraClassPath": "/opt/conda/miniconda3/lib/python3.10/site-packages/hail/backend/hail-all-spark.jar",
                    "spark.executor.extraClassPath": "./hail-all-spark.jar",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                    "spark.kryo.registrator": "is.hail.kryo.HailKryoRegistrator",
                },
            },
        },
    )


default_args = {
    "owner": "Open Targets Data Team",
    # Tell Airflow to start one day ago, so that it runs as soon as you upload it.
    "start_date": pendulum.now(tz="Europe/London").subtract(days=1),
    "schedule_interval": "@once",
    "project_id": project_id,
    "catchup": False,
    "retries": 0,
}


@dag(
    dag_id="preprocess",
    default_args=default_args,
    description="Open Targets Genetics ETL workflow",
    tags=["genetics_etl", "experimental"],
)
def create_dag() -> None:
    """Preprocess DAG definition."""
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=project_id,
        cluster_config=cluster_generator_config,
        region=region,
        cluster_name=cluster_name,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    install_dependencies = DataprocSubmitJobOperator(
        task_id="install_dependencies",
        region=region,
        project_id=project_id,
        job={
            "job_uuid": "airflow-install-dependencies",
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "pig_job": {
                "jar_file_uris": [
                    f"gs://genetics_etl_python_playground/initialisation/{otg_version}/install_dependencies_on_cluster.sh"
                ],
                "query_list": {
                    "queries": [
                        "sh chmod 750 ${PWD}/install_dependencies_on_cluster.sh",
                        "sh ${PWD}/install_dependencies_on_cluster.sh",
                    ]
                },
            },
        },
    )

    task_ingest_finngen = generate_pyspark_job(
        "ingest_finngen",
        finngen_phenotype_table_url="https://r9.finngen.fi/api/phenos",
        finngen_release_prefix="FINNGEN_R9",
        finngen_sumstat_url_prefix="https://storage.googleapis.com/finngen-public-data-r9/summary_stats/finngen_R9_",
        finngen_sumstat_url_suffix=".gz",
        finngen_study_index_out=f"{outputs}/finngen_study_index",
        spark_write_mode=spark_write_mode,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=project_id,
        cluster_name=cluster_name,
        region="europe-west1",
        trigger_rule=TriggerRule.ALL_DONE,
        deferrable=True,
    )

    create_cluster >> install_dependencies >> [task_ingest_finngen] >> delete_cluster


dag = create_dag()
