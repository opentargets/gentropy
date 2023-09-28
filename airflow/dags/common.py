"""Airflow boilerplate code which can be shared by several workflows."""

from __future__ import annotations

import binascii
import os

import gcsfs
import pandas as pd
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# Code version. It has to be repeated here as well as in `pyproject.toml`, because Airflow isn't able to look at files outside of its `dags/` directory.
otg_version = "1.0.0"


# Cloud configuration.
project_id = "open-targets-genetics-dev"
region = "europe-west1"
zone = "europe-west1-d"
image_version = "2.1"
google_application_credentials = (
    "/opt/airflow/config/application_default_credentials.json"
)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_application_credentials
os.environ["GOOGLE_CLOUD_PROJECT"] = project_id


# Executable configuration.
initialisation_base_path = (
    f"gs://genetics_etl_python_playground/initialisation/{otg_version}"
)
python_cli = f"{initialisation_base_path}/cli.py"
config_tar = f"{initialisation_base_path}/config.tar.gz"
package_wheel = f"{initialisation_base_path}/otgenetics-{otg_version}-py3-none-any.whl"
initialisation_executable_file = [
    f"{initialisation_base_path}/install_dependencies_on_cluster.sh"
]


# Input/output file configuration.
version = "XX.XX"
inputs = "gs://genetics_etl_python_playground/input"
outputs = f"gs://genetics_etl_python_playground/output/python_etl/parquet/{version}"
spark_write_mode = "overwrite"


# Common cluster operations.


def generate_create_cluster_task(cluster_name):
    """Generate an Airflow task to create a Dataproc cluster. Common parameters are reused, and varying parameters can be specified as needed."""
    cluster_generator_config = ClusterGenerator(
        project_id=project_id,
        zone=zone,
        master_machine_type="n1-highmem-32",
        worker_machine_type="n1-standard-32",
        master_disk_size=1000,
        worker_disk_size=500,
        num_workers=16,
        num_local_ssds=1,
        image_version=image_version,
        enable_component_gateway=True,
        init_actions_uris=initialisation_executable_file,
        metadata={
            "CONFIGTAR": config_tar,
            "PACKAGE": package_wheel,
        },
        idle_delete_ttl=300,
    ).make()
    return DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=project_id,
        cluster_config=cluster_generator_config,
        region=region,
        cluster_name=cluster_name,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )


def generate_delete_cluster_task(cluster_name):
    """Generate an Airflow task to delete a Dataproc cluster. Common parameters are reused, and varying parameters can be specified as needed."""
    return DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=project_id,
        cluster_name=cluster_name,
        region="europe-west1",
        trigger_rule=TriggerRule.ALL_DONE,
        deferrable=True,
    )


# Partials and common functions for Dataproc operations.


def generate_pyspark_job_params(cluster_name, python_module: str, **kwargs) -> dict:
    """Generates a PySpark Dataproc job object to be passed to the Airflow operator."""
    return {
        # Job ID is random because it's tracked by Airflow/Dataproc internally.
        # Example: airflow-5a4dcc9558b3dc65ed911e8ed58b4f.
        "job_uuid": f"airflow-{binascii.b2a_hex(os.urandom(15)).decode()}",
        "reference": {"project_id": project_id},
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": f"{initialisation_base_path}/preprocess/{python_module}",
            "args": list(map(str, kwargs.values())),
        },
    }


def generate_dataproc_submit_job_operator(task_id, job=None):
    """Generates an Airflow operator for submitting a Dataproc job. If job is not provided, returns a partial instead."""
    kwargs = {"task_id": task_id, "region": region, "project_id": project_id}
    if job:
        return DataprocSubmitJobOperator(**kwargs, job=job)
    else:
        return DataprocSubmitJobOperator.partial(**kwargs)


# Utilities for working with Google Cloud Storage.


def read_parquet_from_path(path):
    """Recursively reads all parquet files from a Google Storage path and combines them into a single Pandas dataframe."""
    all_parquet_files = [
        f"gs://{f}" for f in gcsfs.GCSFileSystem().ls(path) if f.endswith(".parquet")
    ]
    full_df = pd.concat(pd.read_parquet(filename) for filename in all_parquet_files)
    return full_df
