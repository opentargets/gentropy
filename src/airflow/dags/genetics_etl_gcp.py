"""Generate jinja2 template for workflow."""
from __future__ import annotations

from pathlib import Path

import pendulum
import yaml
from airflow.decorators import dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow_common import create_cluster, delete_cluster, submit_pyspark_job

DAG_ID = "etl_using_external_flat_file"

DAG_DIR = Path(__file__).parent
CONFIG_DIR = "configs"

SOURCES_FILE_NAME = "dag.yaml"
SOURCE_CONFIG_FILE_PATH = DAG_DIR / CONFIG_DIR / SOURCES_FILE_NAME

# Managed cluster
project_id = "open-targets-genetics-dev"
otg_version = "0.1.4"
initialisation_base_path = (
    f"gs://genetics_etl_python_playground/initialisation/{otg_version}"
)
python_cli = "cli.py"
config_name = "my_config"
config_tar = f"{initialisation_base_path}/config.tar.gz"
package_wheel = f"{initialisation_base_path}/otgenetics-{otg_version}-py3-none-any.whl"
initialisation_executable_file = [f"{initialisation_base_path}/initialise_cluster.sh"]
image_version = "2.1"
num_local_ssds = 1
# job
cluster_config_dir = "/config"

default_args = {
    "owner": "Open Targets Data Team",
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": pendulum.now(tz="Europe/London").subtract(days=1),
    # "start_date": pendulum.datetime(2020, 1, 1, tz="Europe/London"),
    "schedule_interval": "@once",
    "project_id": project_id,
    "catchup": False,
    "retries": 3,
}


@dag(
    dag_id=Path(__file__).stem,
    default_args=default_args,
    description="Open Targets Genetics ETL workflow",
    tags=["genetics_etl", "experimental"],
)
def create_dag() -> None:
    """Submit dataproc workflow."""
    assert (
        SOURCE_CONFIG_FILE_PATH.exists()
    ), f"Config path {SOURCE_CONFIG_FILE_PATH} does not exist."

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    with open(SOURCE_CONFIG_FILE_PATH, "r") as config_file:
        tasks_groups = {}
        steps = yaml.safe_load(config_file)
        for step in steps:
            step_id = step["id"]

            @task_group(
                group_id=step_id,
                prefix_group_id=True,
            )
            def tgroup(step_id: str) -> None:
                """Self-sufficient task group for one task."""
                cluster_name = f"workflow-otg-cluster-{step_id.replace('_', '-')}"
                step_pyspark_job = submit_pyspark_job(
                    cluster_name=cluster_name,
                    task_id=f"job-{step_id}",
                    python_module_path=python_cli,
                    args=[
                        f"step={step_id}",
                        f"--config-dir={cluster_config_dir}",
                        f"--config-name={config_name}",
                    ],
                )
                # Chain the steps within the task group.
                (
                    create_cluster(cluster_name)
                    >> step_pyspark_job
                    >> delete_cluster(cluster_name)
                )

            thisgroup = tgroup(step_id)
            tasks_groups[step_id] = thisgroup
            if "prerequisites" in step:
                for prerequisite in step["prerequisites"]:
                    thisgroup.set_upstream(tasks_groups[prerequisite])

            # Add task group to the DAG.
            start >> thisgroup >> end


dag = create_dag()
