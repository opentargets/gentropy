"""Generate jinja2 template for workflow."""
from __future__ import annotations

from pathlib import Path

import yaml
from airflow.models.dag import DAG
from common_airflow import (
    create_cluster,
    delete_cluster,
    shared_dag_args,
    shared_dag_kwargs,
    submit_pyspark_job,
)

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "configs" / "dag.yaml"
PYTHON_CLI = "cli.py"
CONFIG_NAME = "my_config"
CLUSTER_CONFIG_DIR = "/config"
CLUSTER_NAME = "workflow-otg-cluster"


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics ETL workflow",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
):
    assert (
        SOURCE_CONFIG_FILE_PATH.exists()
    ), f"Config path {SOURCE_CONFIG_FILE_PATH} does not exist."
    with open(SOURCE_CONFIG_FILE_PATH, "r") as config_file:
        # Parse and define all steps and their prerequisites.
        tasks = {}
        steps = yaml.safe_load(config_file)
        for step in steps:
            # Define task for the current step.
            step_id = step["id"]
            this_task = submit_pyspark_job(
                cluster_name=CLUSTER_NAME,
                task_id=f"job-{step_id}",
                python_module_path=PYTHON_CLI,
                args=[
                    f"step={step_id}",
                    f"--config-dir={CLUSTER_CONFIG_DIR}",
                    f"--config-name={CONFIG_NAME}",
                ],
            )
            # Chain prerequisites.
            tasks[step_id] = this_task
            for prerequisite in step.get("prerequisites", []):
                this_task.set_upstream(tasks[prerequisite])

        # Construct the DAG with all tasks.
        (
            create_cluster(CLUSTER_NAME)
            >> list(tasks.values())
            >> delete_cluster(CLUSTER_NAME)
        )
