"""Generate jinja2 template for workflow."""
from __future__ import annotations

from pathlib import Path

import yaml
from airflow.models.dag import DAG

from . import common_airflow as common

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "configs" / "dag.yaml"
CLUSTER_NAME = "otg-etl"


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics ETL workflow",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
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
            this_task = common.submit_step(
                cluster_name=CLUSTER_NAME,
                step_id=step_id,
            )
            # Chain prerequisites.
            tasks[step_id] = this_task
            for prerequisite in step.get("prerequisites", []):
                this_task.set_upstream(tasks[prerequisite])
        # Construct the DAG with all tasks.
        dag = common.generate_dag(cluster_name=CLUSTER_NAME, tasks=list(tasks.values()))
