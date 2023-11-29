"""Airflow DAG for the ETL part of the pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Union

import common_airflow as common
from airflow.models import BaseOperator
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

CLUSTER_NAME = "otg-etl-il"
SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "configs" / "dag.yaml"
RELEASEBUCKET = "gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics ETL workflow",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    # Parse and define all steps and their prerequisites.
    tasks: Dict[str, Union[BaseOperator, TaskGroup]] = {}
    nodes = common.read_yaml_config(SOURCE_CONFIG_FILE_PATH)

    for node in nodes:
        node_id = node["id"]
        if "tasks" in node:
            # Group of tasks for nodes with multiple tasks
            with TaskGroup(node_id) as tgroup:
                prev_task = None
                for task in node["tasks"]:
                    parsed_args = [
                        f"step.{key}={value}"
                        for key, value in task.get("args", {}).items()
                    ]
                    current_task = common.submit_step(
                        cluster_name=CLUSTER_NAME,
                        step_id=task["id"],
                        other_args=parsed_args,
                    )
                    if prev_task:
                        prev_task >> current_task
                    prev_task = current_task

            tasks[node_id] = tgroup
        else:
            # Single task step
            tasks[node_id] = common.submit_step(
                cluster_name=CLUSTER_NAME,
                step_id=node_id,
            )

    # Chain prerequisites.
    for prerequisite in node.get("prerequisites", []):
        if prerequisite in tasks:
            # Set upstream for both TaskGroup and BaseOperator
            tasks[node_id].set_upstream(tasks[prerequisite])

    # Construct the DAG with all tasks.
    common.generate_dag(cluster_name=CLUSTER_NAME, tasks=list(tasks.values()))
