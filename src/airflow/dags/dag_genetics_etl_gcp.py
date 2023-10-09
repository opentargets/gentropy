"""Generate jinja2 template for workflow."""
from __future__ import annotations

from pathlib import Path

import yaml
from airflow.decorators import task_group
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from common_airflow import (
    create_cluster,
    delete_cluster,
    shared_dag_kwargs,
    submit_pyspark_job,
)

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "configs" / "dag.yaml"
PYTHON_CLI = "cli.py"
CONFIG_NAME = "my_config"
CLUSTER_CONFIG_DIR = "/config"


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics ETL workflow",
    **shared_dag_kwargs,
):
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    assert (
        SOURCE_CONFIG_FILE_PATH.exists()
    ), f"Config path {SOURCE_CONFIG_FILE_PATH} does not exist."
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
                    python_module_path=PYTHON_CLI,
                    args=[
                        f"step={step_id}",
                        f"--config-dir={CLUSTER_CONFIG_DIR}",
                        f"--config-name={CONFIG_NAME}",
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
