"""Airflow DAG for the Preprocess part of the pipeline."""
from __future__ import annotations

from pathlib import Path

from airflow.models.dag import DAG

from . import common_airflow as common

CLUSTER_NAME = "workflow-otg-cluster"

ALL_STEPS = [
    "finngen",
]


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    (
        common.create_cluster(CLUSTER_NAME)
        >> common.install_dependencies(CLUSTER_NAME)
        >> [
            common.submit_step(cluster_name=CLUSTER_NAME, step_id=step)
            for step in ALL_STEPS
        ]
        >> common.delete_cluster(CLUSTER_NAME)
    )
