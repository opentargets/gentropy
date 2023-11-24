"""Airflow DAG for the Preprocess part of the pipeline."""
from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.utils.trigger_rule import TriggerRule

CLUSTER_NAME = "otg-preprocess-finngen"
AUTOSCALING = "finngen-preprocess"

RELEASEBUCKET = "gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Finngen preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    study_and_sumstats = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="finngen",
        task_id="finngen_sumstats_and_study_index",
    )

    clumping = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="clump",
        task_id="finngen_clump",
        other_args=[
            f"step.summary_stats_path={RELEASEBUCKET}/summary_statistics/finngen",
            f"step.clumped_study_locus_out={RELEASEBUCKET}/study_locus/from_sumstats_study_locus/finngen",
        ],
        # This allows to attempt running the task when above step fails do to failifexists
        trigger_rule=TriggerRule.ALL_DONE,
    )

    pics = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="pics",
        task_id="finngen_pics",
        other_args=[
            f"step.study_locus_ld_annotated_in={RELEASEBUCKET}/study_locus/from_sumstats_study_locus/finngen",
            f"step.picsed_study_locus_out={RELEASEBUCKET}/credible_set/from_sumstats_study_locus/finngen",
        ],
        # This allows to attempt running the task when above step fails do to failifexists
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        common.create_cluster(CLUSTER_NAME, autoscaling_policy=AUTOSCALING)
        >> common.install_dependencies(CLUSTER_NAME)
        >> study_and_sumstats
        >> clumping
        >> pics
        >> common.delete_cluster(CLUSTER_NAME)
    )
