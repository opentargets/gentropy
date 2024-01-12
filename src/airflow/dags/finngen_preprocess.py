"""Airflow DAG for the Preprocess part of the pipeline."""
from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG
from airflow.utils.trigger_rule import TriggerRule

CLUSTER_NAME = "otg-preprocess-finngen"
AUTOSCALING = "finngen-preprocess"

RELEASEBUCKET = "gs://genetics_etl_python_playground/output/python_etl/parquet/XX.XX"
SUMSTATS = f"{RELEASEBUCKET}/summary_statistics/finngen"
WINDOWBASED_CLUMPED = (
    f"{RELEASEBUCKET}/study_locus/from_sumstats_study_locus_window_clumped/finngen"
)
LD_CLUMPED = f"{RELEASEBUCKET}/study_locus/from_sumstats_study_locus_ld_clumped/finngen"
PICSED = f"{RELEASEBUCKET}/credible_set/from_sumstats/finngen"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Finngen preprocess",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    study_index = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="ot_finngen_studies",
        task_id="finngen_studies",
    )

    window_based_clumping = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="window_based_clumping",
        task_id="finngen_window_based_clumping",
        other_args=[
            f"step.summary_statistics_input_path={SUMSTATS}",
            f"step.study_locus_output_path={WINDOWBASED_CLUMPED}",
        ],
    )
    ld_clumping = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="ld_based_clumping",
        task_id="finngen_ld_clumping",
        other_args=[
            f"step.study_locus_input_path={WINDOWBASED_CLUMPED}",
            f"step.ld_index_path={RELEASEBUCKET}/ld_index",
            f"step.study_index_path={RELEASEBUCKET}/study_index/finngen",
            f"step.clumped_study_locus_output_path={LD_CLUMPED}",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    pics = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="ot_pics",
        task_id="finngen_pics",
        other_args=[
            f"step.study_locus_ld_annotated_in={LD_CLUMPED}",
            f"step.picsed_study_locus_out={PICSED}",
        ],
        # This allows to attempt running the task when above step fails do to failifexists
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        common.create_cluster(
            CLUSTER_NAME,
            autoscaling_policy=AUTOSCALING,
            master_disk_size=2000,
            num_workers=6,
        )
        >> common.install_dependencies(CLUSTER_NAME)
        >> study_index
        >> window_based_clumping
        >> ld_clumping
        >> pics
        >> common.delete_cluster(CLUSTER_NAME)
    )
