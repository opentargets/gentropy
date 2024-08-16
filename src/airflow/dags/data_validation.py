"""DAG to validate study locus and study index datasets."""

from __future__ import annotations

from pathlib import Path

import common_airflow as common
from airflow.models.dag import DAG

CLUSTER_NAME = "otg-validation"

# Input datasets:
STUDY_INDICES = [
    "gs://gwas_catalog_data/study_index",
    "gs://eqtl_catalogue_data/study_index",
    "gs://finngen_data/r10/study_index",
]
STUDY_LOCI = [
    "gs://gwas_catalog_data/credible_set_datasets/gwas_catalog_PICSed_curated_associations",
    "gs://gwas_catalog_data/credible_set_datasets/gwas_catalog_PICSed_summary_statistics",
    "gs://eqtl_catalogue_data/credible_set_datasets/susie",
    "gs://finngen_data/r10/credible_set_datasets/finngen_susie_processed",
]
TARGET_INDEX = "gs://genetics_etl_python_playground/releases/24.06/gene_index"
DISEASE_INDEX = "gs://open-targets-pre-data-releases/24.06/output/etl/parquet/diseases"

# Output datasets:
VALIDATED_STUDY = "gs://ot-team/dsuveges/otg-data/validated_study_index"
VALIDATED_STUDY_LOCI = "gs://ot-team/dsuveges/otg-data/validated_credible_set"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Study locus and study index validation",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
) as dag:
    # Definition of the study index validation step:
    validate_studies = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="study_validation",
        task_id="study_validation",
        other_args=[
            f"step.study_index_path={STUDY_INDICES}",
            f"step.target_index_path={TARGET_INDEX}",
            f"step.disease_index_path={DISEASE_INDEX}",
            f"step.output_path={VALIDATED_STUDY}",
        ],
    )

    # Definition of the study locus validation step:
    validate_study_loci = common.submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="credible_set_validation",
        task_id="credible_set_validation",
        other_args=[
            f"step.study_index_path={VALIDATED_STUDY}",
            f"step.study_locus_path={STUDY_LOCI}",
            f"step.output_path={VALIDATED_STUDY_LOCI}",
        ],
    )

    (
        common.create_cluster(
            CLUSTER_NAME,
            master_machine_type="n1-highmem-32",
        )
        >> common.install_dependencies(CLUSTER_NAME)
        >> validate_studies
        >> validate_study_loci
        # >> common.delete_cluster(CLUSTER_NAME)
    )
