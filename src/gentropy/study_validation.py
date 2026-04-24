"""Step to validate study index against disease and target index."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field
from pyspark.sql import functions as f

from gentropy.common.session import Session
from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.target_index import TargetIndex


class StudyValidationDefaults(BaseModel, frozen=True):
    """Defaults for StudyValidationStep.

    All values are frozen - create a new instance to override.
    """

    study_index_path: Annotated[
        list[str], Field(description="Path to study index file.")
    ]
    target_index_path: Annotated[str, Field(description="Path to target index file.")]
    disease_index_path: Annotated[str, Field(description="Path to disease index file.")]
    biosample_index_path: Annotated[
        str, Field(description="Path to biosample index file.")
    ]
    valid_study_index_path: Annotated[
        str, Field(description="Path to write the valid records.")
    ]
    invalid_study_index_path: Annotated[
        str, Field(description="Path to write the output file.")
    ]
    invalid_qc_reasons: Annotated[
        list[str] | None,
        Field(
            description="List of invalid quality check reason names from `StudyQualityCheck`."
        ),
    ] = []
    deprecated_project_ids: Annotated[
        list[str] | None,
        Field(description="List of deprecated projectIds (e.g. ['GTEx'])."),
    ] = None


class StudyValidationStep:
    """Study index validation step.

    This step reads and outputs a study index dataset with flagged studies
    when target of disease validation fails.
    """

    def __init__(
        self,
        config: StudyValidationDefaults,
        session: Session,
    ) -> None:
        """Initialize step.

        Args:
            config: Step configuration defaults.
            session: Session object.
        """
        invalid_qc_reasons = (
            list(config.invalid_qc_reasons) if config.invalid_qc_reasons else []
        )
        deprecated_project_ids = (
            list(config.deprecated_project_ids) if config.deprecated_project_ids else []
        )

        # Reading datasets:
        target_index = TargetIndex.from_parquet(session, config.target_index_path)
        biosample_index = BiosampleIndex.from_parquet(
            session, config.biosample_index_path
        )
        # Reading disease index and pre-process.
        # This logic does not belong anywhere, but gentropy has no disease dataset yet.
        disease_index = (
            session.spark.read.parquet(config.disease_index_path)
            .select(
                f.col("id").alias("diseaseId"),
                f.explode_outer(
                    f.when(
                        f.col("obsoleteTerms").isNotNull(),
                        f.array_union(f.array("id"), f.col("obsoleteTerms")),
                    )
                ).alias("efo"),
            )
            .withColumn("efo", f.coalesce(f.col("efo"), f.col("diseaseId")))
        )
        study_index = StudyIndex.from_parquet(session, list(config.study_index_path))

        # Running validation:
        study_index_with_qc = (
            study_index.deconvolute_studies()  # Deconvolute studies where the same study is ingested from multiple sources
            .validate_study_type()  # Flagging non-supported study types
            .validate_project_id(deprecated_project_ids)  # Flag obsolete projectIds
            .validate_target(target_index)  # Flagging QTL studies with invalid targets
            .validate_disease(disease_index)  # Flagging invalid EFOs
            .validate_biosample(biosample_index)  # Flagging invalid biosample in QTLs
            .validate_analysis_flags()  # Flagging studies with case case design
            .persist()  # we will need this for 2 types of outputs
        )

        result = study_index_with_qc.valid_rows(invalid_qc_reasons)
        (
            result.valid.df.coalesce(session.output_partitions)
            .write.mode(session.write_mode)
            .parquet(config.valid_study_index_path)
        )
        (
            result.invalid.df.coalesce(session.output_partitions)
            .write.mode(session.write_mode)
            .parquet(config.invalid_study_index_path)
        )
