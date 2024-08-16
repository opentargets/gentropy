"""Step to validate study index against disease and target index."""

from __future__ import annotations

from pyspark.sql import functions as f

from gentropy.common.session import Session
from gentropy.dataset.gene_index import GeneIndex
from gentropy.dataset.study_index import StudyIndex


class StudyValidationStep:
    """Study index validation step.

    This step reads and outputs a study index dataset with flagged studies
    when target of disease validation fails.
    """

    def __init__(
        self,
        session: Session,
        study_index_path: list[str],
        target_index_path: str,
        disease_index_path: str,
        output_path: str,
    ) -> None:
        """Initialize step.

        Args:
            session (Session): Session object.
            study_index_path (list[str]): Path to study index file.
            target_index_path (str): Path to target index file.
            disease_index_path (str): Path to disease index file.
            output_path (str): Path to write the output file.
        """
        # Reading datasets:
        target_index = GeneIndex.from_parquet(session, target_index_path)
        # Reading disease index and pre-process.
        # This logic does not belong anywhere, but gentorpy has no disease dataset yet.
        disease_index = (
            session.spark.read.from_parquet(disease_index_path)
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
        study_index = StudyIndex.from_parquet(session, study_index_path)

        # Running validation then writing output:
        (
            study_index.validate_disease(disease_index)
            .validate_unique_study_id()  # Flagging duplicated study ids
            .validate_study_type()  # Flagging non-supported study types.
            .validate_target(target_index)  # Flagging QTL studies with invalid targets
            .validate_disease(disease_index)  # Flagging invalid EFOs
            .df.write.parquet(output_path)
        )
