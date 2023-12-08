"""GWAS Catalog study locus dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql.window import Window

from otg.dataset.study_locus import StudyLocus, StudyLocusQualityCheck

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class StudyLocusGWASCatalog(StudyLocus):
    """Study locus dataset for GWAS Catalog associations.

    A study index dataset captures all the metadata for all studies including GWAS and Molecular QTL.
    """

    def update_study_id(
        self: StudyLocusGWASCatalog, study_annotation: DataFrame
    ) -> StudyLocusGWASCatalog:
        """Update final studyId and studyLocusId with a dataframe containing study annotation.

        Args:
            study_annotation (DataFrame): Dataframe containing `updatedStudyId` and key columns `studyId` and `subStudyDescription`.

        Returns:
            StudyLocusGWASCatalog: Updated study locus with new `studyId` and `studyLocusId`.
        """
        self.df = (
            self._df.join(
                study_annotation, on=["studyId", "subStudyDescription"], how="left"
            )
            .withColumn("studyId", f.coalesce("updatedStudyId", "studyId"))
            .drop("subStudyDescription", "updatedStudyId")
        ).withColumn(
            "studyLocusId",
            StudyLocus.assign_study_locus_id(f.col("studyId"), f.col("variantId")),
        )
        return self

    def qc_ambiguous_study(self: StudyLocusGWASCatalog) -> StudyLocusGWASCatalog:
        """Flag associations with variants that can not be unambiguously associated with one study.

        Returns:
            StudyLocusGWASCatalog: Updated study locus.
        """
        assoc_ambiguity_window = Window.partitionBy(
            f.col("studyId"), f.col("variantId")
        )

        self._df.withColumn(
            "qualityControls",
            StudyLocus.update_quality_flag(
                f.col("qualityControls"),
                f.count(f.col("variantId")).over(assoc_ambiguity_window) > 1,
                StudyLocusQualityCheck.AMBIGUOUS_STUDY,
            ),
        )
        return self
