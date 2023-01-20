"""Variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Type

import pyspark.sql.functions as f

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset
from otg.dataset.study_locus_overlap import StudyLocusOverlap

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.common.session import ETLSession


class CredibleInterval(Enum):
    """Credible interval enum.

    Interval within which an unobserved parameter value falls with a particular probability.

    Attributes:
        IS95 (str): 95% credible interval
        IS99 (str): 99% credible interval
    """

    IS95 = "is95CredibleSet"
    IS99 = "is99CredibleSet"


@dataclass
class StudyLocus(Dataset):
    """Study-Locus dataset.

    This dataset captures associations between study/traits and a genetic loci as provided by finemapping methods.
    """

    schema: StructType = parse_spark_schema("study_locus.json")

    @staticmethod
    def _overlapping_peaks(credset_to_overlap: DataFrame) -> DataFrame:
        """Calculate overlapping signals (study-locus) between GWAS-GWAS and GWAS-Molecular trait.

        Args:
            credset_to_overlap (DataFrame): DataFrame containing at least `studyLocusId`, `studyType`, `chromosome` and `tagVariantId` columns.

        Returns:
            DataFrame: containing `left_studyLocusId`, `right_studyLocusId` and `chromosome` columns.
        """
        # Reduce columns to the minimum to reduce the size of the dataframe
        credset_to_overlap = credset_to_overlap.select(
            "studyLocusId", "studyType", "chromosome", "tagVariantId"
        )
        return (
            credset_to_overlap.alias("left")
            .filter(f.col("studyType") == "gwas")
            # Self join with complex condition. Left it's all gwas and right can be gwas or molecular trait
            .join(
                credset_to_overlap.alias("right"),
                on=[
                    f.col("left.chromosome") == f.col("right.chromosome"),
                    f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                    (f.col("right.studyType") != "gwas")
                    | (f.col("left.studyLocusId") > f.col("right.studyLocusId")),
                ],
                how="inner",
            )
            .select(
                f.col("left.studyLoucusId").alias("left_studyLocusId"),
                f.col("right.studyLoucusId").alias("right_studyLocusId"),
                f.col("left.chromosome").alias("chromosome"),
            )
            .distinct()
            .repartition("chromosome")
            .persist()
        )

    @staticmethod
    def _align_overlapping_tags(
        credset_to_overlap: DataFrame, peak_overlaps: DataFrame
    ) -> StudyLocusOverlap:
        """Align overlapping tags in pairs of overlapping study-locus, keeping all tags in both loci.

        Args:
            credset_to_overlap (DataFrame): containing `studyLocusId`, `studyType`, `chromosome`, `tagVariantId`, `logABF` and `posteriorProbability` columns.
            peak_overlaps (DataFrame): containing `left_studyLocusId`, `right_studyLocusId` and `chromosome` columns.

        Returns:
            StudyLocusOverlap: Pairs of overlapping study-locus with aligned tags.
        """
        # Complete information about all tags in the left study-locus of the overlap
        overlapping_left = credset_to_overlap.select(
            f.col("chromosome"),
            f.col("tagVariantId"),
            f.col("studyLocusId").alias("left_studyLocusId"),
            f.col("logABF").alias("left_logABF"),
            f.col("posteriorProbability").alias("left_posteriorProbability"),
        ).join(peak_overlaps, on=["chromosome", "left_studyLocusId"], how="inner")

        # Complete information about all tags in the right study-locus of the overlap
        overlapping_right = credset_to_overlap.select(
            f.col("chromosome"),
            f.col("tagVariantId"),
            f.col("studyLocusId").alias("right_studyLocusId"),
            f.col("logABF").alias("right_logABF"),
            f.col("posteriorProbability").alias("right_posteriorProbability"),
        ).join(peak_overlaps, on=["chromosome", "right_studyLocusId"], how="inner")

        # Include information about all tag variants in both study-locus aligned by tag variant id
        return StudyLocusOverlap(
            _df=overlapping_left.join(
                overlapping_right,
                on=[
                    "chromosome",
                    "right_studyLocusId",
                    "left_studyLocusId",
                    "tagVariantId",
                ],
                how="outer",
            )
        )

    @classmethod
    def from_parquet(cls: Type[StudyLocus], etl: ETLSession, path: str) -> StudyLocus:
        """Initialise StudyLocus from parquet file.

        Args:
            etl (ETLSession): ETL session
            path (str): Path to parquet file

        Returns:
            StudyLocus: Study-locus dataset
        """
        return super().from_parquet(etl, path, cls.schema)

    def credible_set(
        self: StudyLocus,
        credible_interval: CredibleInterval,
    ) -> StudyLocus:
        """Filter study-locus dataset based on credible interval.

        Args:
            credible_interval (CredibleInterval): Credible interval to filter for.

        Returns:
            StudyLocus: Filtered study-locus dataset.
        """
        self.df.filter(f"credibleSet, tag -> (tag.{credible_interval})")
        return self

    def overlaps(self: StudyLocus) -> StudyLocusOverlap:
        """Calculate overlapping study-locus.

        Find overlapping study-locus that share at least one tagging variant. All GWAS-GWAS and all GWAS-Molecular traits are computed with the Molecular traits always
        appearing on the right side.

        Returns:
            StudyLocusOverlap: Pairs of overlapping study-locus with aligned tags.
        """
        credset_to_overlap = (
            self.df.withColumn("credibleSet", f.explode("credibleSet"))
            .select(
                "studyLocusId",
                "studyType",
                "chromosome",
                f.explode("credibleSet.tagVariantId").alias("tagVariantId"),
                f.explode("credibleSet.logABF").alias("logABF"),
                f.explode("credibleSet.posteriorProbability").alias(
                    "posteriorProbability"
                ),
            )
            .persist()
        )

        # overlapping study-locus
        peak_overlaps = self._overlapping_peaks(credset_to_overlap)

        # study-locus overlap by aligning overlapping variants
        return self._align_overlapping_tags(credset_to_overlap, peak_overlaps)

    def unique_variants(self: StudyLocus) -> DataFrame:
        """All unique lead and tag variants contained in the `StudyLocus` dataframe.

        Returns:
            DataFrame: A dataframe containing `variantId` and `chromosome` columns.
        """
        lead_tags = (
            self.df.select(
                f.col("variantId"),
                f.col("chromosome"),
                f.explode("credibleSet.tagVariantId").alias("tagVariantId"),
            )
            .repartition("chromosome")
            .persist()
        )
        return (
            lead_tags.select("variantId", "chromosome")
            .union(
                lead_tags.select(f.col("tagVariantId").alias("variantId"), "chromosome")
            )
            .distinct()
        )
