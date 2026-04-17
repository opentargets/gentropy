"""Study locus overlap index dataset."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, NamedTuple

import pyspark.sql.functions as f
from pyspark.sql import Column

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.study_locus import StudyLocus



class UnsupportedOverlapTypeError(ValueError):
    """Unsupported overlap type error.

    Raised when an unsupported overlap type is provided to the `expression` method of the `OverlapType` enum.
    """


class OverlapType(StrEnum):
    """Overlap type enum.

    Attributes:
        GWAS_VS_GWAS (str): Overlap between two GWAS studies
        GWAS_VS_QTL (str): Overlap between a GWAS and a molecular QTL study
        QTL_VS_QTL (str): Overlap between two molecular QTL studies
        GWAS_VS_ALL (str): Overlap between a GWAS and any other study (GWAS or molecular QTL). For this type of overlap, the `StudyLocusOverlap` dataset will contain all the overlapping signals between studyLocus associations once they have been clumped and fine-mapped, including both GWAS vs GWAS and GWAS vs molecular QTL overlaps. The `expression` method of this enum will return the expressions to find both types of overlaps.
    """

    GWAS_VS_GWAS = "gwas_vs_gwas"
    GWAS_VS_QTL = "gwas_vs_qtl"
    QTL_VS_QTL = "qtl_vs_qtl"
    GWAS_VS_ALL = "gwas_vs_all"

    @classmethod
    def expression(cls, label: str) -> OverlapExpression:
        """Get the expressions to find overlaps of a given type.

        Args:
            label (str): Overlap type label. It needs to be one of the values in the `OverlapType` enum.

        Returns:
            OverlapExpression: Expressions to find overlaps of the given type.

        Raises:
            UnsupportedOverlapTypeError: If the provided label is not a valid overlap type.
        """
        match label:
            case cls.GWAS_VS_GWAS.value:
                return OverlapExpression(
                    expressions=[
                        f.col("left.chromosome") == f.col("right.chromosome"),
                        f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                        f.col("left.studyType") == f.lit("gwas"),
                        f.col("right.studyType") == f.lit("gwas"),
                        f.col("left.studyLocusId") > f.col("right.studyLocusId"),
                    ]
                )
            case cls.GWAS_VS_QTL.value:
                return OverlapExpression(
                    expressions=[
                        f.col("left.chromosome") == f.col("right.chromosome"),
                        f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                        f.col("left.studyType") == f.lit("gwas"),
                        f.col("right.studyType") != f.lit("gwas"),
                    ]
                )
            case cls.QTL_VS_QTL.value:
                return OverlapExpression(
                    expressions=[
                        f.col("left.chromosome") == f.col("right.chromosome"),
                        f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                        f.col("left.studyType") != f.lit("gwas"),
                        f.col("right.studyType") != f.lit("gwas"),
                        f.col("left.studyLocusId") > f.col("right.studyLocusId"),
                    ]
                )

            case cls.GWAS_VS_ALL.value:
                return OverlapExpression(
                    expressions=[
                        f.col("left.chromosome") == f.col("right.chromosome"),
                        f.col("left.tagVariantId") == f.col("right.tagVariantId"),
                        f.col("left.studyType") == f.lit("gwas"),
                        (f.col("right.studyType") != "gwas")
                        | (f.col("left.studyLocusId") > f.col("right.studyLocusId")),
                    ]
                )
            case _:
                raise UnsupportedOverlapTypeError(f"Unsupported overlap type: {label}")


class OverlapExpression(NamedTuple):
    """Overlap expression model.

    This model captures the expressions to define the type of overlaps to find in the `find_overlaps` method of the `StudyLocus` dataset.

    Attributes:
        expressions (list[Column]): Expressions to define the overlap. Each expression needs to be a valid Spark SQL expression that can be evaluated in the context of the `StudyLocus` dataset.
    """

    expressions: list[Column]


@dataclass
class StudyLocusOverlap(Dataset):
    """Study-Locus overlap.

    This dataset captures pairs of overlapping `StudyLocus`: that is associations whose credible sets share at least one tagging variant.

    !!! note

        This is a helpful dataset for other downstream analyses, such as colocalisation. This dataset will contain the overlapping signals between studyLocus associations once they have been clumped and fine-mapped.
    """

    @classmethod
    def get_schema(cls: type[StudyLocusOverlap]) -> StructType:
        """Provides the schema for the StudyLocusOverlap dataset.

        Returns:
            StructType: Schema for the StudyLocusOverlap dataset
        """
        return parse_spark_schema("study_locus_overlap.json")

    @classmethod
    def from_associations(
        cls: type[StudyLocusOverlap], study_locus: StudyLocus
    ) -> StudyLocusOverlap:
        """Find the overlapping signals in a particular set of associations (StudyLocus dataset).

        Args:
            study_locus (StudyLocus): Study-locus associations to find the overlapping signals

        Returns:
            StudyLocusOverlap: Study-locus overlap dataset
        """
        return study_locus.find_overlaps(OverlapType.GWAS_VS_ALL)


    def calculate_beta_ratio(self: StudyLocusOverlap) -> DataFrame:
        """Calculate the beta ratio for the overlapping signals.

        Returns:
            DataFrame: A dataframe containing left and right loci IDs, chromosome
            and the average sign of the beta ratio
        """
        return (
            # Unpack statistics column:
            self.df.select("*", "statistics.*")
            .drop("statistics")
            # Drop any rows where the beta is null or zero
            .filter(
                f.col("left_beta").isNotNull() &
                f.col("right_beta").isNotNull() &
                (f.col("left_beta") != 0) &
                (f.col("right_beta") != 0)
            )
            # Calculate the beta ratio and get the sign, then calculate the average sign across all variants in the locus
            .withColumn(
                "betaRatioSign",
                f.signum(f.col("left_beta") / f.col("right_beta"))
            )
            # Aggregate beta signs:
            .groupBy("leftStudyLocusId","rightStudyLocusId","chromosome")
            .agg(
                f.avg("betaRatioSign").alias("betaRatioSignAverage")
            )
        )

    def _convert_to_square_matrix(self: StudyLocusOverlap) -> StudyLocusOverlap:
        """Convert the dataset to a square matrix.

        Returns:
            StudyLocusOverlap: Square matrix of the dataset
        """
        return StudyLocusOverlap(
            _df=self.df.unionByName(
                self.df.selectExpr(
                    "leftStudyLocusId as rightStudyLocusId",
                    "rightStudyLocusId as leftStudyLocusId",
                    "rightStudyType",
                    "tagVariantId",
                    "chromosome",
                    "statistics",
                )
            ).distinct(),
            _schema=self.get_schema(),
        )
