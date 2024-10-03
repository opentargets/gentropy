"""Study locus overlap index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.study_locus import StudyLocus


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
        return study_locus.find_overlaps()


    def calculate_beta_ratio(self: StudyLocusOverlap) -> DataFrame:
        """Calculate the beta ratio for the overlapping signals.

        Returns:
            DataFrame: A dataframe containing left and right loci IDs, chromosome
            and the average sign of the beta ratio
        """
        expanded_overlaps = self.df.select("*", "statistics.*").drop("statistics")

        # Drop any rows where the beta is null
        both_betas_not_null_overlaps = (expanded_overlaps
            .filter(f.col("right_beta").isNotNull())
            .filter(f.col("left_beta").isNotNull()))

        # Calculate the beta ratio and get the sign, then calculate the average sign across all variants in the locus
        beta_ratio_sign = (both_betas_not_null_overlaps
            .withColumn("beta_ratio_sign",
                        f.signum(f.col("left_beta") / f.col("right_beta")))
            .groupBy("leftStudyLocusId",
                    "rightStudyLocusId",
                    "chromosome")
            .agg(f.avg("beta_ratio_sign").alias("beta_ratio_sign_avg")))

        # Remove any rows where the average sign is not 1 or -1
        return beta_ratio_sign.filter(f.abs(f.col("beta_ratio_sign_avg") != 1))


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
                )
            ).distinct(),
            _schema=self.get_schema(),
        )
