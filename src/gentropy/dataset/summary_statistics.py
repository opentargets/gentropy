"""Summary satistics dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Window

from gentropy.common.genomic_region import GenomicRegion
from gentropy.common.schemas import parse_spark_schema
from gentropy.common.stats import pvalue_at_most
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class SummaryStatistics(Dataset):
    """Summary Statistics dataset.

    A summary statistics dataset contains all single point statistics resulting from a GWAS.
    """

    @classmethod
    def get_schema(cls: type[SummaryStatistics]) -> StructType:
        """Provides the schema for the SummaryStatistics dataset.

        Returns:
            StructType: Schema for the SummaryStatistics dataset
        """
        return parse_spark_schema("summary_statistics.json")

    def pvalue_filter(self: SummaryStatistics, pvalue: float) -> SummaryStatistics:
        """Filter summary statistics based on the provided p-value threshold.

        Args:
            pvalue (float): upper limit of the p-value to be filtered upon.

        Returns:
            SummaryStatistics: summary statistics object containing single point associations with p-values at least as significant as the provided threshold.
        """
        df = self._df.filter(
            pvalue_at_most(f.col("pValueMantissa"), f.col("pValueExponent"), pvalue)
        )
        return SummaryStatistics(_df=df, _schema=self._schema)

    def exclude_region(
        self: SummaryStatistics, region: GenomicRegion
    ) -> SummaryStatistics:
        """Exclude a region from the summary stats dataset.

        Args:
            region (GenomicRegion): Genomic region to be excluded.

        Returns:
            SummaryStatistics: filtered summary statistics.
        """
        return SummaryStatistics(
            _df=(
                self.df.filter(
                    ~(
                        (f.col("chromosome") == region.chromosome)
                        & (
                            (f.col("position") >= region.start)
                            & (f.col("position") <= region.end)
                        )
                    )
                )
            ),
            _schema=SummaryStatistics.get_schema(),
        )

    def sanity_filter(self: SummaryStatistics) -> SummaryStatistics:
        """The function filters the summary statistics by sanity filters.

        The function filters the summary statistics by the following filters:
            - The p-value should be less than 1.
            - The pValueMantissa should be greater than 0.
            - The beta should not be equal 0.
            - The p-value, beta and se should not be NaN.
            - The se should be positive.
            - The beta and se should not be infinite.
            - Variants cannot be duplicated across single studyId.

        Returns:
            SummaryStatistics: The filtered summary statistics.
        """
        gwas_df = self._df
        gwas_df = gwas_df.dropna(
            subset=["beta", "standardError", "pValueMantissa", "pValueExponent"]
        )
        gwas_df = gwas_df.filter((f.col("beta") != 0) & (f.col("standardError") > 0))
        gwas_df = gwas_df.filter(
            (f.col("pValueMantissa") * 10 ** f.col("pValueExponent") < 1)
            & (f.col("pValueMantissa") > 0)
        )
        cols = ["beta", "standardError"]
        summary_stats = (
            SummaryStatistics(_df=gwas_df)
            .drop_infinity_values(*cols)
            .drop_variant_duplicates()
        )

        return summary_stats

    def drop_variant_duplicates(self) -> SummaryStatistics:
        """Drop duplicate variants in the summary statistics dataset.

        A (studyId, variantId) pair is kept only if it appears exactly once; if it appears multiple times, all occurrences are removed.

        Returns:
            SummaryStatistics: Summary statistics dataset with rows duplicated by (variantId, studyId) dropped.
        """
        # Counts occurrences in a single window pass, and reuses the input's
        # partitioning when it is already clustered by studyId.
        return SummaryStatistics(
            _df=self.df.withColumn(
                "occurrences",
                f.count(f.lit(1)).over(Window.partitionBy("studyId", "variantId")),
            )
            .filter(f.col("occurrences") == 1)
            .drop("occurrences")
        )
