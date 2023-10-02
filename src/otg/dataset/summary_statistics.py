"""Summary satistics dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from otg.common.schemas import parse_spark_schema
from otg.common.utils import parse_region, split_pvalue
from otg.dataset.dataset import Dataset
from otg.method.window_based_clumping import WindowBasedClumping

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from otg.dataset.study_locus import StudyLocus


@dataclass
class SummaryStatistics(Dataset):
    """Summary Statistics dataset.

    A summary statistics dataset contains all single point statistics resulting from a GWAS.
    """

    @classmethod
    def get_schema(cls: type[SummaryStatistics]) -> StructType:
        """Provides the schema for the SummaryStatistics dataset."""
        return parse_spark_schema("summary_statistics.json")

    def pvalue_filter(self: SummaryStatistics, pvalue: float) -> SummaryStatistics:
        """Filter summary statistics based on the provided p-value threshold.

        Args:
            pvalue (float): upper limit of the p-value to be filtered upon.

        Returns:
            SummaryStatistics: summary statistics object containing single point associations with p-values at least as significant as the provided threshold.
        """
        # Converting p-value to mantissa and exponent:
        (mantissa, exponent) = split_pvalue(pvalue)

        # Applying filter:
        df = self._df.filter(
            (f.col("pValueExponent") < exponent)
            | (
                (f.col("pValueExponent") == exponent)
                & (f.col("pValueMantissa") <= mantissa)
            )
        )
        return SummaryStatistics(_df=df, _schema=self._schema)

    def window_based_clumping(
        self: SummaryStatistics,
        distance: int,
        gwas_significance: float = 5e-8,
        with_locus: bool = False,
        baseline_significance: float = 0.05,
        locus_collect_distance: int | None = None,
    ) -> StudyLocus:
        """Generate study-locus from summary statistics by distance based clumping + collect locus.

        Args:
            distance (int): Distance in base pairs to be used for clumping.
            gwas_significance (float, optional): GWAS significance threshold. Defaults to 5e-8.
            baseline_significance (float, optional): Baseline significance threshold for inclusion in the locus. Defaults to 0.05.
            locus_collect_distance (int, optional): The distance to collect locus around semi-indices.

        Returns:
            StudyLocus: Clumped study-locus containing variants based on window.
        """
        if locus_collect_distance is None:
            locus_collect_distance = distance
        # Based on if we want to get the locus different clumping function is called:
        if with_locus:
            clumped_df = WindowBasedClumping.clump_with_locus(
                self,
                window_length=distance,
                p_value_significance=gwas_significance,
                p_value_baseline=baseline_significance,
                locus_window_lenght=locus_collect_distance,
            )
        else:
            clumped_df = WindowBasedClumping.clump(
                self, window_length=distance, p_value_significance=gwas_significance
            )

        return clumped_df

    def exclude_region(self: SummaryStatistics, region: str) -> SummaryStatistics:
        """Exclude a region from the summary stats dataset.

        Args:
            region (str): region given in "chr##:#####-####" format

        Returns:
            SummaryStatistics: filtered summary statistics.
        """
        (chromosome, start_position, end_position) = parse_region(region)

        return SummaryStatistics(
            _df=(
                self.df.filter(
                    ~(
                        (f.col("chromosome") == chromosome)
                        & (
                            (f.col("position") >= start_position)
                            & (f.col("position") <= end_position)
                        )
                    )
                )
            ),
            _schema=SummaryStatistics.get_schema(),
        )
