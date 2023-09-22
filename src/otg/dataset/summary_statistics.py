"""Summary satistics dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.common.schemas import parse_spark_schema
from otg.common.utils import (
    calculate_confidence_interval,
    convert_odds_ratio_to_beta,
    parse_pvalue,
)
from otg.dataset.single_point_association import SinglePointAssociation
from otg.method.window_based_clumping import WindowBasedClumping

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.dataset.study_locus import StudyLocus


@dataclass
class SummaryStatistics(SinglePointAssociation):
    """Summary Statistics dataset.

    A summary statistics dataset contains all single point statistics resulting from a GWAS.
    """

    @classmethod
    def get_schema(cls: type[SummaryStatistics]) -> StructType:
        """Provides the schema for the SummaryStatistics dataset."""
        return parse_spark_schema("summary_statistics.json")

    @classmethod
    def from_gwas_harmonized_summary_stats(
        cls: type[SummaryStatistics],
        sumstats_df: DataFrame,
        study_id: str,
    ) -> SummaryStatistics:
        """Create summary statistics object from summary statistics flatfile, harmonized by the GWAS Catalog.

        Args:
            sumstats_df (DataFrame): Harmonized dataset read as a spark dataframe from GWAS Catalog.
            study_id (str): GWAS Catalog study accession.

        Returns:
            SummaryStatistics
        """
        # The effect allele frequency is an optional column, we have to test if it is there:
        allele_frequency_expression = (
            f.col("hm_effect_allele_frequency").cast(t.FloatType())
            if "hm_effect_allele_frequency" in sumstats_df.columns
            else f.lit(None)
        )

        # Processing columns of interest:
        processed_sumstats_df = (
            sumstats_df
            # Dropping rows which doesn't have proper position:
            .filter(f.col("hm_pos").cast(t.IntegerType()).isNotNull())
            .select(
                # Adding study identifier:
                f.lit(study_id).cast(t.StringType()).alias("studyId"),
                # Adding variant identifier:
                f.col("hm_variant_id").alias("variantId"),
                f.col("hm_chrom").alias("chromosome"),
                f.col("hm_pos").cast(t.IntegerType()).alias("position"),
                # Parsing p-value mantissa and exponent:
                *parse_pvalue(f.col("p_value")),
                # Converting/calculating effect and confidence interval:
                *convert_odds_ratio_to_beta(
                    f.col("hm_beta").cast(t.DoubleType()),
                    f.col("hm_odds_ratio").cast(t.DoubleType()),
                    f.col("standard_error").cast(t.DoubleType()),
                ),
                allele_frequency_expression.alias("effectAlleleFrequencyFromSource"),
            )
            # The previous select expression generated the necessary fields for calculating the confidence intervals:
            .select(
                "*",
                *calculate_confidence_interval(
                    f.col("pValueMantissa"),
                    f.col("pValueExponent"),
                    f.col("beta"),
                    f.col("standardError"),
                ),
            )
            .repartition(200, "chromosome")
            .sortWithinPartitions("position")
        )

        # Initializing summary statistics object:
        return cls(
            _df=processed_sumstats_df,
            _schema=cls.get_schema(),
        )

    def window_based_clumping(
        self: SummaryStatistics,
        distance: int,
        gwas_significance: float = 5e-8,
        with_locus: bool = False,
        baseline_significance: float = 0.05,
    ) -> StudyLocus:
        """Generate study-locus from summary statistics by distance based clumping + collect locus.

        Args:
            distance (int): Distance in base pairs to be used for clumping.
            gwas_significance (float, optional): GWAS significance threshold. Defaults to 5e-8.
            baseline_significance (float, optional): Baseline significance threshold for inclusion in the locus. Defaults to 0.05.

        Returns:
            StudyLocus: Clumped study-locus containing variants based on window.
        """
        # Based on if we want to get the locus different clumping function is called:
        if with_locus:
            clumped_df = WindowBasedClumping.clump_with_locus(
                self,
                window_length=distance,
                p_value_significance=gwas_significance,
                p_value_baseline=baseline_significance,
            )
        else:
            clumped_df = WindowBasedClumping.clump(
                self, window_length=distance, p_value_significance=gwas_significance
            )

        return clumped_df
