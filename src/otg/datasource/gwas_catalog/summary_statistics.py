"""GWAS Catalog Summary Statistics reader."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.common.utils import (
    calculate_confidence_interval,
    convert_odds_ratio_to_beta,
    parse_pvalue,
)
from otg.dataset.summary_statistics import SummaryStatistics

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class GWASCatalogSummaryStatistics(SummaryStatistics):
    """GWAS Catalog Summary Statistics reader."""

    @classmethod
    def from_gwas_harmonized_summary_stats(
        cls: type[GWASCatalogSummaryStatistics],
        sumstats_df: DataFrame,
        study_id: str,
    ) -> GWASCatalogSummaryStatistics:
        """Create summary statistics object from summary statistics flatfile, harmonized by the GWAS Catalog.

        Args:
            sumstats_df (DataFrame): Harmonized dataset read as a spark dataframe from GWAS Catalog.
            study_id (str): GWAS Catalog study accession.

        Returns:
            GWASCatalogSummaryStatistics: Summary statistics object.
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
