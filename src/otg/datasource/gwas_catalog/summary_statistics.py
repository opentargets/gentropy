"""GWAS Catalog Summary Statistics reader."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.common.spark_helpers import neglog_pvalue_to_mantissa_and_exponent
from otg.common.utils import convert_odds_ratio_to_beta, parse_pvalue
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
            f.col("effect_allele_frequency").cast(t.FloatType())
            if "effect_allele_frequency" in sumstats_df.columns
            else f.lit(None)
        )

        # Do we have sample size? This expression captures 99.7% of sample size columns.
        sample_size_expression = (
            f.col("n").cast(t.IntegerType())
            if "n" in sumstats_df.columns
            else f.lit(None).cast(t.IntegerType())
        )

        # Depending on the input, we might have beta, but the column might not be there at all:
        beta_expression = (
            f.col("beta").cast(t.DoubleType())
            if "beta" in sumstats_df.columns
            else f.lit(None).alias("beta").cast(t.DoubleType())
        )

        # We might have odds ratio or hazard ratio, wich are basically the same:
        odds_ratio_expression = (
            f.col("odds_ratio").cast(t.DoubleType())
            if "odds_ratio" in sumstats_df.columns
            else f.col("hazard_ratio").alias("odds_ratio").cast(t.DoubleType())
            if "hazard_ratio" in sumstats_df.columns
            else f.lit(None).alias("odds_ratio").cast(t.DoubleType())
        )

        # p-values might come in two different flavours:
        pvalue_expression = (
            parse_pvalue(f.col("p_value"))
            if "p_value" in sumstats_df.columns
            else neglog_pvalue_to_mantissa_and_exponent(f.col(""))
        )

        # Processing columns of interest:
        processed_sumstats_df = (
            sumstats_df
            # Dropping rows which doesn't have proper position:
            .select(
                # Adding study identifier:
                f.lit(study_id).cast(t.StringType()).alias("studyId"),
                # Adding variant identifier:
                f.concat_ws(
                    "_",
                    f.col("chromosome"),
                    f.col("position"),
                    f.col("other_allele"),
                    f.col("effect_allele"),
                ).alias("variantId"),
                f.col("chromosome").alias("chromosome"),
                f.col("base_pair_location").cast(t.IntegerType()).alias("position"),
                # Parsing p-value mantissa and exponent:
                *pvalue_expression,
                # Converting/calculating effect and confidence interval:
                *convert_odds_ratio_to_beta(
                    beta_expression,
                    odds_ratio_expression,
                    f.col("standard_error").cast(t.DoubleType()),
                ),
                allele_frequency_expression.alias("effectAlleleFrequencyFromSource"),
                sample_size_expression.alias("sampleSize"),
            )
            .filter(
                # Dropping associations where no harmonized position is available:
                f.col("position").isNotNull()
                &
                # We are not interested in associations with zero effect:
                (f.col("beta") != 0)
            )
            .orderBy(f.col("chromosome"), f.col("position"))
            .repartition(400)
        )

        # Initializing summary statistics object:
        return cls(
            _df=processed_sumstats_df,
            _schema=cls.get_schema(),
        )
