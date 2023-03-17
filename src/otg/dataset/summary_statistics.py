"""Summary satistics dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import parse_pvalue, pvalue_to_zscore
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType

    from otg.common.session import Session


@dataclass
class SummaryStatistics(Dataset):
    """Summary Statistics dataset.

    A summary statistics dataset contains all single point statistics resulting from a GWAS.
    """

    _schema: StructType = parse_spark_schema("summary_statistics.json")

    @staticmethod
    def _harmonize_effect(
        pvalue: Column,
        beta: Column,
        odds_ratio: Column,
        ci_lower: Column,
        standard_error: Column,
    ) -> tuple:
        """Harmonizing effect to beta.

        - If odds-ratio is given, calculate beta.
        - If odds ratio is given re-calculate lower and upper confidence interval.
        - If no standard error is given, calculate from p-value and effect.

        Args:
            pvalue (Column): mandatory
            beta (Column): optional
            odds_ratio (Column): optional either beta or or should be provided.
            ci_lower (Column): optional
            standard_error (Column): optional

        Returns:
            tuple: columns containing beta, betaConfidenceIntervalLower and betaConfidenceIntervalUpper
        """
        perc_97th = 1.95996398454005423552

        # Converting all effect to beta:
        beta = (
            f.when(odds_ratio.isNotNull(), f.log(odds_ratio))
            .otherwise(beta)
            .alias("beta")
        )

        # Convert/calculate standard error when needed:
        standard_error = (
            f.when(
                standard_error.isNull(), f.abs(beta) / f.abs(pvalue_to_zscore(pvalue))
            )
            .when(
                odds_ratio.isNotNull(),
                (f.log(odds_ratio) - f.log(ci_lower)) / perc_97th,
            )
            .otherwise(standard_error)
        )

        # Calculate upper and lower beta:
        ci_lower = (beta - standard_error).alias("betaConfidenceIntervalLower")
        ci_upper = (beta + standard_error).alias("betaConfidenceIntervalUpper")

        return (beta, ci_lower, ci_upper)

    @classmethod
    def from_parquet(
        cls: type[SummaryStatistics], session: Session, path: str
    ) -> SummaryStatistics:
        """Initialise SummaryStatistics from parquet file.

        Args:
            session (Session): Session
            path (str): Path to parquet file

        Returns:
            SummaryStatistics: SummaryStatistics dataset
        """
        return super().from_parquet(session, path, cls._schema)

    @classmethod
    def from_gwas_harmonized_summary_stats(
        cls: type[SummaryStatistics], sumstats_df: DataFrame, study_id: str
    ) -> SummaryStatistics:
        """Create summary statistics object from harmonized dataset.

        Args:
            sumstats_df (DataFrame): Harmonized dataset read as dataframe from GWAS Catalog.
            study_id (str): GWAS Catalog Study accession.

        Returns:
            SummaryStatistics
        """
        # The effect allele frequency is an optional column, we have to test if it is there:
        allele_frequency_expression = (
            f.col("hm_effect_allele_frequency").cast(t.DoubleType())
            if "hm_effect_allele_frequency" in sumstats_df.columns
            else f.lit(None)
        )

        # Processing columns of interest:
        processed_sumstats_df = sumstats_df.select(
            # Adding study identifier:
            f.lit(study_id).cast(t.StringType()).alias("studyId"),
            # Adding variant identifier:
            f.col("hm_variant_id").alias("variantId"),
            f.col("hm_chrom").alias("chromosome"),
            f.col("hm_pos").cast(t.LongType()).alias("position"),
            # Parsing p-value mantissa and exponent:
            *parse_pvalue(f.col("p_value").cast(t.FloatType())),
            # Converting/calculating effect and confidence interval:
            *cls._harmonize_effect(
                f.col("p_value").cast(t.FloatType()),
                f.col("hm_beta").cast(t.FloatType()),
                f.col("hm_odds_ratio").cast(t.FloatType()),
                f.col("hm_ci_lower").cast(t.FloatType()),
                f.col("standard_error").cast(t.FloatType()),
            ),
            allele_frequency_expression.alias("effectAlleleFrequencyFromSource"),
        )
        # Initializing summary statistics object:
        summary_stats = cls(
            _df=processed_sumstats_df,
        )

        return summary_stats.df.repartition(
            200,
            "chromosome",
        ).sortWithinPartitions("position")
