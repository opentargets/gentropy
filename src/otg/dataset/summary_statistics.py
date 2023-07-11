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
    split_pvalue,
)
from otg.dataset.dataset import Dataset
from otg.method.window_based_clumping import WindowBasedClumping

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from otg.common.session import Session
    from otg.dataset.study_locus import StudyLocus


@dataclass
class SummaryStatistics(Dataset):
    """Summary Statistics dataset.

    A summary statistics dataset contains all single point statistics resulting from a GWAS.
    """

    _schema: t.StructType = parse_spark_schema("summary_statistics.json")

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
        df = session.read_parquet(path=path, schema=cls._schema)
        return cls(_df=df, _schema=cls._schema)

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
        )

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
        return SummaryStatistics(_df=df)

    def window_based_clumping(self: SummaryStatistics, distance: int) -> StudyLocus:
        """Perform distance-based clumping.

        Args:
            distance (int): Distance in base pairs

        Returns:
            StudyLocus: StudyLocus object
        """
        # Calculate distance-based clumping:
        return WindowBasedClumping.clump(self, distance)
