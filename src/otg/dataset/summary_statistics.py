"""Summary satistics dataset."""
from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import parse_pvalue, pvalue_to_zscore
from otg.common.utils import split_pvalue
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from otg.common.session import Session


@dataclass
class SummaryStatistics(Dataset):
    """Summary Statistics dataset.

    A summary statistics dataset contains all single point statistics resulting from a GWAS.
    """

    _schema: t.StructType = parse_spark_schema("summary_statistics.json")

    @staticmethod
    def _convert_odds_ratio_to_beta(
        beta: Column, odds_ratio: Column, standard_error: Column
    ) -> tuple:
        """Harmonizes effect and standard error to beta.

        Args:
            beta (Column): Effect in beta
            odds_ratio (Column): Effect in odds ratio
            standard_error (Column): Standard error of the effect

        Returns:
            tuple: beta, standard error

        Examples:
            >>> df = spark.createDataFrame([{"beta": 0.1, "oddsRatio": 1.1, "standardError": 0.1}, {"beta": None, "oddsRatio": 1.1, "standardError": 0.1}, {"beta": 0.1, "oddsRatio": None, "standardError": 0.1}, {"beta": 0.1, "oddsRatio": 1.1, "standardError": None}])
            >>> df.select("*", *SummaryStatistics._convert_odds_ratio_to_beta(f.col("beta"), f.col("oddsRatio"), f.col("standardError"))).show()
            +----+---------+-------------+-------------------+-------------+
            |beta|oddsRatio|standardError|               beta|standardError|
            +----+---------+-------------+-------------------+-------------+
            | 0.1|      1.1|          0.1|                0.1|          0.1|
            |null|      1.1|          0.1|0.09531017980432493|         null|
            | 0.1|     null|          0.1|                0.1|          0.1|
            | 0.1|      1.1|         null|                0.1|         null|
            +----+---------+-------------+-------------------+-------------+
            <BLANKLINE>

        """
        # We keep standard error when effect is given in beta, otherwise drop.
        standard_error = f.when(
            standard_error.isNotNull() & beta.isNotNull(), standard_error
        ).alias("standardError")

        # Odds ratio is converted to beta:
        beta = (
            f.when(beta.isNotNull(), beta)
            .when(odds_ratio.isNotNull(), f.log(odds_ratio))
            .alias("beta")
        )

        return (beta, standard_error)

    @staticmethod
    def _calculate_confidence_interval(
        pvalue_mantissa: Column,
        pvalue_exponent: Column,
        beta: Column,
        standard_error: Column,
    ) -> tuple:
        """This function calculates the confidence interval for the effect based on the p-value and the effect size.

        If the standard error already available, don't re-calculate from p-value.

        Args:
            pvalue_mantissa (Column): p-value mantissa (float)
            pvalue_exponent (Column): p-value exponent (integer)
            beta (Column): effect size in beta (float)
            standard_error (Column): standard error.

        Returns:
            tuple: betaConfidenceIntervalLower (float), betaConfidenceIntervalUpper (float)
        """
        # Calculate p-value from mantissa and exponent:
        pvalue = pvalue_mantissa * f.pow(10, pvalue_exponent)

        # Fix p-value underflow:
        pvalue = f.when(pvalue == 0, sys.float_info.min).otherwise(pvalue)

        # Compute missing standard error:
        standard_error = f.when(
            standard_error.isNull(), f.abs(beta) / f.abs(pvalue_to_zscore(pvalue))
        ).otherwise(standard_error)

        # Calculate upper and lower confidence interval:
        ci_lower = (beta - standard_error).alias("betaConfidenceIntervalLower")
        ci_upper = (beta + standard_error).alias("betaConfidenceIntervalUpper")

        return (ci_lower, ci_upper)

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
        """Create summary statistics object from summary statistics harmonized by the GWAS Catalog.

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
        processed_sumstats_df = (
            sumstats_df.select(
                # Adding study identifier:
                f.lit(study_id).cast(t.StringType()).alias("studyId"),
                # Adding variant identifier:
                f.col("hm_variant_id").alias("variantId"),
                f.col("hm_chrom").alias("chromosome"),
                f.col("hm_pos").cast(t.IntegerType()).alias("position"),
                # Parsing p-value mantissa and exponent:
                *parse_pvalue(f.col("p_value").cast(t.FloatType())),
                # Converting/calculating effect and confidence interval:
                *cls._convert_odds_ratio_to_beta(
                    f.col("hm_beta").cast(t.DoubleType()),
                    f.col("hm_odds_ratio").cast(t.DoubleType()),
                    f.col("standard_error").cast(t.DoubleType()),
                ),
                allele_frequency_expression.alias("effectAlleleFrequencyFromSource"),
            )
            .repartition(200, "chromosome")
            .sortWithinPartitions("position")
        )

        # Initializing summary statistics object:
        return cls(
            _df=processed_sumstats_df,
        )

    @classmethod
    def from_ukbiobank_summary_stats(
        cls: type[SummaryStatistics],
        sumstats_df: DataFrame,
        ukbiobank_study_id: str,
    ) -> SummaryStatistics:
        """Create summary statistics object from UK Biobank summary statistics.

        Args:
            sumstats_df (DataFrame): Dataset read as dataframe from UK Biobank.
            ukbiobank_study_id (str): UK Biobank study id.

        Returns:
            SummaryStatistics
        """
        # The effect allele frequency is an optional column, we have to test if it is there:
        allele_frequency_expression = (
            f.col("effect_allele_frequency").cast(t.DoubleType())
            if "effect_allele_frequency" in sumstats_df.columns
            else f.lit(None)
        )

        # Processing columns of interest:
        processed_sumstats_df = (
            sumstats_df.select(
                # Adding study identifier:
                f.lit(ukbiobank_study_id).cast(t.StringType()).alias("studyId"),
                # Adding variant identifier:
                f.concat_ws(
                    "_",
                    f.col("chromosome"),
                    f.col("base_pair_location"),
                    f.col("other_allele"),
                    f.col("effect_allele"),
                ).alias("variantId"),
                f.col("chromosome").cast(t.StringType()).alias("chromosome"),
                f.col("base_pair_location").cast(t.IntegerType()).alias("position"),
                # Parsing p-value mantissa and exponent:
                *parse_pvalue(f.col("p-value").cast(t.FloatType())),
                # Converting/calculating effect and confidence interval:
                *cls._convert_odds_ratio_to_beta(
                    f.col("beta").cast(t.DoubleType()),
                    f.col("odds_ratio").cast(t.DoubleType()),
                    f.col("standard_error").cast(t.DoubleType()),
                ),
                allele_frequency_expression.alias("effectAlleleFrequencyFromSource"),
            )
            .repartition(200, "chromosome")
            .sortWithinPartitions("position")
        )

        # Initializing summary statistics object:
        return cls(
            _df=processed_sumstats_df,
        )

    def calculate_confidence_interval(self: SummaryStatistics) -> SummaryStatistics:
        """A Function to add upper and lower confidence interval to a summary statistics dataset.

        Returns:
            SummaryStatistics:
        """
        columns = self._df.columns

        # If confidence interval has already been calculated skip:
        if (
            "betaConfidenceIntervalLower" in columns
            and "betaConfidenceIntervalUpper" in columns
        ):
            return self

        # Calculate CI:
        return SummaryStatistics(
            _df=(
                self._df.select(
                    "*",
                    *self._calculate_confidence_interval(
                        f.col("pValueMantissa"),
                        f.col("pValueExponent"),
                        f.col("beta"),
                        f.col("standardError"),
                    ),
                )
            )
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
