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
    def from_ingested_gwas_summarystats(
        cls: type[SummaryStatistics], session: Session, path: str
    ) -> SummaryStatistics:
        """Generate summary statistics dataset from ingested GWAS Catalog summary statistics.

        Args:
            session (Session): Session
            path (str): path to ingested summary satistics in parquet format

        Returns:
            SummaryStatistics
        """
        # Raw sumstats schema:
        gwas_sumstats_schema = t.StructType(
            [
                t.StructField("studyId", t.StringType(), True),
                t.StructField("rsId", t.StringType(), True),
                t.StructField("variantId", t.StringType(), True),
                t.StructField("chromosome", t.StringType(), True),
                t.StructField("position", t.IntegerType(), True),
                t.StructField("referenceAllele", t.StringType(), True),
                t.StructField("alternateAllele", t.StringType(), True),
                t.StructField("pValue", t.StringType(), True),
                t.StructField("oddsRatio", t.DoubleType(), True),
                t.StructField("beta", t.DoubleType(), True),
                t.StructField("confidenceIntervalLower", t.DoubleType(), True),
                t.StructField("confidenceIntervalUpper", t.DoubleType(), True),
                t.StructField("standardError", t.DoubleType(), True),
                t.StructField("alternateAlleleFrequency", t.DoubleType(), True),
            ]
        )

        # Reading all data:
        df = (
            session.spark.read.option("recursiveFileLookup", "true")
            .schema(gwas_sumstats_schema)
            .parquet(path)
        )

        summarystats_df = (
            df.select(
                # Select study and variant related columns:
                "studyId",
                "variantId",
                "chromosome",
                f.col("position").cast(t.IntegerType()),
                # Parsing p-value (string) to mantissa (float) and exponent (int):
                *parse_pvalue(f.col("pValue").cast(t.FloatType())),
                # Converting/calculating effect and confidence interval:
                *cls._convert_oddsRatio_to_beta(
                    f.col("beta"),
                    f.col("oddsRatio"),
                    f.col("standardError"),
                ),
                f.col("alternateAlleleFrequency").alias(
                    "effectAlleleFrequencyFromSource"
                ),
            )
            .repartition(200, "chromosome")
            .sortWithinPartitions("position")
        )

        # Returning summary statistics:
        return cls(
            _df=summarystats_df,
        )

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
                *parse_pvalue(f.col("pValue").cast(t.FloatType())),
                # Converting/calculating effect and confidence interval:
                *cls._convert_oddsRatio_to_beta(
                    f.col("hm_beta"),
                    f.col("hm_odds_ratio"),
                    f.col("standard_error"),
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
