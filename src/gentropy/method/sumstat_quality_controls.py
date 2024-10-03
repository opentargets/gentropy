"""Summary statistics qulity control methods."""

from __future__ import annotations

import numpy as np
import pyspark.sql.functions as f
import pyspark.sql.types as t
import scipy as sc
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, log10, row_number
from pyspark.sql.window import Window
from scipy.stats import chi2

from gentropy.dataset.summary_statistics import SummaryStatistics


class SummaryStatisticsQC:
    """Summary statistics QC methods.

    This module contains methods for quality control of GWAS summary statistics.
    The list of methods includes:

        - sumstat_qc_beta_check: This is the mean beta check. The mean beta should be close to 0.

        - sumstat_qc_pz_check: This is the PZ check. It runs a linear regression between reported p-values and p-values inferred from z-scores.

        - sumstat_n_eff_check: This is the effective sample size check. It estimates the ratio between the effective sample size and the expected one and checks its distribution.

        - gc_lambda_check: This is the genomic control lambda check.

        - number_of_snps: This function calculates the number of SNPs and the number of SNPs with a p-value less than 5e-8.
    """

    @staticmethod
    def sumstat_qc_beta_check(
        gwas_for_qc: SummaryStatistics,
    ) -> DataFrame:
        """The mean beta check for QC of GWAS summary statstics.

        Args:
            gwas_for_qc (SummaryStatistics): The instance of the SummaryStatistics class.

        Returns:
            DataFrame: PySpark DataFrame with the mean beta for each study.
        """
        gwas_df = gwas_for_qc._df
        qc_c = gwas_df.groupBy("studyId").agg(
            f.mean("beta").alias("mean_beta"),
        )
        return qc_c

    @staticmethod
    def _calculate_logpval(z2: float) -> float:
        """Calculate negative log10-pval from Z-score.

        Args:
            z2 (float): Z-score squared.

        Returns:
            float: log10-pval.

        Examples:
            >>> SummaryStatisticsQC._calculate_logpval(1.0)
            0.49851554582799334
        """
        logpval = -np.log10(sc.stats.chi2.sf((z2), 1))
        return float(logpval)

    @staticmethod
    def sumstat_qc_pz_check(
        gwas_for_qc: SummaryStatistics,
    ) -> DataFrame:
        """The PZ check for QC of GWAS summary statstics. It runs linear regression between reported p-values and p-values infered from z-scores.

        Args:
            gwas_for_qc (SummaryStatistics): The instance of the SummaryStatistics class.

        Returns:
            DataFrame: PySpark DataFrame with the results of the linear regression for each study.
        """
        gwas_df = gwas_for_qc._df

        calculate_logpval_udf = f.udf(
            SummaryStatisticsQC._calculate_logpval, t.DoubleType()
        )

        qc_c = (
            gwas_df.withColumn("Z2", (f.col("beta") / f.col("standardError")) ** 2)
            .filter(f.col("Z2") <= 100)
            .withColumn("new_logpval", calculate_logpval_udf(f.col("Z2")))
            .withColumn("log_mantissa", log10("pValueMantissa"))
            .withColumn(
                "diffpval",
                -f.col("log_mantissa") - f.col("pValueExponent") - f.col("new_logpval"),
            )
            .groupBy("studyId")
            .agg(
                f.mean("diffpval").alias("mean_diff_pz"),
                f.stddev("diffpval").alias("se_diff_pz"),
            )
            .select("studyId", "mean_diff_pz", "se_diff_pz")
        )

        return qc_c

    @staticmethod
    def sumstat_n_eff_check(
        gwas_for_qc: SummaryStatistics,
        n_total: int = 100_000,
        limit: int = 10_000_000,
        min_count: int = 100,
    ) -> DataFrame:
        """The effective sample size check for QC of GWAS summary statstics.

        It estiamtes the ratio between effective sample size and the expected one and checks it's distribution.
        It is possible to conduct only if the effective allele frequency is provided in the study.
        The median rartio is always close to 1, but standard error could be inflated.

        Args:
            gwas_for_qc (SummaryStatistics): The instance of the SummaryStatistics class.
            n_total (int): The reported sample size of the study. The QC metrics is robust toward the sample size.
            limit (int): The limit for the number of variants to be used for the estimation.
            min_count (int): The minimum number of variants to be used for the estimation.

        Returns:
            DataFrame: PySpark DataFrame with the effective sample size ratio for each study.
        """
        gwas_df = gwas_for_qc._df

        gwas_df = gwas_df.dropna(subset=["effectAlleleFrequencyFromSource"])

        counts_df = gwas_df.groupBy("studyId").count()

        # Join the original DataFrame with the counts DataFrame
        df_with_counts = gwas_df.join(counts_df, on="studyId")

        # Filter the DataFrame to keep only the groups with count greater than or equal to min_count
        filtered_df = df_with_counts.filter(f.col("count") >= min_count).drop("count")

        window = Window.partitionBy("studyId").orderBy("studyId")
        gwas_df = (
            filtered_df.withColumn("row_num", row_number().over(window))
            .filter(f.col("row_num") <= limit)
            .drop("row_num")
        )

        gwas_df = gwas_df.withColumn(
            "var_af",
            2
            * (
                f.col("effectAlleleFrequencyFromSource")
                * (1 - f.col("effectAlleleFrequencyFromSource"))
            ),
        ).withColumn(
            "pheno_var",
            ((f.col("standardError") ** 2) * n_total * f.col("var_af"))
            + ((f.col("beta") ** 2) * f.col("var_af")),
        )

        window = Window.partitionBy("studyId").orderBy("studyId")

        # Calculate the median of 'pheno_var' for each 'studyId' and add it as a new column
        gwas_df = gwas_df.withColumn(
            "pheno_median", expr("percentile_approx(pheno_var, 0.5)").over(window)
        )

        gwas_df = gwas_df.withColumn(
            "N_hat_ratio",
            (
                (f.col("pheno_median") - ((f.col("beta") ** 2) * f.col("var_af")))
                / ((f.col("standardError") ** 2) * f.col("var_af") * n_total)
            ),
        )

        qc_c = (
            gwas_df.groupBy("studyId")
            .agg(
                f.stddev("N_hat_ratio").alias("se_N"),
            )
            .select("studyId", "se_N")
        )

        return qc_c

    @staticmethod
    def gc_lambda_check(
        gwas_for_qc: SummaryStatistics,
    ) -> DataFrame:
        """The genomic control lambda check for QC of GWAS summary statstics.

        Args:
            gwas_for_qc (SummaryStatistics): The instance of the SummaryStatistics class.

        Returns:
            DataFrame: PySpark DataFrame with the genomic control lambda for each study.
        """
        gwas_df = gwas_for_qc._df

        qc_c = (
            gwas_df.select("studyId", "beta", "standardError")
            .withColumn("Z2", (f.col("beta") / f.col("standardError")) ** 2)
            .groupBy("studyId")
            .agg(f.expr("percentile_approx(Z2, 0.5)").alias("gc_lambda"))
            .withColumn("gc_lambda", f.col("gc_lambda") / chi2.ppf(0.5, df=1))
            .select("studyId", "gc_lambda")
        )

        return qc_c

    @staticmethod
    def number_of_snps(
        gwas_for_qc: SummaryStatistics, pval_threshold: float = 5e-8
    ) -> DataFrame:
        """The function caluates number of SNPs and number of SNPs with p-value less than 5e-8.

        Args:
            gwas_for_qc (SummaryStatistics): The instance of the SummaryStatistics class.
            pval_threshold (float): The threshold for the p-value.

        Returns:
            DataFrame: PySpark DataFrame with the number of SNPs and number of SNPs with p-value less than threshold.
        """
        gwas_df = gwas_for_qc._df

        snp_counts = gwas_df.groupBy("studyId").agg(
            f.count("*").alias("n_variants"),
            f.sum(
                (
                    f.log10(f.col("pValueMantissa")) + f.col("pValueExponent")
                    <= np.log10(pval_threshold)
                ).cast("int")
            ).alias("n_variants_sig"),
        )

        return snp_counts

    @staticmethod
    def get_quality_control_metrics(
        gwas: SummaryStatistics,
        pval_threshold: float = 1e-8,
    ) -> DataFrame:
        """The function calculates the quality control metrics for the summary statistics.

        Args:
            gwas (SummaryStatistics): The instance of the SummaryStatistics class.
            pval_threshold (float): The threshold for the p-value.

        Returns:
            DataFrame: PySpark DataFrame with the quality control metrics for the summary statistics.
        """
        qc1 = SummaryStatisticsQC.sumstat_qc_beta_check(gwas_for_qc=gwas)
        qc2 = SummaryStatisticsQC.sumstat_qc_pz_check(gwas_for_qc=gwas)
        qc4 = SummaryStatisticsQC.gc_lambda_check(gwas_for_qc=gwas)
        qc5 = SummaryStatisticsQC.number_of_snps(
            gwas_for_qc=gwas, pval_threshold=pval_threshold
        )
        df = (
            qc1.join(qc2, on="studyId", how="outer")
            .join(qc4, on="studyId", how="outer")
            .join(qc5, on="studyId", how="outer")
        )

        return df
