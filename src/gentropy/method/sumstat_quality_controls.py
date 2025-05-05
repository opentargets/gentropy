"""Summary statistics QC methods.

This module contains methods for quality control of GWAS summary statistics.
The list of methods includes:

    - sumstat_qc_beta_check: This is the mean beta check. The mean beta should be close to 0.

    - sumstat_qc_pz_check: This is the PZ check. It runs a linear regression between reported p-values and p-values inferred from z-scores.

    - sumstat_n_eff_check: This is the effective sample size check. It estimates the ratio between the effective sample size and the expected one and checks its distribution.

    - gc_lambda_check: This is the genomic control lambda check.

    - number_of_snps: This function calculates the number of SNPs and the number of SNPs with a p-value less than 5e-8.
"""

from __future__ import annotations

import numpy as np
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import expr, row_number
from pyspark.sql.window import Window
from scipy.stats import chi2

from gentropy.dataset.summary_statistics import SummaryStatistics


def neglogpval_from_z2(z2: float) -> float:
    """Calculate negative log10 of p-value from squared Z-score following chi2 distribution.

    **The Z-score^2 is equal to the chi2 with 1 degree of freedom.**

    Args:
        z2 (float): Z-score squared.

    Returns:
        float:  negative log of p-value.

    Examples:
        >>> round(calculate_logpval(1.0),2)
        0.5
    """
    logpval = -np.log10(chi2.sf((z2), 1))
    return float(logpval)


def sumstat_qc_beta_check(
    gwas_for_qc: DataFrame,
) -> DataFrame:
    """The mean beta check for QC of GWAS summary statistics.

    This function expects to have a dataframe with `studyId` and `beta` columns and
    outputs the dataframe with mean beta aggregated over the studyId.

    Args:
        gwas_for_qc (SummaryStatistics): The instance of the SummaryStatistics class.

    Returns:
        DataFrame: PySpark DataFrame with the mean beta for each study.

    Examples:
        >>> s = "studyId STRING, variantId STRING, beta DOUBLE"
        >>> df = spark.createDataFrame([('S1', '1_10000_A_T', 1.0), ('S1', '1_10001_C_T', 1.0), ('S2', '1_10001_C_T', 0.028)], schema=s)
        >>> sumstat_qc_beta_check(df).show()
        +-------+---------+
        |studyId|mean_beta|
        +-------+---------+
        |     S1|      1.0|
        |     S2|    0.028|
        +-------+---------+
        <BLANKLINE>
    """
    study_id = f.col("studyId")
    beta = f.col("beta")
    qc_c = gwas_for_qc.groupBy(study_id).agg(
        f.mean(beta).alias("mean_beta"),
    )
    return qc_c


def genotypic_variance(af: Column) -> Column:
    """Calculate the genotypic variance explained - VarG.

    Args:
        af (Column): Allele frequency.

    Returns:
        Column: Column varG with genotypic variance.

    Examples:
        >>> s = 'variantId STRING, alleleFrequency FLOAT'
        >>> d = [('1_10001_C_T', 0.01), ('1_10002_G_C', 0.50), ('1_10003_A_T', 0.99)]
        >>> df = spark.createDataFrame(d, s)
        >>> var_g = f.round(genotypic_variance(f.col('alleleFrequency')), 2).alias('varG')
        >>> df.select('variantId', 'alleleFrequency', var_g).show()
        +-----------+---------------+----+
        |  variantId|alleleFrequency|varG|
        +-----------+---------------+----+
        |1_10001_C_T|           0.01|0.02|
        |1_10002_G_C|            0.5| 0.5|
        |1_10003_A_T|           0.99|0.02|
        +-----------+---------------+----+
        <BLANKLINE>
    """
    return (2 * af * (1 - af)).alias("varG")


def genetic_variance_explained_by_snp(af: Column, beta: Column) -> Column:
    return (f.pow(beta, 2.0) * genotypic_variance(af)).alias("varSNP")


def phenotypic_variance_explained_by_snp(
    af: Column, beta: Column, se: Column, sample_size: Column
) -> Column:
    """Calculate the phenotypic variance explained by SNP."""
    return (+genetic_variance_explained_by_snp(af, beta)).alias("pve")


def residual_variance_explained(se: Column, sample_size: Column, af: Column) -> Column:
    return f.pow(se, 2.0) * sample_size * genotypic_variance(af).alias("varResidual")


def sumstat_qc_pz_check(
    gwas_for_qc: DataFrame,
) -> DataFrame:
    """The PZ check for QC of GWAS summary statistics. It runs linear regression between reported p-values and p-values inferred from z-scores.

    The algorithm does following things:
    (1) recalculates the negative logarithm of p-value from the square z-score
    (2) calculates the difference between the sum of logarithms derived form reported p-value mantissa and exponent and value recalculated from z-score.

    Args:
        gwas_for_qc (SummaryStatistics): The instance of the SummaryStatistics class.

    Returns:
        DataFrame: PySpark DataFrame with the results of the linear regression for each study.

    Examples:
        >>> s = 'studyId STRING, beta DOUBLE, standardError DOUBLE, pValueMantissa FLOAT, pValueExponent INTEGER'
        >>> # Example where the variant reaches upper and lower boundaries for mantissa and upper bound for exponent
        >>> d1 = [("S1", 1.81, 0.2, 9.9, -20), ("S1", -0.1, 0.2, 1.0, -1)]
        >>> # Example where z-score^2 (beta / se) > 100
        >>> d2 = [("S2", 101.0, 10.0, 1.0, -1), ("S2", 1.0, 0.1, 1.0, -1), ("S2", 1.0, 0.1, 2.0, -2)]
        >>> df = spark.createDataFrame(d1 + d2, s)
        >>> df.show()
        +-------+-----+-------------+--------------+--------------+
        |studyId| beta|standardError|pValueMantissa|pValueExponent|
        +-------+-----+-------------+--------------+--------------+
        |     S1| 1.81|          0.2|           9.9|           -20|
        |     S1| -0.1|          0.2|           1.0|            -1|
        |     S2|101.0|         10.0|           1.0|            -1|
        |     S2|  1.0|          0.1|           1.0|            -1|
        |     S2|  1.0|          0.1|           2.0|            -2|
        +-------+-----+-------------+--------------+--------------+
        <BLANKLINE>

        This method outputs two values per study, mean and standard deviation of the difference between log p-value(s)

        >>> mean_diff_pz = f.round("mean_diff_pz", 2).alias("mean_diff_pz")
        >>> se_diff_pz = f.round("se_diff_pz", 2).alias("se_diff_pz")
        >>> sumstat_qc_pz_check(df).select('studyId', mean_diff_pz, se_diff_pz).show()
        +-------+------------+----------+
        |studyId|mean_diff_pz|se_diff_pz|
        +-------+------------+----------+
        |     S1|        0.47|      0.45|
        |     S2|      -21.47|      0.49|
        +-------+------------+----------+
        <BLANKLINE>
    """
    neglogpval_from_z2_udf = f.udf(neglogpval_from_z2, t.DoubleType())

    qc_c = (
        gwas_for_qc.withColumn("Z2", (f.col("beta") / f.col("standardError")) ** 2)
        .filter(f.col("Z2") <= 100)
        .withColumn("neglogpValFromZScore", neglogpval_from_z2_udf(f.col("Z2")))
        .withColumn(
            "neglogpVal", -1 * (f.log10("pValueMantissa") + f.col("pValueExponent"))
        )
        .withColumn("diffpval", f.col("neglogpVal") - f.col("neglogpValfromZScore"))
        .groupBy("studyId")
        .agg(
            f.mean("diffpval").alias("mean_diff_pz"),
            # FIXME: We actually calculate standard deviation, not standard error
            f.stddev("diffpval").alias("se_diff_pz"),
        )
        .select("studyId", "mean_diff_pz", "se_diff_pz")
    )

    return qc_c


def sumstat_n_eff_check(
    gwas_for_qc: DataFrame,
    n_total: int = 100_000,
    limit: int = 10_000_000,
    min_count: int = 100,
) -> DataFrame:
    """The effective sample size check for QC of GWAS summary statistics.

    It estimates the ratio between effective sample size and the expected one and checks it's distribution.
    It is possible to conduct only if the effective allele frequency is provided in the study.
    The median ratio is always close to 1, but standard error could be inflated.

    Args:
        gwas_for_qc (DataFrame): The instance of the SummaryStatistics class.
        n_total (int): The reported sample size of the study. The QC metrics is robust toward the sample size.
        limit (int): The limit for the number of variants to be used for the estimation.
        min_count (int): The minimum number of variants to be used for the estimation.

    Returns:
        DataFrame: PySpark DataFrame with the effective sample size ratio for each study.


    Examples:
        >>> s = 'studyId STRING, beta DOUBLE, standardError DOUBLE, effectAlleleFrequencyFromSource FLOAT'
        >>> # Example where we have a very common and very rare variant
        >>> d1 = [("S1", 1.81, 0.2, 0.999), ("S1", -0.1, 0.2, 0.001), ("S1", 1.0, 0.1, 0.5)]
        >>> # Example where z-score^2 (beta / se) > 100
        >>> d2 = [("S2", 1.81, 0.2, None), ("S2", 1.0, 0.1, 0.5), ("S2", 1.0, 0.1, 0.5)]
        >>> df = spark.createDataFrame(d1 + d2, s)
        >>> df.show()
        +-------+----+-------------+-------------------------------+
        |studyId|beta|standardError|effectAlleleFrequencyFromSource|
        +-------+----+-------------+-------------------------------+
        |     S1|1.81|          0.2|                          0.999|
        |     S1|-0.1|          0.2|                          0.001|
        |     S2|1.81|          0.2|                           NULL|
        |     S2| 1.0|          0.1|                            0.5|
        |     S2| 1.0|          0.1|                            0.5|
        +-------+----+-------------+-------------------------------+
        <BLANKLINE>

        This method outputs one value per study

        >>> se_n = f.round("se_N", 2)
        >>> sumstat_n_eff_check(df, min_count=2, limit=2).select("studyId", se_n).show()
        +-------+--------------+
        |studyId|round(se_N, 2)|
        +-------+--------------+
        |     S1|           0.0|
        |     S2|           0.0|
        +-------+--------------+
        <BLANKLINE>

    """
    af = f.col("effectAlleleFrequencyFromSource")
    se = f.col("standardError")
    varG = f.col("genotypicVariance")
    window = Window.partitionBy("studyId").orderBy("studyId")

    df_with_counts = gwas_for_qc.dropna(
        subset=["effectAlleleFrequencyFromSource"]
    ).withColumn("count", f.count("studyId").over(window))

    # Filter the DataFrame to keep only the groups with count greater than or equal to min_count
    filtered_df = df_with_counts.filter(f.col("count") >= min_count).drop("count")

    # Keep the number of variants up to the limit
    gwas_df = (
        filtered_df.withColumn("row_num", row_number().over(window))
        .filter(f.col("row_num") <= limit)
        .drop("row_num")
    )
    # Calculate the genotypic variance following the formula 2 * AlleleFrequency * (1 - AlleleFrequency)
    gwas_df = gwas_df.withColumn(
        "genotypicVariance", genotypic_variance(af)
    ).withColumn("pheno_var", (se**2) * n_total * varG + ((f.col("beta") ** 2) * varG))

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
