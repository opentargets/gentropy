"""Summary statistics QC methods.

This module contains methods for quality control of GWAS summary statistics.
The list of methods includes:

    - mean_beta_check : This is the mean beta check. The mean beta should be close to 0.

    - pz_test: This is the PZ check. It runs a linear regression between reported p-values and p-values inferred from z-scores.

    - sumstat_n_eff_check: This is the effective sample size check. It estimates the ratio between the effective sample size and the expected one and checks its distribution.

    - gc_lambda_check: This is the genomic control lambda check.

    - number_of_variants: This function calculates the number of SNPs and the number of SNPs with a p-value less than 5e-8.
"""

from __future__ import annotations

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from scipy.stats import chi2

from gentropy.common.stats import neglogpval_from_z2


def genotypic_variance(af: Column) -> Column:
    """Calculate the genotypic variance of biallelic SNP.

    The genotypic variance of a biallelic SNP refers to the statistical variance in the genotype values across individuals in a population, where the SNP has two alleles,
    Var(G) tells us how much individuals’ genotypes deviate from the average number of minor alleles E[G].


    Note:
        The formula is derived from `Hardy-Weinberg Equilibrium` where

            * 0 = Homozygous major (AA), Pr(G=0)=(1−f)^2
            * 1 = Heterozygous (Aa), Pr(G=1)=2f(1-f)
            * 2 = Homozygous minor (aa). Pr(G=2)=f^2

        Total genotypic variance of biallelic SNP is:
        ```
        Var(G) = E[G^2] - (E[G])^2
        E[G] - expected value - average number of copies of the minor allele in the population at a specific biallelic SNP.
        E[G] = sum(Pr(G) * G)
        ```
        Calculate the expected value of the square of the genotype values.
        The probabilities are not squared.

        ```
        E[G^2] = sum((Pr(G) * G)^2)
        E[G^2] = (Pr(G=0) * 0^2) + (Pr(G=1) * 1^2) + (Pr(G=2) * 2^2)
        E[G^2] = 2f(1-f) + 4f^2
        ```
        Calculate the square of the expected value of the genotype values
        ```
        (E[G])^2 = (sum(Pr(G) * G)^2
        (E[G])^2 = ((1−f)^2 * 0 + 2f(1-f) * 1 + f^2 * 2)^2
        (E[G])^2 = (0 + 2f(1-f) + 2f^2)^2
        (E[G])^2 = (2f - 2f^2 + 2f^2)^2
        (E[G])^2 = 4f^2
        ```

        Calculate the variance of minor allele
        ```
        Var(G) = E[G^2] - (E[G])^2
        Var(G) = 2f(1-f) + 4f^2 - 4f^2
        Var(G) = 2f(1-f)
        ```

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


def gc_lambda_check(gwas_for_qc: DataFrame) -> DataFrame:
    """The genomic control lambda check for QC of GWAS summary statistics.

    The genomic control lambda is a measure of the inflation of test statistics in a GWAS.
    It is calculated as the ratio of the median of the squared test statistics to the expected median under the null hypothesis.
    The expected median under the null hypothesis is calculated using the chi-squared distribution with 1 degree of freedom.

    Args:
        gwas_for_qc (DataFrame): Dataframe with `studyId`, `beta`, and `standardError` columns.

    Returns:
        DataFrame: PySpark DataFrame with the genomic control lambda for each study.

    Warning:
        High lambda values indicate inflation of test statistics, which may be due to the population stratification or other confounding factors.

    Examples:
        >>> s = 'studyId STRING, beta DOUBLE, standardError DOUBLE'
        >>> d1 = [("S1", 1.81, 0.2), ("S1", -0.1, 0.2)]
        >>> d2 = [("S2", 1.0, 0.1), ("S2", 1.0, 0.1)]
        >>> df = spark.createDataFrame(d1 + d2, s)
        >>> df.show()
        +-------+----+-------------+
        |studyId|beta|standardError|
        +-------+----+-------------+
        |     S1|1.81|          0.2|
        |     S1|-0.1|          0.2|
        |     S2| 1.0|          0.1|
        |     S2| 1.0|          0.1|
        +-------+----+-------------+
        <BLANKLINE>

        **This method outputs one value per study**

        >>> gc_lambda = f.round("gc_lambda", 2).alias("gc_lambda")
        >>> gc_lambda_check(df).select("studyId", gc_lambda).show()
        +-------+---------+
        |studyId|gc_lambda|
        +-------+---------+
        |     S1|     0.55|
        |     S2|   219.81|
        +-------+---------+
        <BLANKLINE>
    """
    z_score = f.col("beta") / f.col("standardError")
    # NOTE! The statistic can be calculated using scipy.stats.chi2.isf(0.5, df=1)
    # as well, since both functions are equal at the 0.5 quantile.
    stat = chi2.ppf(0.5, df=1)
    qc_c = (
        gwas_for_qc.select("studyId", "beta", "standardError")
        .withColumn("Z2", z_score**2)
        .groupBy("studyId")
        .agg(f.percentile_approx("Z2", 0.5).alias("z2_median"))
        .withColumn("gc_lambda", f.col("z2_median") / stat)
        .select("studyId", "gc_lambda")
    )

    return qc_c


def p_z_test(gwas_for_qc: DataFrame) -> DataFrame:
    """The P-Z test for QC of GWAS summary statistics.

    This function expects to have a dataframe with `studyId`, `beta`, `standardError`, `pValueMantissa` and `pValueExponent` columns

    It runs linear regression between reported p-values and p-values inferred from z-scores.

    Args:
        gwas_for_qc (DataFrame): Dataframe with `studyId`, `beta`, `standardError`, `pValueMantissa` and `pValueExponent` columns.

    Returns:
        DataFrame: PySpark DataFrame with the results of the linear regression for each study.

    Note:
        The algorithm does following things:
        1. recalculates the negative logarithm of p-value from the square z-score
        2. calculates the mean and se difference between the sum of logarithms derived form reported p-value mantissa and exponent and value recalculated from z-score.

    Warning:
         The function requires the calculation of the **chi-squared survival function** to obtain the p-value from the z-score.
         which is not available in PySpark. The function uses scipy instead, thus **it is not optimized for large datasets**.

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

        **This method outputs two values per study, mean and standard deviation of the difference between log p-value(s)**

        >>> mean_diff_pz = f.round("mean_diff_pz", 2).alias("mean_diff_pz")
        >>> se_diff_pz = f.round("se_diff_pz", 2).alias("se_diff_pz")
        >>> p_z_test(df).select('studyId', mean_diff_pz, se_diff_pz).show()
        +-------+------------+----------+
        |studyId|mean_diff_pz|se_diff_pz|
        +-------+------------+----------+
        |     S1|        0.47|      0.45|
        |     S2|      -21.47|      0.49|
        +-------+------------+----------+
        <BLANKLINE>
    """
    qc_c = (
        gwas_for_qc.withColumn("Z2", (f.col("beta") / f.col("standardError")) ** 2)
        .filter(f.col("Z2") <= 100)
        .withColumn("neglogpValFromZScore", neglogpval_from_z2(f.col("Z2")))
        .withColumn(
            "neglogpVal", -1 * (f.log10("pValueMantissa") + f.col("pValueExponent"))
        )
        .withColumn("diffpval", f.col("neglogpVal") - f.col("neglogpValfromZScore"))
        .groupBy("studyId")
        .agg(
            f.mean("diffpval").alias("mean_diff_pz"),
            f.stddev("diffpval").alias("se_diff_pz"),
        )
        .select("studyId", "mean_diff_pz", "se_diff_pz")
    )

    return qc_c


def mean_beta_check(
    gwas_for_qc: DataFrame,
) -> DataFrame:
    """The mean beta check for QC of GWAS summary statistics.

    This function expects to have a dataframe with `studyId` and `beta` columns and
    outputs the dataframe with mean beta aggregated over the studyId.

    Args:
        gwas_for_qc (DataFrame): Dataframe with `studyId` and `beta` columns.

    Returns:
        DataFrame: PySpark DataFrame with the mean beta for each study.

    Examples:
        >>> s = "studyId STRING, variantId STRING, beta DOUBLE"
        >>> df = spark.createDataFrame([('S1', '1_10000_A_T', 1.0), ('S1', '1_10001_C_T', 1.0), ('S2', '1_10001_C_T', 0.028)], schema=s)
        >>> df.show()
        +-------+-----------+-----+
        |studyId|  variantId| beta|
        +-------+-----------+-----+
        |     S1|1_10000_A_T|  1.0|
        |     S1|1_10001_C_T|  1.0|
        |     S2|1_10001_C_T|0.028|
        +-------+-----------+-----+
        <BLANKLINE>

        **This method outputs one value per study**

        >>> mean_beta = f.round("mean_beta", 3).alias("mean_beta")
        >>> mean_beta_check(df).select('studyId', mean_beta).show()
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
        gwas_for_qc (DataFrame): Dataframe with `studyId`, `beta`, `standardError`, and `effectAlleleFrequencyFromSource` columns.
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
        |     S1| 1.0|          0.1|                            0.5|
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
    beta = f.col("beta")
    varG = genotypic_variance(af)
    pheno_var = (se**2) * n_total * varG + ((f.col("beta") ** 2) * varG)

    window = Window.partitionBy("studyId").orderBy("studyId")
    pheno_median = f.percentile_approx(pheno_var, 0.5).over(window)
    N_hat_ratio = (pheno_median - (beta**2 * varG)) / (se**2 * varG * n_total)

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
    qc_c = (
        gwas_df.withColumn("pheno_var", pheno_var)
        .withColumn("pheno_median", pheno_median)
        .withColumn("N_hat_ratio", N_hat_ratio)
        .groupBy("studyId")
        .agg(f.stddev("N_hat_ratio").alias("se_N"))
        .select("studyId", "se_N")
    )

    return qc_c


def number_of_variants(
    gwas_for_qc: DataFrame,
    pval_threshold: float = 5e-8,
) -> DataFrame:
    """The function calculates number of SNPs and number of SNPs with p-value less than the threshold (default to 5e-8).

    Args:
        gwas_for_qc (DataFrame): Dataframe with `studyId`, `variantId`, `pValueMantissa` and `pValueExponent` columns.
        pval_threshold (float): The threshold for the p-value.

    Returns:
        DataFrame: PySpark DataFrame with the number of SNPs and number of SNPs with p-value less than threshold.

    Examples:
        >>> s = 'studyId STRING, variantId STRING, pValueMantissa FLOAT, pValueExponent INTEGER'
        >>> d1 = [("S1", "1_10000_A_T", 9.9, -20), ("S1", "1_10001_C_T", 1.0, -1), ("S1", "1_10002_G_C", 5.0, -8)]
        >>> d2 = [("S2", "1_10001_C_T", 1.0, -1), ("S2", "1_10002_G_C", 2.0, -2)]
        >>> df = spark.createDataFrame(d1 + d2, s)
        >>> df.show()
        +-------+-----------+--------------+--------------+
        |studyId|  variantId|pValueMantissa|pValueExponent|
        +-------+-----------+--------------+--------------+
        |     S1|1_10000_A_T|           9.9|           -20|
        |     S1|1_10001_C_T|           1.0|            -1|
        |     S1|1_10002_G_C|           5.0|            -8|
        |     S2|1_10001_C_T|           1.0|            -1|
        |     S2|1_10002_G_C|           2.0|            -2|
        +-------+-----------+--------------+--------------+
        <BLANKLINE>

        **This method outputs two values per study `n_variants_sig` and `n_variants`**

        >>> number_of_variants(df).show()
        +-------+----------+--------------+
        |studyId|n_variants|n_variants_sig|
        +-------+----------+--------------+
        |     S1|         3|             2|
        |     S2|         2|             0|
        +-------+----------+--------------+
        <BLANKLINE>
    """
    log10pval = f.log10(f.col("pValueMantissa")) + f.col("pValueExponent")
    threshold = f.log10(f.lit(pval_threshold))
    snp_counts = gwas_for_qc.groupBy("studyId").agg(
        f.count("*").alias("n_variants"),
        f.sum((log10pval <= threshold).cast(t.IntegerType())).alias("n_variants_sig"),
    )
    return snp_counts
