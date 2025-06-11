"""Common functions in the Genetics datasets."""

from __future__ import annotations

import sys
from math import floor, log10
from typing import TYPE_CHECKING

import hail as hl
import numpy as np
from pyspark.sql import functions as f
from pyspark.sql import types as t
from scipy.stats import chi2

from gentropy.common.spark_helpers import calculate_neglog_pvalue, pvalue_to_zscore

if TYPE_CHECKING:
    from hail.table import Table
    from numpy.typing import NDArray
    from pyspark.sql import Column


def calculate_confidence_interval(
    pvalue_mantissa: Column,
    pvalue_exponent: Column,
    beta: Column,
    standard_error: Column,
) -> tuple[Column, Column]:
    """Calculate the confidence interval for the effect based on the p-value and the effect size.

    If the standard error already available, don't re-calculate from p-value.

    Args:
        pvalue_mantissa (Column): p-value mantissa (float)
        pvalue_exponent (Column): p-value exponent (integer)
        beta (Column): effect size in beta (float)
        standard_error (Column): standard error.

    Returns:
        tuple[Column, Column]: betaConfidenceIntervalLower (float), betaConfidenceIntervalUpper (float)

    Examples:
        >>> df = spark.createDataFrame([
        ...     (2.5, -10, 0.5, 0.2),
        ...     (3.0, -5, 1.0, None),
        ...     (1.5, -8, -0.2, 0.1)
        ...     ], ["pvalue_mantissa", "pvalue_exponent", "beta", "standard_error"]
        ... )
        >>> df.select("*", *calculate_confidence_interval(f.col("pvalue_mantissa"), f.col("pvalue_exponent"), f.col("beta"), f.col("standard_error"))).show()
        +---------------+---------------+----+--------------+---------------------------+---------------------------+
        |pvalue_mantissa|pvalue_exponent|beta|standard_error|betaConfidenceIntervalLower|betaConfidenceIntervalUpper|
        +---------------+---------------+----+--------------+---------------------------+---------------------------+
        |            2.5|            -10| 0.5|           0.2|        0.10799999999999998|                      0.892|
        |            3.0|             -5| 1.0|          NULL|         0.5303663900832607|         1.4696336099167393|
        |            1.5|             -8|-0.2|           0.1|                     -0.396|       -0.00400000000000...|
        +---------------+---------------+----+--------------+---------------------------+---------------------------+
        <BLANKLINE>
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
    z_score_095 = 1.96
    ci_lower = (beta - z_score_095 * standard_error).alias(
        "betaConfidenceIntervalLower"
    )
    ci_upper = (beta + z_score_095 * standard_error).alias(
        "betaConfidenceIntervalUpper"
    )

    return (ci_lower, ci_upper)


def normalise_gwas_statistics(
    beta: Column,
    odds_ratio: Column,
    standard_error: Column,
    ci_upper: Column,
    ci_lower: Column,
) -> list[Column]:
    """Harmonizes effect and standard error to beta.

    Args:
        beta (Column): Effect in beta
        odds_ratio (Column): Effect in odds ratio
        standard_error (Column): Standard error of the effect
        ci_upper (Column): Upper bound of the confidence interval
        ci_lower (Column): Lower bound of the confidence interval

    Returns:
        list[Column]: beta, standard error

    Examples:
        >>>
        >>> x1 = (0.1, 1.1, 0.1, None, None) # keep beta, keep std error
        >>> x2 = (None, 1.1, 0.1, None, None) # convert odds ratio to beta, keep std error
        >>> x3 = (None, 1.1, None, 1.30, 0.90) # convert odds ratio to beta, convert ci to standard error
        >>> x4 = (0.1, 1.1, None, 1.30, 0.90) # keep beta, convert ci to standard error
        >>> data = [x1, x2, x3, x4]

        >>> schema = "beta FLOAT, oddsRatio FLOAT, standardError FLOAT, ci_upper FLOAT, ci_lower FLOAT"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show()
        +----+---------+-------------+--------+--------+
        |beta|oddsRatio|standardError|ci_upper|ci_lower|
        +----+---------+-------------+--------+--------+
        | 0.1|      1.1|          0.1|    NULL|    NULL|
        |NULL|      1.1|          0.1|    NULL|    NULL|
        |NULL|      1.1|         NULL|     1.3|     0.9|
        | 0.1|      1.1|         NULL|     1.3|     0.9|
        +----+---------+-------------+--------+--------+
        <BLANKLINE>


        >>> beta = f.col("beta")
        >>> odds_ratio = f.col("oddsRatio")
        >>> se = f.col("standardError")
        >>> ci_upper = f.col("ci_upper")
        >>> ci_lower = f.col("ci_lower")
        >>> cols = normalise_gwas_statistics(beta, odds_ratio, se, ci_upper, ci_lower)
        >>> beta_computed = f.round(cols[0], 2).alias("beta")
        >>> standard_error_computed = f.round(cols[1], 2).alias("standardError")
        >>> df.select(beta_computed, standard_error_computed).show()
        +----+-------------+
        |beta|standardError|
        +----+-------------+
        | 0.1|          0.1|
        | 0.1|         NULL|
        | 0.1|         0.09|
        | 0.1|         0.09|
        +----+-------------+
        <BLANKLINE>
    """
    # We keep standard error when effect is given in beta, otherwise drop.
    # In case the beta is not present, but we have the odds ratio, we assume that
    # we can calculate the standard error from the confidence interval.
    standard_error = (
        f.when(standard_error.isNotNull() & beta.isNotNull(), standard_error)
        .when(
            odds_ratio.isNotNull() & ci_lower.isNotNull() & ci_upper.isNotNull(),
            se_from_ci(ci_upper, ci_lower),
        )
        .otherwise(f.lit(None))
        .alias("standardError")
    )

    # Odds ratio is converted to beta:
    beta = (
        f.when(beta.isNotNull(), beta)
        .when(odds_ratio.isNotNull(), f.log(odds_ratio))
        .alias("beta")
    )

    return [beta, standard_error]


def parse_pvalue(pv: Column) -> list[Column]:
    """This function takes a p-value string and returns two columns mantissa (float), exponent (integer).

    Args:
        pv (Column): P-value as string

    Returns:
        list[Column]: pValueMantissa (float), pValueExponent (integer)

    Examples:
        >>> d = [("0.01",),("4.2E-45",),("43.2E5",),("0",),("1",)]
        >>> spark.createDataFrame(d, ['pval']).select('pval',*parse_pvalue(f.col('pval'))).show()
        +-------+--------------+--------------+
        |   pval|pValueMantissa|pValueExponent|
        +-------+--------------+--------------+
        |   0.01|           1.0|            -2|
        |4.2E-45|           4.2|           -45|
        | 43.2E5|          43.2|             5|
        |      0|         2.225|          -308|
        |      1|           1.0|             0|
        +-------+--------------+--------------+
        <BLANKLINE>
    """
    # Making sure there's a number in the string:
    pv = f.when(
        pv == f.lit("0"), f.lit(sys.float_info.min).cast(t.StringType())
    ).otherwise(pv)

    # Get exponent:
    exponent = f.when(
        f.upper(pv).contains("E"),
        f.split(f.upper(pv), "E").getItem(1),
    ).otherwise(f.floor(f.log10(pv)))

    # Get mantissa:
    mantissa = f.when(
        f.upper(pv).contains("E"),
        f.split(f.upper(pv), "E").getItem(0),
    ).otherwise(pv / (10**exponent))

    # Round value:
    mantissa = f.round(mantissa, 3)

    return [
        mantissa.cast(t.FloatType()).alias("pValueMantissa"),
        exponent.cast(t.IntegerType()).alias("pValueExponent"),
    ]


def _liftover_loci(
    variant_index: Table, chain_path: str, dest_reference_genome: str
) -> Table:
    """Liftover a Hail table containing variant information from GRCh37 to GRCh38 or viceversa.

    Args:
        variant_index (Table): Variants to be lifted over
        chain_path (str): Path to chain file for liftover
        dest_reference_genome (str): Destination reference genome. It can be either GRCh37 or GRCh38.

    Returns:
        Table: LD variant index with coordinates in the new reference genome
    """
    if not hl.get_reference("GRCh37").has_liftover(
        "GRCh38"
    ):  # True when a chain file has already been registered
        rg37 = hl.get_reference("GRCh37")
        rg38 = hl.get_reference("GRCh38")
        if dest_reference_genome == "GRCh38":
            rg37.add_liftover(chain_path, rg38)
        elif dest_reference_genome == "GRCh37":
            rg38.add_liftover(chain_path, rg37)
    # Dynamically create the new field with transmute
    new_locus = f"locus_{dest_reference_genome}"
    return variant_index.transmute(
        **{new_locus: hl.liftover(variant_index.locus, dest_reference_genome)}
    )


def split_pvalue(pvalue: float) -> tuple[float, int]:
    """Convert a float to 10 based exponent and mantissa.

    Args:
        pvalue (float): p-value

    Returns:
        tuple[float, int]: Tuple with mantissa and exponent

    Raises:
        ValueError: If p-value is not between 0 and 1

    Examples:
        >>> split_pvalue(0.00001234)
        (1.234, -5)

        >>> split_pvalue(1)
        (1.0, 0)

        >>> split_pvalue(0.123)
        (1.23, -1)

        >>> split_pvalue(0.99)
        (9.9, -1)
    """
    if pvalue < 0.0 or pvalue > 1.0:
        raise ValueError("P-value must be between 0 and 1")

    exponent = floor(log10(pvalue)) if pvalue != 0 else 0
    mantissa = round(pvalue / 10**exponent, 3)
    return (mantissa, exponent)


def parse_efos(efo_uri: Column) -> Column:
    """Extracting EFO identifiers.

    This function parses EFO identifiers from a comma-separated list of EFO URIs.

    Args:
        efo_uri (Column): column with a list of EFO URIs

    Returns:
        Column: column with a sorted list of parsed EFO IDs

    Examples:
        >>> d = [("http://www.ebi.ac.uk/efo/EFO_0000001,http://www.ebi.ac.uk/efo/EFO_0000002",)]
        >>> df = spark.createDataFrame(d).toDF("efos")
        >>> df.withColumn("efos_parsed", parse_efos(f.col("efos"))).show(truncate=False)
        +-------------------------------------------------------------------------+--------------------------+
        |efos                                                                     |efos_parsed               |
        +-------------------------------------------------------------------------+--------------------------+
        |http://www.ebi.ac.uk/efo/EFO_0000001,http://www.ebi.ac.uk/efo/EFO_0000002|[EFO_0000001, EFO_0000002]|
        +-------------------------------------------------------------------------+--------------------------+
        <BLANKLINE>

    """
    colname = efo_uri._jc.toString()
    return f.array_sort(f.expr(f"regexp_extract_all(`{colname}`, '([A-Z]+_[0-9]+)')"))


def get_logsum(arr: NDArray[np.float64]) -> float:
    """Calculates logarithm of the sum of exponentials of a vector. The max is extracted to ensure that the sum is not Inf.

    This function emulates scipy's logsumexp expression.

    Args:
        arr (NDArray[np.float64]): input array

    Returns:
        float: logsumexp of the input array

    Example:
        >>> l = [0.2, 0.1, 0.05, 0]
        >>> round(get_logsum(l), 6)
        1.476557
    """
    themax = np.max(arr)
    result = themax + np.log(np.sum(np.exp(arr - themax)))
    return float(result)


def access_gcp_secret(secret_id: str, project_id: str) -> str:
    """Access GCP secret manager to get the secret value.

    Args:
        secret_id (str): ID of the secret
        project_id (str): ID of the GCP project

    Returns:
        str: secret value
    """
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def copy_to_gcs(source_path: str, destination_blob: str) -> None:
    """Copy a file to a Google Cloud Storage bucket.

    Args:
        source_path (str): Path to the local file to copy
        destination_blob (str): GS path to the destination blob in the GCS bucket

    Raises:
        ValueError: If the path is a directory
    """
    import os
    from urllib.parse import urlparse

    from google.cloud import storage

    if os.path.isdir(source_path):
        raise ValueError("Path should be a file, not a directory.")
    client = storage.Client()
    bucket = client.bucket(bucket_name=urlparse(destination_blob).hostname)
    blob = bucket.blob(blob_name=urlparse(destination_blob).path.lstrip("/"))
    blob.upload_from_filename(source_path)


def extract_chromosome(variant_id: Column) -> Column:
    """Extract chromosome from variant ID.

    This function extracts the chromosome from a variant ID. The variantId is expected to be in the format `chromosome_position_ref_alt`.
    The function does not convert the GENCODE to Ensembl chromosome notation.
    See https://genome.ucsc.edu/FAQ/FAQgenes.html#:~:text=maps%20only%20once.-,The%20differences,-Some%20of%20our

    Args:
        variant_id (Column): Variant ID

    Returns:
        Column: Chromosome

    Examples:
        >>> d = [("chr1_12345_A_T",),("15_KI270850v1_alt_48777_C_T",),]
        >>> df = spark.createDataFrame(d).toDF("variantId")
        >>> df.withColumn("chromosome", extract_chromosome(f.col("variantId"))).show(truncate=False)
        +---------------------------+-----------------+
        |variantId                  |chromosome       |
        +---------------------------+-----------------+
        |chr1_12345_A_T             |chr1             |
        |15_KI270850v1_alt_48777_C_T|15_KI270850v1_alt|
        +---------------------------+-----------------+
        <BLANKLINE>

    """
    return f.regexp_extract(variant_id, r"^(.*)_\d+_.*$", 1)


def extract_position(variant_id: Column) -> Column:
    """Extract position from variant ID.

    This function extracts the position from a variant ID. The variantId is expected to be in the format `chromosome_position_ref_alt`.

    Args:
        variant_id (Column): Variant ID

    Returns:
        Column: Position

    Examples:
        >>> d = [("chr1_12345_A_T",),("15_KI270850v1_alt_48777_C_T",),]
        >>> df = spark.createDataFrame(d).toDF("variantId")
        >>> df.withColumn("position", extract_position(f.col("variantId"))).show(truncate=False)
        +---------------------------+--------+
        |variantId                  |position|
        +---------------------------+--------+
        |chr1_12345_A_T             |12345   |
        |15_KI270850v1_alt_48777_C_T|48777   |
        +---------------------------+--------+
        <BLANKLINE>

    """
    return f.regexp_extract(variant_id, r"^.*_(\d+)_.*$", 1)


def neglogpval_from_z2(z2: float) -> float:
    """Calculate negative log10 of p-value from squared Z-score following chi2 distribution.

    **The Z-score^2 is equal to the chi2 with 1 degree of freedom.**

    Args:
        z2 (float): Z-score squared.

    Returns:
        float:  negative log of p-value.

    Examples:
        >>> round(neglogpval_from_z2(1.0),2)
        0.5

        >>> round(neglogpval_from_z2(2000),2)
        436.02
    """
    MAX_EXACT_Z2 = 1400
    APPROX_A = 1.4190
    APPROX_B = 0.2173
    if z2 <= MAX_EXACT_Z2:
        logpval = -np.log10(chi2.sf((z2), 1))
        return float(logpval)
    else:
        return APPROX_A + APPROX_B * z2


def chi2_from_pvalue(p_value_mantissa: Column, p_value_exponent: Column) -> Column:
    """Calculate chi2 from p-value.

    This function calculates the chi2 value from the p-value mantissa and exponent.
    In case the p-value is very small (exponent < -300), it uses an approximation based on a linear regression model.
    The approximation is based on the formula: -5.367 * neglog_pval + 4.596, where neglog_pval is the negative log10 of the p-value mantissa.


    Args:
        p_value_mantissa (Column): Mantissa of the p-value (float)
        p_value_exponent (Column): Exponent of the p-value (integer)

    Returns:
        Column: Chi2 value (float)

    Examples:
        >>> data = [(5.0, -8), (9.0, -300), (9.0, -301)]
        >>> schema = "pValueMantissa FLOAT, pValueExponent INT"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show()
        +--------------+--------------+
        |pValueMantissa|pValueExponent|
        +--------------+--------------+
        |           5.0|            -8|
        |           9.0|          -300|
        |           9.0|          -301|
        +--------------+--------------+
        <BLANKLINE>

        >>> mantissa = f.col("pValueMantissa")
        >>> exponent = f.col("pValueExponent")
        >>> chi2 = f.round(chi2_from_pvalue(mantissa, exponent), 2).alias("chi2")
        >>> df2 = df.select(mantissa, exponent, chi2)
        >>> df2.show()
        +--------------+--------------+-------+
        |pValueMantissa|pValueExponent|   chi2|
        +--------------+--------------+-------+
        |           5.0|            -8|  29.72|
        |           9.0|          -300|1369.48|
        |           9.0|          -301|1373.64|
        +--------------+--------------+-------+
        <BLANKLINE>
    """
    neglog_pval = calculate_neglog_pvalue(p_value_mantissa, p_value_exponent)
    p_value = p_value_mantissa * f.pow(10, p_value_exponent)
    neglog_approximation_intercept = f.lit(-5.367)
    neglog_approximation_coeff = f.lit(4.596)
    neglog_approx = (
        neglog_pval * neglog_approximation_coeff + neglog_approximation_intercept
    ).cast(t.DoubleType())
    chi2_udf = f.udf(lambda x: float(chi2.isf(x, df=1)), t.FloatType())

    return (
        f.when(p_value_exponent < f.lit(-300), neglog_approx)
        .otherwise(chi2_udf(p_value))
        .alias("chi2")
    )


def stderr_from_pvalue(chi2_col: Column, beta: Column) -> Column:
    """Calculate standard error from chi2 and beta.

    This function calculates the standard error from the chi2 value and beta.

    Args:
        chi2_col (Column): Chi2 value (float)
        beta (Column): Beta value (float)

    Returns:
        Column: Standard error (float)

    Examples:
        >>> data = [(29.72, 3.0), (3.84, 1.0)]
        >>> schema = "chi2 FLOAT, beta FLOAT"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show()
        +-----+----+
        | chi2|beta|
        +-----+----+
        |29.72| 3.0|
        | 3.84| 1.0|
        +-----+----+
        <BLANKLINE>

        >>> chi2_col = f.col("chi2")
        >>> beta = f.col("beta")
        >>> standard_error = f.round(stderr_from_pvalue(chi2_col, beta), 2).alias("standardError")
        >>> df2 = df.select(chi2_col, beta, standard_error)
        >>> df2.show()
        +-----+----+-------------+
        | chi2|beta|standardError|
        +-----+----+-------------+
        |29.72| 3.0|         0.55|
        | 3.84| 1.0|         0.51|
        +-----+----+-------------+
        <BLANKLINE>

    """
    return (f.abs(beta) / f.sqrt(chi2_col)).alias("standardError")


def se_from_ci(ci_upper: Column, ci_lower: Column) -> Column:
    """Calculate standard error from confidence interval.

    This function calculates the standard error from the confidence interval upper and lower bounds.

    Args:
        ci_upper (Column): Upper bound of the confidence interval (float)
        ci_lower (Column): Lower bound of the confidence interval (float)

    Returns:
        Column: Standard error (float)

    Examples:
        >>> data = [(0.5, 0.1), (1.0, 0.5)]
        >>> schema = "ci_upper FLOAT, ci_lower FLOAT"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show()
        +--------+--------+
        |ci_upper|ci_lower|
        +--------+--------+
        |     0.5|     0.1|
        |     1.0|     0.5|
        +--------+--------+
        <BLANKLINE>

        >>> ci_upper = f.col("ci_upper")
        >>> ci_lower = f.col("ci_lower")
        >>> standard_error = f.round(se_from_ci(ci_upper, ci_lower), 2).alias("standardError")
        >>> df2 = df.select(ci_upper, ci_lower, standard_error)
        >>> df2.show()
        +--------+--------+-------------+
        |ci_upper|ci_lower|standardError|
        +--------+--------+-------------+
        |     0.5|     0.1|         0.41|
        |     1.0|     0.5|         0.18|
        +--------+--------+-------------+
        <BLANKLINE>
    """
    return ((f.log(ci_upper) - f.log(ci_lower)) / (2 * 1.96)).alias("standardError")
