"""Common functions in the Genetics datasets."""
from __future__ import annotations

import sys
from math import floor, log10
from typing import TYPE_CHECKING, List, Tuple

import hail as hl
from pyspark.sql import functions as f
from pyspark.sql import types as t

from otg.common.spark_helpers import pvalue_to_zscore

if TYPE_CHECKING:
    from hail.expr.expressions import Int32Expression, StringExpression
    from pyspark.sql import Column


def parse_region(region: str) -> Tuple[str, int, int]:
    """Parse region string to chr:start-end.

    Args:
        region (str): Genomic region expected to follow chr##:#,###-#,### format or ##:####-#####.

    Raises:
        ValueError: If the end and start positions cannot be casted to integer or not all three values value error is raised.

    Returns:
        Tuple[str, int, int]: Chromosome, start position, end position


    Examples
        >>> parse_region('chr6:28,510,120-33,480,577')
        ('6', 28510120, 33480577)
        >>> parse_region('6:28510120-33480577')
        ('6', 28510120, 33480577)
        >>> parse_region('6:28510120')
        Traceback (most recent call last):
            ...
        ValueError: Genomic region should follow the chr##:####-#### format.
        >>> parse_region('6:28510120-foo')
        Traceback (most recent call last):
            ...
        ValueError: Start and the end position of the region has to be integer.
    """
    region = region.replace(":", "-").replace(",", "")
    try:
        (chromosome, start_position, end_position) = region.split("-")
    except ValueError:
        raise ValueError("Genomic region should follow the chr##:####-#### format.")

    try:
        return (chromosome.replace("chr", ""), int(start_position), int(end_position))
    except ValueError:
        raise ValueError("Start and the end position of the region has to be integer.")


def calculate_confidence_interval(
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
        |            2.5|            -10| 0.5|           0.2|                        0.3|                        0.7|
        |            3.0|             -5| 1.0|          null|         0.7603910153486024|         1.2396089846513976|
        |            1.5|             -8|-0.2|           0.1|       -0.30000000000000004|                       -0.1|
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
    ci_lower = (beta - standard_error).alias("betaConfidenceIntervalLower")
    ci_upper = (beta + standard_error).alias("betaConfidenceIntervalUpper")

    return (ci_lower, ci_upper)


def convert_odds_ratio_to_beta(
    beta: Column, odds_ratio: Column, standard_error: Column
) -> List[Column]:
    """Harmonizes effect and standard error to beta.

    Args:
        beta (Column): Effect in beta
        odds_ratio (Column): Effect in odds ratio
        standard_error (Column): Standard error of the effect

    Returns:
        tuple: beta, standard error

    Examples:
        >>> df = spark.createDataFrame([{"beta": 0.1, "oddsRatio": 1.1, "standardError": 0.1}, {"beta": None, "oddsRatio": 1.1, "standardError": 0.1}, {"beta": 0.1, "oddsRatio": None, "standardError": 0.1}, {"beta": 0.1, "oddsRatio": 1.1, "standardError": None}])
        >>> df.select("*", *convert_odds_ratio_to_beta(f.col("beta"), f.col("oddsRatio"), f.col("standardError"))).show()
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

    return [beta, standard_error]


def parse_pvalue(pv: Column) -> List[Column]:
    """This function takes a p-value string and returns two columns mantissa (float), exponent (integer).

    Args:
        pv (Column): P-value as string

    Returns:
        Column: p-value mantissa (float)
        Column: p-value exponent (integer)

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


def convert_gnomad_position_to_ensembl(
    position: Column, reference: Column, alternate: Column
) -> Column:
    """Converting GnomAD variant position to Ensembl variant position.

    For indels (the reference or alternate allele is longer than 1), then adding 1 to the position, for SNPs, the position is unchanged.
    More info about the problem: https://www.biostars.org/p/84686/

    Args:
        position (Column): Column
        reference (Column): The reference allele.
        alternate (Column): The alternate allele

    Returns:
        The position of the variant in the Ensembl genome.

    Examples:
        >>> d = [(1, "A", "C"), (2, "AA", "C"), (3, "A", "AA")]
        >>> df = spark.createDataFrame(d).toDF("position", "reference", "alternate")
        >>> df.withColumn("new_position", convert_gnomad_position_to_ensembl(f.col("position"), f.col("reference"), f.col("alternate"))).show()
        +--------+---------+---------+------------+
        |position|reference|alternate|new_position|
        +--------+---------+---------+------------+
        |       1|        A|        C|           1|
        |       2|       AA|        C|           3|
        |       3|        A|       AA|           4|
        +--------+---------+---------+------------+
        <BLANKLINE>

    """
    return f.when(
        (f.length(reference) > 1) | (f.length(alternate) > 1), position + 1
    ).otherwise(position)


def convert_gnomad_position_to_ensembl_hail(
    position: Int32Expression, reference: StringExpression, alternate: StringExpression
) -> Int32Expression:
    """Converting GnomAD variant position to Ensembl variant position in hail table.

    For indels (the reference or alternate allele is longer than 1), then adding 1 to the position, for SNPs, the position is unchanged.
    More info about the problem: https://www.biostars.org/p/84686/

    Args:
        position (Int32Expression): Position of the variant in the GnomAD genome.
        reference (StringExpression): The reference allele.
        alternate (StringExpression): The alternate allele

    Returns:
        The position of the variant according to Ensembl genome.
    """
    return hl.if_else(
        (reference.length() > 1) | (alternate.length() > 1), position + 1, position
    )


def split_pvalue(pvalue: float) -> tuple[float, int]:
    """Function to convert a float to 10 based exponent and mantissa.

    Args:
        pvalue (float): p-value

    Returns:
        tuple[float, int]: Tuple with mantissa and exponent
    """
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
    colname = efo_uri._jc.toString()  # type: ignore
    return f.array_sort(f.expr(f"regexp_extract_all(`{colname}`, '([A-Z]+_[0-9]+)')"))


def get_study_locus_id(study_id_col_name: str, variant_id_col_name: str) -> Column:
    """Hashes a column with a variant ID and a study ID to extract a consistent studyLocusId.

    Args:
        study_id_col_name (str): column name with a study ID
        variant_id_col_name (str): column name with a variant ID

    Returns:
        Column: column with a study locus ID

    Examples:
        >>> df = spark.createDataFrame([("GCST000001", "1_1000_A_C"), ("GCST000002", "1_1000_A_C")]).toDF("studyId", "variantId")
        >>> df.withColumn("study_locus_id", get_study_locus_id(*["variantId", "studyId"])).show()
        +----------+----------+--------------------+
        |   studyId| variantId|      study_locus_id|
        +----------+----------+--------------------+
        |GCST000001|1_1000_A_C| 7437284926964690765|
        |GCST000002|1_1000_A_C|-7653912547667845377|
        +----------+----------+--------------------+
        <BLANKLINE>

    """
    return f.xxhash64(*[study_id_col_name, variant_id_col_name]).alias("studyLocusId")
