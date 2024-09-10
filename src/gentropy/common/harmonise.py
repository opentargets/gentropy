"""Variant harmonisation utilities."""

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import DataFrame, SparkSession

from gentropy.common.spark_helpers import neglog_pvalue_to_mantissa_and_exponent


def harmonise_summary_stats(
    spark: SparkSession,
    raw_summary_stats_path: str,
    tmp_variant_annotation_path: str,
    chromosome: str,
    colname_position: str,
    colname_allele0: str,
    colname_allele1: str,
    colname_a1freq: str,
    colname_info: str,
    colname_beta: str,
    colname_se: str,
    colname_mlog10p: str,
    colname_n: str,
) -> DataFrame:
    """Ingest and harmonise the summary stats.

    1. Rename chromosome 23 to X.
    2. Filter out low INFO rows.
    3. Filter out low frequency rows.
    4. Assign variant types.
    5. Create variant ID for joining the variant annotation dataset.
    6. Join with the Variant Annotation dataset.
    7. Drop bad quality variants.

    Args:
        spark (SparkSession): Spark session object.
        raw_summary_stats_path (str): Input raw summary stats path.
        tmp_variant_annotation_path (str): Input variant annotation dataset path.
        chromosome (str): Which chromosome to process.
        colname_position (str): Column name for position.
        colname_allele0 (str): Column name for allele0.
        colname_allele1 (str): Column name for allele1.
        colname_a1freq (str): Column name for allele1 frequency (optional).
        colname_info (str): Column name for INFO, reflecting variant quality (optional).
        colname_beta (str): Column name for beta.
        colname_se (str): Column name for beta standard error.
        colname_mlog10p (str): Column name for -log10(p).
        colname_n (str): Column name for the number of samples.

    Returns:
        DataFrame: A harmonised summary stats dataframe.
    """
    # Read the precomputed variant annotation dataset.
    va_df = (
        spark
        .read
        .parquet(tmp_variant_annotation_path)
        .filter(f.col("vaChromosome") == ("X" if chromosome == "23" else chromosome))
        .persist()
    )

    # Read and process the summary stats dataset.
    df = (
        spark
        .read
        .parquet(raw_summary_stats_path)
        .filter(f.col("chromosome") == chromosome)
        # Harmonise, 1: Rename chromosome 23 to X.
        .withColumn(
            "chromosome",
            f.when(
                f.col("chromosome") == "23", "X"
            ).otherwise(f.col("chromosome"))
        )
        # Harmonise, 2: Filter out low INFO rows.
        .filter(f.col(colname_info) >= 0.8)
        # Harmonise, 3: Filter out low frequency rows.
        .withColumn(
            "MAF",
            f.when(f.col(colname_a1freq) < 0.5, f.col(colname_a1freq))
            .otherwise(1 - f.col(colname_a1freq))
        )
        .filter(f.col("MAF") >= 0.0001)
        .drop("MAF")
        # Harmonise, 4: Assign variant types.
        .withColumn(
            "variant_type",
            f.when(
                (f.length(colname_allele0) == 1) & (f.length(colname_allele1) == 1),
                f.when(
                    ((f.col(colname_allele0) == "A") & (f.col(colname_allele1) == "T")) |
                    ((f.col(colname_allele0) == "T") & (f.col(colname_allele1) == "A")) |
                    ((f.col(colname_allele0) == "G") & (f.col(colname_allele1) == "C")) |
                    ((f.col(colname_allele0) == "C") & (f.col(colname_allele1) == "G")),
                    "snp_c"
                )
                .otherwise(
                    "snp_n"
                )
            )
            .otherwise(
                "indel"
            )
        )
        # Harmonise, 5: Create variant ID for joining the variant annotation dataset.
        .withColumn(
            colname_position,
            f.col(colname_position).cast("integer")
        )
        .withColumn(
            "summary_stats_id",
            f.concat_ws(
                "_",
                f.col("chromosome"),
                f.col(colname_position),
                f.col(colname_allele0),
                f.col(colname_allele1)
            )
        )
    )
    # Harmonise, 6: Join with the Variant Annotation dataset.
    df = (
        df
        .join(va_df, (df["chromosome"] == va_df["vaChromosome"]) & (df["summary_stats_id"] == va_df["summary_stats_id"]), "inner")
        .drop("vaChromosome", "summary_stats_id")
        .withColumn(
            "effectAlleleFrequencyFromSource",
            f.when(
                f.col("direction") == "direct",
                f.col(colname_a1freq).cast("float")
            ).otherwise(1 - f.col(colname_a1freq).cast("float"))
        )
        .withColumn(
            "beta",
            f.when(
                f.col("direction") == "direct",
                f.col(colname_beta).cast("double")
            ).otherwise(-f.col(colname_beta).cast("double"))
        )
    )
    df = (
        # Harmonise, 7: Drop bad quality variants.
        df
        .filter(
            ~ ((f.col("variant_type") == "snp_c") & (f.col("direction") == "flip"))
        )
    )

    # Prepare the fields according to schema.
    df = (
        df
        .select(
            f.col("studyId"),
            f.col("chromosome"),
            f.col("variantId"),
            f.col("beta"),
            f.col(colname_position).cast(t.IntegerType()).alias("position"),
            # Parse p-value into mantissa and exponent.
            *neglog_pvalue_to_mantissa_and_exponent(f.col(colname_mlog10p).cast(t.DoubleType())),
            # Add standard error and sample size information.
            f.col(colname_se).cast("double").alias("standardError"),
            f.col(colname_n).cast("integer").alias("sampleSize"),
        )
        # Drop rows which don't have proper position or beta value.
        .filter(
            f.col("position").cast(t.IntegerType()).isNotNull()
            & (f.col("beta") != 0)
        )
    )

    # Return the dataframe.
    return df
