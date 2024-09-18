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
    colname_a1freq: str | None,
    colname_info: str | None,
    colname_beta: str,
    colname_se: str,
    colname_mlog10p: str,
    colname_n: str | None,
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
        tmp_variant_annotation_path (str): Path to the Variant Annotation dataset which has been further prepared and processed by the per_chromosome module (previous PR in the chain) to speed up the joins in the harmonisation phase. It includes all variants in both the direct (A0/A1) and reverse (A1/A0) orientations, so that the direction of the variant can be easily determined on joining.
        chromosome (str): Which chromosome to process.
        colname_position (str): Column name for position.
        colname_allele0 (str): Column name for allele0.
        colname_allele1 (str): Column name for allele1.
        colname_a1freq (str | None): Column name for allele1 frequency (optional).
        colname_info (str | None): Column name for INFO, reflecting variant quality (optional).
        colname_beta (str): Column name for beta.
        colname_se (str): Column name for beta standard error.
        colname_mlog10p (str): Column name for -log10(p).
        colname_n (str | None): Column name for the number of samples (optional).

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
    )
    if colname_info:
        # Harmonise, 2: Filter out low INFO rows.
        df = df.filter(f.col(colname_info) >= 0.8)
    if colname_a1freq:
        # Harmonise, 3: Filter out low frequency rows.
        df = (
            df
            .withColumn(
                "MAF",
                f.when(f.col(colname_a1freq) < 0.5, f.col(colname_a1freq))
                .otherwise(1 - f.col(colname_a1freq))
            )
            .filter(f.col("MAF") >= 0.0001)
            .drop("MAF")
        )
    df = (
        df
        # Harmonise, 4: Assign variant types.
        # There are three possible variant types:
        # 1. `snp_c` means an SNP converting a base into its complementary base: A<>T or G><C.
        # 2. `snp_n` means any other SNP where the length of each allele is still exactly 1.
        # 3. `indel` means any other variant where the length of at least one allele is greater than 1.
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
            "beta",
            f.when(
                f.col("direction") == "direct",
                f.col(colname_beta).cast("double")
            ).otherwise(-f.col(colname_beta).cast("double"))
        )
    )
    if colname_a1freq:
        df = (
            df
            .withColumn(
                "effectAlleleFrequencyFromSource",
                f.when(
                    f.col("direction") == "direct",
                    f.col(colname_a1freq).cast("float")
                )
                .otherwise(1 - f.col(colname_a1freq).cast("float"))
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
    select_expr = [
        f.col("studyId"),
        f.col("chromosome"),
        f.col("variantId"),
        f.col("beta"),
        f.col(colname_position).cast(t.IntegerType()).alias("position"),
        # Parse p-value into mantissa and exponent.
        *neglog_pvalue_to_mantissa_and_exponent(f.col(colname_mlog10p).cast(t.DoubleType())),
        # Add standard error and sample size information.
        f.col(colname_se).cast("double").alias("standardError"),
    ]
    if colname_n:
        select_expr.append(f.col(colname_n).cast("integer").alias("sampleSize"))
    df = (
        df
        .select(*select_expr)
        # Drop rows which don't have proper position or beta value.
        .filter(
            f.col("position").cast(t.IntegerType()).isNotNull()
            & (f.col("beta") != 0)
        )
    )

    # Return the dataframe.
    return df
