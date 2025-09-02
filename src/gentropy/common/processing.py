"""Common functions in the Genetics datasets."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.types as t
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from gentropy.common.spark import extract_column_name
from gentropy.common.stats import (
    chi2_from_pvalue,
    pvalue_from_neglogpval,
    stderr_from_chi2_and_effect_size,
)

if TYPE_CHECKING:
    from pyspark.sql import Column

    from gentropy.common.session import Session
    from gentropy.datasource.finngen_ukb_meta.summary_stats import (
        FinngenUkbMetaSummaryStats,
    )
    from gentropy.datasource.ukb_ppp_eur.summary_stats import UkbPppEurSummaryStats

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
    name = extract_column_name(efo_uri)
    return f.array_sort(f.expr(f"regexp_extract_all(`{name}`, '([A-Z]+_[0-9]+)')"))


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
        spark.read.parquet(tmp_variant_annotation_path)
        .filter(f.col("vaChromosome") == ("X" if chromosome == "23" else chromosome))
        .persist()
    )

    # Read and process the summary stats dataset.
    df = (
        spark.read.parquet(raw_summary_stats_path)
        .filter(f.col("chromosome") == chromosome)
        # Harmonise, 1: Rename chromosome 23 to X.
        .withColumn(
            "chromosome",
            f.when(f.col("chromosome") == "23", "X").otherwise(f.col("chromosome")),
        )
    )
    if colname_info:
        # Harmonise, 2: Filter out low INFO rows.
        df = df.filter(f.col(colname_info) >= 0.8)
    if colname_a1freq:
        # Harmonise, 3: Filter out low frequency rows.
        df = (
            df.withColumn(
                "MAF",
                f.when(f.col(colname_a1freq) < 0.5, f.col(colname_a1freq)).otherwise(
                    1 - f.col(colname_a1freq)
                ),
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
                    ((f.col(colname_allele0) == "A") & (f.col(colname_allele1) == "T"))
                    | (
                        (f.col(colname_allele0) == "T")
                        & (f.col(colname_allele1) == "A")
                    )
                    | (
                        (f.col(colname_allele0) == "G")
                        & (f.col(colname_allele1) == "C")
                    )
                    | (
                        (f.col(colname_allele0) == "C")
                        & (f.col(colname_allele1) == "G")
                    ),
                    "snp_c",
                ).otherwise("snp_n"),
            ).otherwise("indel"),
        )
        # Harmonise, 5: Create variant ID for joining the variant annotation dataset.
        .withColumn(colname_position, f.col(colname_position).cast("integer"))
        .withColumn(
            "summary_stats_id",
            f.concat_ws(
                "_",
                f.col("chromosome"),
                f.col(colname_position),
                f.col(colname_allele0),
                f.col(colname_allele1),
            ),
        )
    )
    # Harmonise, 6: Join with the Variant Annotation dataset.
    df = (
        df.join(
            va_df,
            (df["chromosome"] == va_df["vaChromosome"])
            & (df["summary_stats_id"] == va_df["summary_stats_id"]),
            "inner",
        )
        .drop("vaChromosome", "summary_stats_id")
        .withColumn(
            "beta",
            f.when(
                f.col("direction") == "direct", f.col(colname_beta).cast("double")
            ).otherwise(-f.col(colname_beta).cast("double")),
        )
    )
    if colname_a1freq:
        df = df.withColumn(
            "effectAlleleFrequencyFromSource",
            f.when(
                f.col("direction") == "direct", f.col(colname_a1freq).cast("float")
            ).otherwise(1 - f.col(colname_a1freq).cast("float")),
        )
    df = (
        # Harmonise, 7: Drop bad quality variants.
        df.filter(
            ~((f.col("variant_type") == "snp_c") & (f.col("direction") == "flip"))
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
        *pvalue_from_neglogpval(f.col(colname_mlog10p).cast(t.DoubleType())),
        # Add standard error and sample size information.
        f.col(colname_se).cast("double").alias("standardError"),
    ]
    if colname_n:
        select_expr.append(f.col(colname_n).cast("integer").alias("sampleSize"))

    df = (
        df.select(*select_expr)
        # Dropping associations where no harmonized position is available:
        .filter(f.col("position").isNotNull())
        # We are not interested in associations empty beta values:
        .filter(f.col("beta").isNotNull())
        # We are not interested in associations with zero effect:
        .filter(f.col("beta") != 0)
        # Drop rows which don't have proper position or beta value.
    )
    # NOTE! In case the standard error is empty, recompute it from p-value and beta.
    # If we leave the standard error empty for all fields, we will cause the sanity filter
    # to skip all rows.
    # Make sure the beta is non empty before computation.
    computed_chi2 = chi2_from_pvalue(f.col("pValueMantissa"), f.col("pValueExponent"))
    computed_stderr = stderr_from_chi2_and_effect_size(computed_chi2, f.col("beta"))
    df = df.withColumn(
        "standardError", f.coalesce(f.col("standardError"), computed_stderr)
    ).orderBy(f.col("chromosome"), f.col("position"))

    # Return the dataframe.
    return df


def prepare_va(session: Session, variant_annotation_path: str, tmp_variant_annotation_path: str) -> None:
    """Prepare the Variant Annotation dataset for efficient per-chromosome joins.

    Args:
        session (Session): The gentropy session wrapper to be used for reading and writing data.
        variant_annotation_path (str): The path to the input variant annotation dataset.
        tmp_variant_annotation_path (str): The path to store the temporary output for the repartitioned annotation dataset.
    """
    va_df = (
        session
        .spark
        .read
        .parquet(variant_annotation_path)
    )
    va_df_direct = (
        va_df.
        select(
            f.col("chromosome").alias("vaChromosome"),
            f.col("variantId"),
            f.concat_ws(
                "_",
                f.col("chromosome"),
                f.col("position"),
                f.col("referenceAllele"),
                f.col("alternateAllele")
            ).alias("summary_stats_id"),
            f.lit("direct").alias("direction")
        )
    )
    va_df_flip = (
        va_df.
        select(
            f.col("chromosome").alias("vaChromosome"),
            f.col("variantId"),
            f.concat_ws(
                "_",
                f.col("chromosome"),
                f.col("position"),
                f.col("alternateAllele"),
                f.col("referenceAllele")
            ).alias("summary_stats_id"),
            f.lit("flip").alias("direction")
        )
    )
    (
        va_df_direct.union(va_df_flip)
        .coalesce(1)
        .repartition("vaChromosome")
        .write
        .partitionBy("vaChromosome")
        .mode("overwrite")
        .parquet(tmp_variant_annotation_path)
    )


def process_summary_stats_per_chromosome(
        session: Session,
        ingestion_class: type[UkbPppEurSummaryStats] | type[FinngenUkbMetaSummaryStats],
        raw_summary_stats_path: str,
        tmp_variant_annotation_path: str,
        summary_stats_output_path: str,
        study_index_path: str,
    ) -> None:
    """Processes summary statistics for each chromosome, partitioning and writing results.

    Args:
        session (Session): The Gentropy session session to use for distributed data processing.
        ingestion_class (type[UkbPppEurSummaryStats] | type[FinngenUkbMetaSummaryStats]): The class used to handle ingestion of source data. Must have a `from_source` method returning a DataFrame.
        raw_summary_stats_path (str): The path to the raw summary statistics files.
        tmp_variant_annotation_path (str): The path to temporary variant annotation data, used for chromosome joins.
        summary_stats_output_path (str): The output path to write processed summary statistics as parquet files.
        study_index_path (str): The path to study index, which is necessary in some cases to populate the sample size column.
    """
    # Set mode to overwrite for processing the first chromosome.
    write_mode = "overwrite"
    # Chromosome 23 is X, this is handled downstream.
    for chromosome in list(range(1, 24)):
        logging_message = f"  Processing chromosome {chromosome}"
        session.logger.info(logging_message)
        (
            ingestion_class.from_source(
                spark=session.spark,
                raw_summary_stats_path=raw_summary_stats_path,
                tmp_variant_annotation_path=tmp_variant_annotation_path,
                chromosome=str(chromosome),
                study_index_path=study_index_path,
            )
            .df
            .coalesce(1)
            .repartition("studyId", "chromosome")
            .write
            .partitionBy("studyId", "chromosome")
            .mode(write_mode)
            .parquet(summary_stats_output_path)
        )
        # Now that we have written the first chromosome, change mode to append for subsequent operations.
        write_mode = "append"
