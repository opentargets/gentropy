"""Common functions in the Genetics datasets."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import Session
from gentropy.common.stats import (
    chi2_from_pvalue,
    pvalue_from_neglogpval,
    stderr_from_chi2_and_effect_size,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession


def parse_efos(efo_uris: Column) -> Column:
    """Extracting EFO identifiers.

    This function parses EFO identifiers from a comma-separated list of EFO URIs.

    Args:
        efo_uris (Column): column with a list of EFO URIs

    Returns:
        Column: column with a sorted and unique list of parsed EFO IDs

    Examples:
        >>> d = [("http://www.ebi.ac.uk/efo/EFO_0000001,http://purl.obolibrary.org/obo/OBA_VT0001253,http://www.orpha.net/ORDO/Orphanet_101953,http://www.ebi.ac.uk/efo/EFO_0000001",)]
        >>> df = spark.createDataFrame(d).toDF("efos")
        >>> df.select(parse_efos(f.col("efos")).alias('col')).show(truncate=False)
        +---------------------------------------------+
        |col                                          |
        +---------------------------------------------+
        |[EFO_0000001, OBA_VT0001253, Orphanet_101953]|
        +---------------------------------------------+
        <BLANKLINE>

    """
    return f.array_distinct(
        f.transform(
            # Splitting colun values to individual URIs:
            f.split(efo_uris, ","),
            # Each URI is further split, and the last component is returned:
            lambda uri: f.element_at(f.split(uri, "/"), -1),
        )
    )


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


def normalize_chromosome(chromosome: Column) -> Column:
    """Normalize chromosome notation.

    This function normalizes chromosome notation by
        1. Removing the "chr" prefix if present.
        2. Converting "M" to "MT".
        3. Converting "23" to "X".
        4. Converting "24" to "Y".

    Args:
        chromosome (Column): Chromosome column

    Returns:
        Column: Normalized chromosome column

    Examples:
        >>> d = [("chr1",),("2",),("chrX",),("Y",),("chrM",),("23",),("24",)]
        >>> df = spark.createDataFrame(data=d, schema="chromosome STRING")
        >>> df.withColumn("normalized_chromosome", normalize_chromosome(f.col("chromosome"))).show(truncate=False)
        +----------+---------------------+
        |chromosome|normalized_chromosome|
        +----------+---------------------+
        |chr1      |1                    |
        |2         |2                    |
        |chrX      |X                    |
        |Y         |Y                    |
        |chrM      |MT                   |
        |23        |X                    |
        |24        |Y                    |
        +----------+---------------------+
        <BLANKLINE>
    """
    chr_str = chromosome.cast(t.StringType())
    ensembl_chr = f.regexp_replace(chr_str, r"^chr", "")
    return (
        f.when(ensembl_chr == "M", "MT")
        .when(ensembl_chr == "23", "X")
        .when(ensembl_chr == "24", "Y")
        .otherwise(ensembl_chr)
    ).alias("chromosome")


def maf(af: Column, scale: int = 10) -> Column:
    """Calculate minor allele frequency from allele frequency.

    Args:
        af (Column): Allele frequency column.
        scale (int): Scale for DecimalType.

    Note:
        the DecimalType is represented by two parameters: precision and scale.
        The precision is the total number of digits that can be stored and the scale
        is the number of digits to the right of the decimal point.

        For AF the value can be only between 0 and 1.0, so we limit the scale to the
        number of digits after the decimal point. The precision is set to scale + 1
        to ensure we can represent values like 1.0 with the given scale.

    Returns:
        Column: Minor allele frequency column.

    Examples:
        >>> data = [("v1", 0.1), ("v2", 0.9), ("v3", None),]
        >>> schema = "variantId STRING, af DOUBLE"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show(truncate=False)
        +---------+----+
        |variantId|af  |
        +---------+----+
        |v1       |0.1 |
        |v2       |0.9 |
        |v3       |NULL|
        +---------+----+
        <BLANKLINE>
        >>> df.withColumn("minorAlleleFrequency", maf(f.col("af"))).show(truncate=False)
        +---------+----+--------------------+
        |variantId|af  |minorAlleleFrequency|
        +---------+----+--------------------+
        |v1       |0.1 |0.1000000000        |
        |v2       |0.9 |0.1000000000        |
        |v3       |NULL|NULL                |
        +---------+----+--------------------+
        <BLANKLINE>
    """
    precision = scale + 1  # to ensure we can represent values like 1.0000
    scaled_af = af.cast(t.DecimalType(precision, scale))
    max_af = f.lit(1.0).cast(t.DecimalType(precision, scale))
    return (
        f.when(af.isNotNull() & (af <= 0.5), scaled_af)
        .when(af.isNotNull(), max_af - scaled_af)
        .otherwise(f.lit(None))
        .alias("minorAlleleFrequency")
    )


def mac(maf: Column, n: Column) -> Column:
    """Calculate minor allele count from minor allele frequency and sample size.

    Args:
        maf (Column): Minor allele frequency column.
        n (Column): Sample size column.

    Returns:
        Column: Minor allele count column.

    Examples:
        >>> data = [("v1", 0.1, 100), ("v2", 0.2, 200), ("v3", None, 150), ("v4", 0.3, None),]
        >>> schema = "variantId STRING, maf DOUBLE, n INT"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show(truncate=False)
        +---------+----+----+
        |variantId|maf |n   |
        +---------+----+----+
        |v1       |0.1 |100 |
        |v2       |0.2 |200 |
        |v3       |NULL|150 |
        |v4       |0.3 |NULL|
        +---------+----+----+
        <BLANKLINE>

        >>> df.withColumn("mac", mac(f.col("maf"), f.col("n"))).show(truncate=False)
        +---------+----+----+----+
        |variantId|maf |n   |mac |
        +---------+----+----+----+
        |v1       |0.1 |100 |20  |
        |v2       |0.2 |200 |80  |
        |v3       |NULL|150 |NULL|
        |v4       |0.3 |NULL|NULL|
        +---------+----+----+----+
        <BLANKLINE>
    """
    return (
        f.when(maf.isNotNull() & n.isNotNull(), (maf * n * 2).cast(t.IntegerType()))
        .otherwise(f.lit(None))
        .alias("minorAlleleCount")
    )


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


def prepare_va(
    session: Session, variant_annotation_path: str, tmp_variant_annotation_path: str
) -> None:
    """Prepare the Variant Annotation dataset for efficient per-chromosome joins.

    Args:
        session (Session): The gentropy session wrapper to be used for reading and writing data.
        variant_annotation_path (str): The path to the input variant annotation dataset.
        tmp_variant_annotation_path (str): The path to store the temporary output for the repartitioned annotation dataset.
    """
    va_df = session.spark.read.parquet(variant_annotation_path)
    va_df_direct = va_df.select(
        f.col("chromosome").alias("vaChromosome"),
        f.col("variantId"),
        f.concat_ws(
            "_",
            f.col("chromosome"),
            f.col("position"),
            f.col("referenceAllele"),
            f.col("alternateAllele"),
        ).alias("summary_stats_id"),
        f.lit("direct").alias("direction"),
    )
    va_df_flip = va_df.select(
        f.col("chromosome").alias("vaChromosome"),
        f.col("variantId"),
        f.concat_ws(
            "_",
            f.col("chromosome"),
            f.col("position"),
            f.col("alternateAllele"),
            f.col("referenceAllele"),
        ).alias("summary_stats_id"),
        f.lit("flip").alias("direction"),
    )
    (
        va_df_direct.union(va_df_flip)
        .coalesce(1)
        .repartition("vaChromosome")
        .write.partitionBy("vaChromosome")
        .mode("overwrite")
        .parquet(tmp_variant_annotation_path)
    )


def flag_star_allele(effect_allele: Column, other_allele: Column) -> Column:
    """Return a predicate that excludes rows containing `*` alleles.

    Args:
        effect_allele (Column): Column representing the effect allele.
        other_allele (Column): Column representing the other allele.

    Returns:
        Column: Boolean predicate that is true when neither allele contains `*`.

    Examples:
        >>> data = [("A", "T"), ("G", "C"), ("*", "A"), ("A", "*")]
        >>> df = spark.createDataFrame(data, ["effectAllele", "otherAllele"])
        >>> df.filter(flag_star_allele(f.col("effectAllele"), f.col("otherAllele"))).show()
        +------------+-----------+
        |effectAllele|otherAllele|
        +------------+-----------+
        |           A|          T|
        |           G|          C|
        +------------+-----------+
        <BLANKLINE>
    """
    return ~effect_allele.contains("*") & ~other_allele.contains("*")


def flag_multiallelics(effect_allele: Column, other_allele: Column) -> Column:
    """Return a predicate that excludes alleles marked as multiallelic with `!`.

    Args:
        effect_allele (Column): Column representing the effect allele.
        other_allele (Column): Column representing the other allele.

    Returns:
        Column: Boolean predicate that is true when neither allele contains `!`.

    Examples:
        >>> data = [("A", "T"), ("G", "C"), ("!", "A"), ("A", "!")]
        >>> df = spark.createDataFrame(data, ["effectAllele", "otherAllele"])
        >>> df.filter(flag_multiallelics(f.col("effectAllele"), f.col("otherAllele"))).show()
        +------------+-----------+
        |effectAllele|otherAllele|
        +------------+-----------+
        |           A|          T|
        |           G|          C|
        +------------+-----------+
        <BLANKLINE>
    """
    return ~effect_allele.contains("!") & ~other_allele.contains("!")


def flag_equal_alleles(effect_allele: Column, other_allele: Column) -> Column:
    """Return a predicate that excludes rows with equal alleles.

    Args:
        effect_allele (Column): Column representing the effect allele.
        other_allele (Column): Column representing the other allele.

    Returns:
        Column: Boolean predicate that is true when the alleles differ.

    Examples:
        >>> data = [("AT", "T"), ("G", "C"), ("A", "A"), ("T", "T")]
        >>> df = spark.createDataFrame(data, ["effectAllele", "otherAllele"])
        >>> df.filter(flag_equal_alleles(f.col("effectAllele"), f.col("otherAllele"))).show()
        +------------+-----------+
        |effectAllele|otherAllele|
        +------------+-----------+
        |          AT|          T|
        |           G|          C|
        +------------+-----------+
        <BLANKLINE>
    """
    return effect_allele != other_allele


def flag_non_atgc_alleles(effect_allele: Column, other_allele: Column) -> Column:
    """Return a predicate requiring both alleles to contain only A/T/G/C bases.

    Args:
        effect_allele (Column): Column representing the effect allele.
        other_allele (Column): Column representing the other allele.

    Returns:
        Column: Boolean predicate that is true when both alleles contain only A/T/G/C.

    Examples:
        >>> data = [("AT", "T"), ("G", "C"), ("N", "A"), ("A", "X")]
        >>> df = spark.createDataFrame(data, ["effectAllele", "otherAllele"])
        >>> df.filter(flag_non_atgc_alleles(f.col("effectAllele"), f.col("otherAllele"))).show()
        +------------+-----------+
        |effectAllele|otherAllele|
        +------------+-----------+
        |          AT|          T|
        |           G|          C|
        +------------+-----------+
        <BLANKLINE>
    """
    return effect_allele.rlike(r"^[ATGC]+$") & other_allele.rlike(r"^[ATGC]+$")


def normalize_af(af: Column, flip: Column) -> Column:
    """Normalize allele frequency based on variant direction.

    Args:
        af (Column): Allele frequency column.
        flip (Column): Direction column indicating if the allele needs to be flipped.

    Returns:
        Column: Normalized allele frequency.

    Examples:
        >>> data = [("v1", 0.1, 1),
        ...       ("v2", 0.2, -1),
        ...       ("v3", None, -1),
        ...       ("V4", 0.1, None),]
        >>> schema = "variantId STRING, af DOUBLE, flip INT"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show(truncate=False)
        +---------+----+----+
        |variantId|af  |flip|
        +---------+----+----+
        |v1       |0.1 |1   |
        |v2       |0.2 |-1  |
        |v3       |NULL|-1  |
        |V4       |0.1 |NULL|
        +---------+----+----+
        <BLANKLINE>

        >>> df = df.withColumn("normalizedAf", normalize_af(f.col("af"), f.col("flip")))
        >>> df.select("variantId", "normalizedAf").show(truncate=False)
        +---------+------------+
        |variantId|normalizedAf|
        +---------+------------+
        |v1       |0.1         |
        |v2       |0.8         |
        |v3       |NULL        |
        |V4       |0.1         |
        +---------+------------+
        <BLANKLINE>
    """
    return f.when((flip == -1) & af.isNotNull(), f.lit(1.0) - af).otherwise(af)


def combined_allele_frequency(
    allele_freq: Column, n_samples_per_cohort: Column
) -> Column:
    """Combined Allele Frequency across all cohorts.

    Args:
        allele_freq (Column): Column containing array of structs with `cohort` and `alleleFrequency` fields.
        n_samples_per_cohort (Column): Column containing array of structs with `cohort` and `nSamples` fields.

    Returns:
        Column: Combined allele frequency across all cohorts.

    Note:
        The combination is made by weighting the allele frequencies by the number of samples in each cohort.

    Examples:
        >>> data = [
        ...    ("v1", [{"cohort": "A", "alleleFrequency": 0.6}, {"cohort": "B", "alleleFrequency": 0.2}, {"cohort": "C", "alleleFrequency": 0.3}],
        ...           [{"cohort": "A", "nSamples": 100}, {"cohort": "B", "nSamples": 200}, {"cohort": "D", "nSamples": 20}]),
        ...    ("v2", [{"cohort": "A", "alleleFrequency": None},], [{"cohort": "A", "nSamples": 50}]),
        ...    ("v3", [{"cohort": "A", "alleleFrequency": 0.05},], [{"cohort": "A", "nSamples": None}]),]
        >>> schema = "variantId STRING, alleleFrequencies ARRAY<STRUCT<cohort: STRING, alleleFrequency: DOUBLE>>, nSamplesPerCohort ARRAY<STRUCT<cohort: STRING, nSamples: INT>>"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show(truncate=False)
        +---------+------------------------------+-----------------------------+
        |variantId|alleleFrequencies             |nSamplesPerCohort            |
        +---------+------------------------------+-----------------------------+
        |v1       |[{A, 0.6}, {B, 0.2}, {C, 0.3}]|[{A, 100}, {B, 200}, {D, 20}]|
        |v2       |[{A, NULL}]                   |[{A, 50}]                    |
        |v3       |[{A, 0.05}]                   |[{A, NULL}]                  |
        +---------+------------------------------+-----------------------------+
        <BLANKLINE>

        >>> df = df.withColumn("combinedAlleleFrequency", combined_allele_frequency(f.col("alleleFrequencies"), f.col("nSamplesPerCohort")))
        >>> df.select("variantId", f.round("combinedAlleleFrequency", 2).alias("caf")).show(truncate=False)
        +---------+----+
        |variantId|caf |
        +---------+----+
        |v1       |0.33|
        |v2       |NULL|
        |v3       |NULL|
        +---------+----+
        <BLANKLINE>
    """
    af_filtered = f.filter(
        allele_freq, lambda x: x.getField("alleleFrequency").isNotNull()
    )
    samples_filtered = f.filter(
        n_samples_per_cohort, lambda x: x.getField("nSamples").isNotNull()
    )

    intersect = f.array_intersect(
        f.transform(af_filtered, lambda x: x.getField("cohort")),
        f.transform(samples_filtered, lambda x: x.getField("cohort")),
    )
    af_map = f.map_from_entries(
        f.filter(
            af_filtered, lambda x: f.array_contains(intersect, x.getField("cohort"))
        )
    )
    n_samples_map = f.map_from_entries(
        f.filter(
            samples_filtered,
            lambda x: f.array_contains(intersect, x.getField("cohort")),
        )
    )
    # Compute numerator: sum(AF * n)
    af_times_n = f.aggregate(
        f.map_entries(af_map),
        f.lit(0.0),
        lambda acc, kv: (
            acc
            + kv["value"]
            * f.coalesce(f.element_at(n_samples_map, kv["key"]), f.lit(0.0))
        ),
    )

    # Compute denominator: sum(n)
    n_total = f.aggregate(
        f.map_entries(n_samples_map),
        f.lit(0),
        lambda acc, kv: acc + f.coalesce(kv["value"], f.lit(0)),
    )

    return (
        f.when(n_total == 0, f.lit(None))
        .otherwise(af_times_n / n_total)
        .cast(t.FloatType())
    )


def infer_allele_frequency_from_maf(imp_maf: Column, eur_af: Column) -> Column:
    """Infer allele frequency from imputed MAF and European AF.

    Args:
        imp_maf (Column): Imputed minor allele frequency from source.
        eur_af (Column): European allele frequency from gnomAD.

    Returns:
        Column: Inferred allele frequency.

    !!! note "Inference logic"
        The effect allele frequency (EAF) is inferred by comparing the imputed minor allele frequency (impMAF)
        from deCODE with the European allele frequency (EUR_AF) from gnomAD.

        * If `EUR_AF` is null, the effect allele frequency cannot be inferred. In this case, the imputed
        minor allele frequency (`impMAF`) is used as the EAF. This typically occurs for variants that are
        unique to deCODE and absent from the gnomAD European population.

        * If `EUR_AF` is closer to `impMAF` than to `1 − impMAF`, the effect allele is assumed to be the
        minor allele in the Icelandic population, and `impMAF` is used as the EAF.

        * If `EUR_AF` is closer to `1 − impMAF` than to `impMAF`, the effect allele is assumed to be the
        major allele in the Icelandic population, and `1 − impMAF` is used as the EAF.

    Examples:
        >>> data = [(0.01, 0.02), (0.01, 0.6), (0.01, None)]
        >>> df = spark.createDataFrame(data, ["impMAF", "EUR_AF"])
        >>> df.withColumn("EAF", infer_allele_frequency_from_maf(f.col("impMAF"), f.col("EUR_AF"))).show()
        +------+------+----+
        |impMAF|EUR_AF| EAF|
        +------+------+----+
        |  0.01|  0.02|0.01|
        |  0.01|   0.6|0.99|
        |  0.01|  NULL|0.01|
        +------+------+----+
        <BLANKLINE>
    """
    d1 = f.abs(eur_af - imp_maf)
    d2 = f.abs(eur_af - (1 - imp_maf))

    return (
        f.when(eur_af.isNull(), imp_maf)
        .when(d1 <= d2, imp_maf)
        .otherwise(1 - imp_maf)
        .alias("effectAlleleFrequencyFromSource")
    )
