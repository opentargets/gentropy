"""Common functions in the Genetics datasets."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.spark import extract_column_name

if TYPE_CHECKING:
    from pyspark.sql import Column


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
        +----------+-----------------------+
        |chromosome|normalized_chromosome  |
        +----------+-----------------------+
        |chr1      |1                      |
        |2         |2                      |
        |chrX      |X                      |
        |Y         |Y                      |
        |chrM      |MT                     |
        |23        |X                      |
        |24        |Y                      |
        +----------+-----------------------+
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
