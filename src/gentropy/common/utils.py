"""Common functions in the Genetics datasets."""

from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl
from pyspark.sql import functions as f

from gentropy.common.spark_helpers import extract_column_name

if TYPE_CHECKING:
    from hail.table import Table
    from pyspark.sql import Column


def liftover_loci(
    variant_index: Table, chain_path: str, dest_reference_genome: str
) -> Table:
    """Liftover a Hail table containing variant information from GRCh37 to GRCh38 or vice versa.

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
