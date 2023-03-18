"""Common functions in the Genetics datasets."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import Column


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
