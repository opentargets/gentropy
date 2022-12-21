"""Common functions in the Genetics datasets."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import Column


def get_gene_tss(strand_col: Column, start_col: Column, end_col: Column) -> Column:
    """Returns the TSS of a gene based on its orientation.

    Args:
        strand_col (Column): Column containing 1 if the coding strand of the gene is forward, and -1 if it is reverse.
        start_col (Column): Column containing the start position of the gene.
        end_col (Column): Column containing the end position of the gene.

    Returns:
        Column: Column containing the TSS of the gene.
    """
    return f.when(strand_col == 1, start_col).when(strand_col == -1, end_col)


def convert_gnomad_position_to_ensembl(
    position: Column, reference: Column, alternate: Column
) -> Column:
    """Converting GnomAD variant position to Ensembl variant position.

    For indels (the reference or alternate allele is longer than 1), then adding 1 to the position, for SNPs, the position is unchanged.

    Args:
        position (Column): Column
        reference (Column): The reference allele.
        alternate (Column): The alternate allele

    Returns:
        The position of the variant in the Ensembl genome.
    """
    return f.when(
        (f.length(reference) > 1) | (f.length(alternate) > 1), position + 1
    ).otherwise(position)
