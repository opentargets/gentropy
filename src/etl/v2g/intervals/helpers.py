"""Interval helper functions."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from etl.common.utils import get_gene_tss


def prepare_gene_interval_lut(gene_index: DataFrame) -> DataFrame:
    """Gene symbol lookup table.

    Pre-processess gene/target dataset to create lookup table of gene symbols, including
    obsoleted gene symbols.

    Args:
        gene_index (DataFrame): gene/target DataFrame

    Returns:
        DataFrame: Gene LUT for symbol mapping
    """
    return gene_index.select(
        f.col("id").alias("geneId"),
        "biotype",
        f.explode(
            f.array_union(f.array("approvedSymbol"), f.col("obsoleteSymbols.label"))
        ).alias("geneSymbol"),
        f.col("genomicLocation.chromosome").alias("chromosome"),
        get_gene_tss(
            f.col("genomicLocation.strand"),
            f.col("genomicLocation.start"),
            f.col("genomicLocation.end"),
        ).alias("tss"),
        "genomicLocation",
    )


def get_variants_in_interval(
    interval_df: DataFrame, variants_df: DataFrame
) -> DataFrame:
    """Explodes the interval dataset to find all variants in the region.

    Args:
        interval_df (DataFrame): Interval dataset
        variants_df (DataFrame): DataFrame with a set of variants of interest with the columns "variantId", "chromosome" and "position"

    Returns:
        DataFrame: V2G evidence based on all the variants found in the intervals
    """
    return (
        interval_df.join(variants_df, on="chromosome", how="inner")
        .filter(f.col("position").between(f.col("start"), f.col("end")))
        .drop("start", "end")
    )
