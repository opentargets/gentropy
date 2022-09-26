from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def prepare_gene_interval_lut(gene_index: DataFrame) -> DataFrame:
    """Pre-processing the gene dataset
    :param gene_index: gene index dataframe
    :return: Spark Dataframe
    """

    # Prepare gene set:
    genes = (
        # Include TSS
        gene_index.withColumn(
            "tss",
            f.when(
                f.col("genomicLocation.strand") == 1, f.col("genomicLocation.start")
            ).otherwise(
                f.col("genomicLocation.end"),
            ),
        )
        # Consider also obsoleted symbols (explode)
        .withColumn(
            "symbols",
            f.array_union(f.array("approvedSymbol"), f.col("obsoleteSymbols.label")),
        )
        .withColumn("symbols", f.explode("symbols"))
        .withColumnRenamed("id", "geneId")
        .withColumn("chromosome", f.col("genomicLocation.chromosome"))
    )
    return genes
