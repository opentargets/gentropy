"""Common utilities in Spark that can be used across the project."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Window

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def nullify_empty_array(column: Column) -> Column:
    """Returns null when a Spark Column has an array of size 0, otherwise return the array.

    Args:
        column (Column): The Spark Column to be processed.

    Returns:
        Column: Nullified column when the array is empty.
    """
    return f.when(f.size(column) != 0, column)


def get_record_with_minimum_value(
    df: DataFrame, grouping_col: str, sorting_col: str
) -> DataFrame:
    """Returns the record with the minimum value of the sorting column within each group of the grouping column."""
    w = Window.partitionBy(grouping_col).orderBy(sorting_col)
    return (
        df.withColumn("row_number", f.row_number().over(w))
        .filter(f.col("row_number") == 1)
        .drop("row_number")
    )


def get_record_with_maximum_value(
    df: DataFrame, grouping_col: str, sorting_col: str
) -> DataFrame:
    """Returns the record with the maximum value of the sorting column within each group of the grouping column."""
    w = Window.partitionBy(grouping_col).orderBy(f.col(sorting_col).desc())
    return (
        df.withColumn("row_number", f.row_number().over(w))
        .filter(f.col("row_number") == 1)
        .drop("row_number")
    )


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
