"""Common utilities in Spark that can be used across the project."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Window

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, WindowSpec


def nullify_empty_array(column: Column) -> Column:
    """Returns null when a Spark Column has an array of size 0, otherwise return the array.

    Args:
        column (Column): The Spark Column to be processed.

    Returns:
        Column: Nullified column when the array is empty.

    Examples:
    >>> df = spark.createDataFrame([[], [1, 2, 3]], "array<int>")
    >>> df.withColumn("new", nullify_empty_array(df.value)).show()
    +---------+---------+
    |    value|      new|
    +---------+---------+
    |       []|     null|
    |[1, 2, 3]|[1, 2, 3]|
    +---------+---------+
    <BLANKLINE>
    """
    return f.when(f.size(column) != 0, column)


def get_top_ranked_in_window(df: DataFrame, w: WindowSpec) -> DataFrame:
    """Returns the record with the top rank within each group of the window."""
    return (
        df.withColumn("row_number", f.row_number().over(w))
        .filter(f.col("row_number") == 1)
        .drop("row_number")
    )


def get_record_with_minimum_value(
    df: DataFrame, grouping_col: str, sorting_col: str
) -> DataFrame:
    """Returns the record with the minimum value of the sorting column within each group of the grouping column.

    Args:
        df (DataFrame): The DataFrame to be processed.
        grouping_col (str): The column name(s) to group the DataFrame by.
        sorting_col (str): The column name to sort the DataFrame by.

    Returns:
        DataFrame: The DataFrame with the record with the minimum value of the sorting column within each group of the grouping column.

    Examples:
    >>> df = spark.createDataFrame(
    ...     [[16, 5738837, "snv", "16_5738837_G_C"],
    ...     [16, 5738892, "snv", "16_5738892_A_G"],
    ...     [17, 5738922, "del", "16_5738922_CA_C"]],
    ...     ["chromosome", "position", "alleleType", "id"])
    >>> df.transform(
    ...     lambda df: get_record_with_minimum_value(
    ...         df, "chromosome", "position"
    ...     )
    ... ).show()
    +----------+--------+----------+---------------+
    |chromosome|position|alleleType|             id|
    +----------+--------+----------+---------------+
    |        17| 5738922|       del|16_5738922_CA_C|
    |        16| 5738837|       snv| 16_5738837_G_C|
    +----------+--------+----------+---------------+
    <BLANKLINE>
    """
    w = Window.partitionBy(grouping_col).orderBy(sorting_col)
    return get_top_ranked_in_window(df, w)


def get_record_with_maximum_value(
    df: DataFrame, grouping_col: str, sorting_col: str
) -> DataFrame:
    """Returns the record with the maximum value of the sorting column within each group of the grouping column.

    Args:
        df (DataFrame): The DataFrame to be processed.
        grouping_col (str): The column name(s) to group the DataFrame by.
        sorting_col (str): The column name to sort the DataFrame by.

    Returns:
        DataFrame: The DataFrame with the record with the maximum value of the sorting column within each group of the grouping column.

    Examples:
    >>> df = spark.createDataFrame(
    ...     [[16, 5738837, "snv", "16_5738837_G_C"],
    ...     [16, 5738892, "snv", "16_5738892_A_G"],
    ...     [17, 5738922, "del", "16_5738922_CA_C"]],
    ...     ["chromosome", "position", "alleleType", "id"])
    >>> df.transform(
    ...     lambda df: get_record_with_maximum_value(
    ...         df, ["alleleType", "chromosome"], "position"
    ...     )
    ... ).show()
    ++----------+--------+----------+---------------+
    |chromosome|position|alleleType|             id|
    +----------+--------+----------+---------------+
    |        16| 5738892|       snv| 16_5738892_A_G|
    |        17| 5738922|       del|16_5738922_CA_C|
    +----------+--------+----------+---------------+
    <BLANKLINE>
    """
    w = Window.partitionBy(grouping_col).orderBy(f.col(sorting_col).desc())
    return get_top_ranked_in_window(df, w)
