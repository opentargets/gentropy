"""Common utilities in Spark that can be used across the project."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
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
    df: DataFrame,
    grouping_col: Column | str | list[Column | str],
    sorting_col: str,
) -> DataFrame:
    """Returns the record with the minimum value of the sorting column within each group of the grouping column.

    Args:
        df (DataFrame): The DataFrame to be processed.
        grouping_col (str): The column name(s) to group the DataFrame by.
        sorting_col (str): The column name to sort the DataFrame by.

    Returns:
        DataFrame: The DataFrame with the record with the minimum value of the sorting column within each group of the grouping column.
    """
    w = Window.partitionBy(grouping_col).orderBy(sorting_col)
    return get_top_ranked_in_window(df, w)


def get_record_with_maximum_value(
    df: DataFrame,
    grouping_col: Column | str | list[Column | str],
    sorting_col: str,
) -> DataFrame:
    """Returns the record with the maximum value of the sorting column within each group of the grouping column.

    Args:
        df (DataFrame): The DataFrame to be processed.
        grouping_col (str): The column name(s) to group the DataFrame by.
        sorting_col (str): The column name to sort the DataFrame by.

    Returns:
        DataFrame: The DataFrame with the record with the maximum value of the sorting column within each group of the grouping column.
    """
    w = Window.partitionBy(grouping_col).orderBy(f.col(sorting_col).desc())
    return get_top_ranked_in_window(df, w)


def normalise_column(
    df: DataFrame, input_col_name: str, output_col_name: str
) -> DataFrame:
    """Normalises a numerical column to a value between 0 and 1.

    Args:
        df (DataFrame): The DataFrame to be processed.
        input_col_name (str): The name of the column to be normalised.
        output_col_name (str): The name of the column to store the normalised values.

    Returns:
        DataFrame: The DataFrame with the normalised column.

    Examples:
    >>> df = spark.createDataFrame([5, 50, 1000], "int")
    >>> df.transform(lambda df: normalise_column(df, "value", "norm_value")).show()
    +-----+----------+
    |value|norm_value|
    +-----+----------+
    |    5|       0.0|
    |   50|      0.05|
    | 1000|       1.0|
    +-----+----------+
    <BLANKLINE>
    """
    vec_assembler = VectorAssembler(
        inputCols=[input_col_name], outputCol="feature_vector"
    )
    scaler = MinMaxScaler(inputCol="feature_vector", outputCol="norm_vector")
    unvector_score = f.round(vector_to_array(f.col("norm_vector"))[0], 2).alias(
        output_col_name
    )
    pipeline = Pipeline(stages=[vec_assembler, scaler])
    return (
        pipeline.fit(df)
        .transform(df)
        .select("*", unvector_score)
        .drop("feature_vector", "norm_vector")
    )
