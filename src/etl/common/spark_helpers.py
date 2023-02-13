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


def _neglog_p(p_value_mantissa: Column, p_value_exponent: Column) -> Column:
    """Compute the negative log p-value.

    Args:
        p_value_mantissa (Column): P-value mantissa
        p_value_exponent (Column): P-value exponent

    Returns:
        Column: Negative log p-value

    Examples:
        >>> d = [(1, 1), (5, -2), (1, -1000)]
        >>> df = spark.createDataFrame(d).toDF("p_value_mantissa", "p_value_exponent")
        >>> df.withColumn("neg_log_p", _neglog_p(f.col("p_value_mantissa"), f.col("p_value_exponent"))).show()
        +----------------+----------------+------------------+
        |p_value_mantissa|p_value_exponent|         neg_log_p|
        +----------------+----------------+------------------+
        |               1|               1|              -1.0|
        |               5|              -2|1.3010299956639813|
        |               1|           -1000|            1000.0|
        +----------------+----------------+------------------+
        <BLANKLINE>
    """
    return -1 * (f.log10(p_value_mantissa) + p_value_exponent)


def adding_quality_flag(
    qc_column: Column, flag_condition: Column, flag_text: str
) -> Column:
    """Update the provided quality control list with a new flag if condition is met.

    Args:
        qc_column (Column): Array column with existing QC flags.
        flag_condition (Column): This is a column of booleans, signing which row should be flagged
        flag_text (str): Text for the new quality control flag

    Returns:
        Column: Array column with the updated list of qc flags.

    Examples:
    >>> data = [(True, ['Existing flag']),(True, []),(False, [])]
    >>> new_flag = 'This is a new flag'
    >>> (
    ...     spark.createDataFrame(data, ['flag', 'qualityControl'])
    ...     .withColumn('qualityControl', adding_quality_flag(f.col('qualityControl'), f.col('flag'), new_flag))
    ...     .show(truncate=False)
    ... )
    +-----+-----------------------------------+
    |flag |qualityControl                     |
    +-----+-----------------------------------+
    |true |[Existing flag, This is a new flag]|
    |true |[This is a new flag]               |
    |false|[]                                 |
    +-----+-----------------------------------+
    <BLANKLINE>
    """
    return f.when(
        flag_condition,
        f.array_union(qc_column, f.array(f.lit(flag_text))),
    ).otherwise(qc_column)
