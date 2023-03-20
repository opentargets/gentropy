"""Common utilities in Spark that can be used across the project."""
from __future__ import annotations

import re
from typing import TYPE_CHECKING, Iterable, Optional

import pyspark.sql.functions as f
from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, WindowSpec


def _convert_from_wide_to_long(
    df: DataFrame,
    id_vars: Iterable[str],
    var_name: str,
    value_name: str,
    value_vars: Optional[Iterable[str]] = None,
) -> DataFrame:
    """Converts a dataframe from wide to long format using Pandas melt built-in function.

    The Pandas df schema needs to be parsed to account for the cases where the df is empty and Spark cannot infer the schema.

    Args:
        df (DataFrame): Dataframe to melt
        id_vars (Iterable[str]): List of fixed columns to keep
        var_name (str): Name of the column containing the variable names
        value_name (str): Name of the column containing the values
        value_vars (Iterable[str]): List of columns to melt. If not specified, uses all columns that are not set as id_vars.

    Returns:
        DataFrame: Melted dataframe

    Examples:
    >>> df = spark.createDataFrame([("a", 1, 2)], ["id", "feature_1", "feature_2"])
    >>> _convert_from_wide_to_long(df, ["id"], "feature", "value", spark).show()
    +---+---------+-----+
    | id|  feature|value|
    +---+---------+-----+
    |  a|feature_1|    1|
    |  a|feature_2|    2|
    +---+---------+-----+
    <BLANKLINE>
    """
    if not value_vars:
        value_vars = [c for c in df.columns if c not in id_vars]
    _vars_and_vals = f.array(
        *(
            f.struct(f.lit(c).alias(var_name), f.col(c).alias(value_name))
            for c in value_vars
        )
    )

    # Add to the DataFrame and explode to convert into rows
    _tmp = df.withColumn("_vars_and_vals", f.explode(_vars_and_vals))

    cols = list(id_vars) + [
        f.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]
    ]
    return _tmp.select(*cols)


def _convert_from_long_to_wide(
    df: DataFrame, id_vars: list[str], var_name: str, value_name: str
) -> DataFrame:
    """Converts a dataframe from long to wide format using Spark pivot built-in function.

    Args:
        df (DataFrame): Dataframe to pivot
        id_vars (list[str]): List of fixed columns to keep
        var_name (str): Name of the column to pivot on
        value_name (str): Name of the column containing the values

    Returns:
        DataFrame: Pivoted dataframe

    Examples:
    >>> df = spark.createDataFrame([("a", "feature_1", 1), ("a", "feature_2", 2)], ["id", "feature", "value"])
    >>> _convert_from_long_to_wide(df, ["id"], "feature", "value").show()
    +---+---------+---------+
    | id|feature_1|feature_2|
    +---+---------+---------+
    |  a|        1|        2|
    +---+---------+---------+
    <BLANKLINE>
    """
    return df.groupBy(id_vars).pivot(var_name).agg(f.first(value_name))


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


def calculate_neglog_pvalue(
    p_value_mantissa: Column, p_value_exponent: Column
) -> Column:
    """Compute the negative log p-value.

    Args:
        p_value_mantissa (Column): P-value mantissa
        p_value_exponent (Column): P-value exponent

    Returns:
        Column: Negative log p-value

    Examples:
        >>> d = [(1, 1), (5, -2), (1, -1000)]
        >>> df = spark.createDataFrame(d).toDF("p_value_mantissa", "p_value_exponent")
        >>> df.withColumn("neg_log_p", calculate_neglog_pvalue(f.col("p_value_mantissa"), f.col("p_value_exponent"))).show()
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


def string2camelcase(col_name: str) -> str:
    """Converting a string to camelcase.

    Args:
        col_name (str): a random string

    Returns:
        str: Camel cased string

    Examples:
        >>> string2camelcase("hello_world")
        'helloWorld'
        >>> string2camelcase("hello world")
        'helloWorld'
    """
    # Removing a bunch of unwanted characters from the column names:
    col_name_normalised = re.sub(r"[\/\(\)\-]+", " ", col_name)

    first, *rest = re.split("[ _-]", col_name_normalised)
    return "".join([first.lower(), *map(str.capitalize, rest)])


def column2camel_case(col_name: str) -> str:
    """A helper function to convert column names to camel cases.

    Args:
        col_name (str): a single column name

    Returns:
        str: spark expression to select and rename the column

    Examples:
        >>> column2camel_case("hello_world")
        '`hello_world` as helloWorld'
    """
    return f"`{col_name}` as {string2camelcase(col_name)}"
