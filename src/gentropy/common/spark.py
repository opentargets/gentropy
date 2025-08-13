"""Common utilities in Spark that can be used across the project."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from functools import reduce, wraps
from itertools import chain
from typing import TYPE_CHECKING, Any, TypeVar

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Column, Row, Window

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, WindowSpec


def convert_from_wide_to_long(
    df: DataFrame,
    id_vars: Iterable[str],
    var_name: str,
    value_name: str,
    value_vars: Iterable[str] | None = None,
) -> DataFrame:
    """Converts a dataframe from wide to long format.

    Args:
        df (DataFrame): Dataframe to melt
        id_vars (Iterable[str]): List of fixed columns to keep
        var_name (str): Name of the column containing the variable names
        value_name (str): Name of the column containing the values
        value_vars (Iterable[str] | None): List of columns to melt. Defaults to None.

    Returns:
        DataFrame: Melted dataframe

    Examples:
        >>> df = spark.createDataFrame([("a", 1, 2)], ["id", "feature_1", "feature_2"])
        >>> convert_from_wide_to_long(df, ["id"], "feature", "value").show()
        +---+---------+-----+
        | id|  feature|value|
        +---+---------+-----+
        |  a|feature_1|  1.0|
        |  a|feature_2|  2.0|
        +---+---------+-----+
        <BLANKLINE>
    """
    if not value_vars:
        value_vars = [c for c in df.columns if c not in id_vars]
    _vars_and_vals = f.array(
        *(
            f.struct(
                f.lit(c).alias(var_name), f.col(c).cast(t.FloatType()).alias(value_name)
            )
            for c in value_vars
        )
    )

    # Add to the DataFrame and explode to convert into rows
    _tmp = df.withColumn("_vars_and_vals", f.explode(_vars_and_vals))

    cols = list(id_vars) + [
        f.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]
    ]
    return _tmp.select(*cols)


def convert_from_long_to_wide(
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
        >>> df = spark.createDataFrame([("a", "feature_1", 1), ("a", "feature_2", 2)], ["id", "featureName", "featureValue"])
        >>> convert_from_long_to_wide(df, ["id"], "featureName", "featureValue").show()
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
        |       []|     NULL|
        |[1, 2, 3]|[1, 2, 3]|
        +---------+---------+
        <BLANKLINE>
    """
    return f.when(f.size(column) != 0, column)


def get_top_ranked_in_window(df: DataFrame, w: WindowSpec) -> DataFrame:
    """Returns the record with the top rank within each group of the window.

    Args:
        df (DataFrame): The DataFrame to be processed.
        w (WindowSpec): The window to be used for ranking.

    Returns:
        DataFrame: The DataFrame with the record with the top rank within each group of the window.
    """
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
        grouping_col (Column | str | list[Column | str]): The column(s) to group the DataFrame by.
        sorting_col (str): The column name to sort the DataFrame by.

    Returns:
        DataFrame: The DataFrame with the record with the minimum value of the sorting column within each group of the grouping column.
    """
    w = Window.partitionBy(grouping_col).orderBy(sorting_col)
    return get_top_ranked_in_window(df, w)


def get_record_with_maximum_value(
    df: DataFrame,
    grouping_col: str | list[str],
    sorting_col: str,
) -> DataFrame:
    """Returns the record with the maximum value of the sorting column within each group of the grouping column.

    Args:
        df (DataFrame): The DataFrame to be processed.
        grouping_col (str | list[str]): The column(s) to group the DataFrame by.
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


def order_array_of_structs_by_field(column_name: str, field_name: str) -> Column:
    """Sort a column of array of structs by a field in descending order, nulls last.

    Args:
        column_name (str): Column name
        field_name (str): Field name

    Returns:
        Column: Sorted column
    """
    return f.expr(
        f"""
        array_sort(
        {column_name},
        (left, right) -> case
                        when left.{field_name} is null and right.{field_name} is null then 0
                        when left.{field_name} is null then 1
                        when right.{field_name} is null then -1
                        when left.{field_name} < right.{field_name} then 1
                        when left.{field_name} > right.{field_name} then -1
                        else 0
                end)
        """
    )


def order_array_of_structs_by_two_fields(
    array_name: str, descending_column: str, ascending_column: str
) -> Column:
    """Sort array of structs by a field in descending order and by an other field in an ascending order.

    This function doesn't deal with null values, assumes the sort columns are not nullable.
    The sorting function compares the descending_column first, in case when two values from descending_column are equal
    it compares the ascending_column. When values in both columns are equal, the rows order is preserved.

    Args:
        array_name (str): Column name with array of structs
        descending_column (str): Name of the first keys sorted in descending order
        ascending_column (str): Name of the second keys sorted in ascending order

    Returns:
        Column: Sorted column

    Examples:
        >>> data = [(1.0, 45, 'First'), (0.5, 232, 'Third'), (0.5, 233, 'Fourth'), (1.0, 125, 'Second'),]
        >>> (
        ...    spark.createDataFrame(data, ['col1', 'col2', 'ranking'])
        ...    .groupBy(f.lit('c'))
        ...    .agg(f.collect_list(f.struct('col1','col2', 'ranking')).alias('list'))
        ...    .select(order_array_of_structs_by_two_fields('list', 'col1', 'col2').alias('sorted_list'))
        ...    .show(truncate=False)
        ... )
        +-----------------------------------------------------------------------------+
        |sorted_list                                                                  |
        +-----------------------------------------------------------------------------+
        |[{1.0, 45, First}, {1.0, 125, Second}, {0.5, 232, Third}, {0.5, 233, Fourth}]|
        +-----------------------------------------------------------------------------+
        <BLANKLINE>
        >>> data = [(1.0, 45, 'First'), (1.0, 45, 'Second'), (0.5, 233, 'Fourth'), (1.0, 125, 'Third'),]
        >>> (
        ...    spark.createDataFrame(data, ['col1', 'col2', 'ranking'])
        ...    .groupBy(f.lit('c'))
        ...    .agg(f.collect_list(f.struct('col1','col2', 'ranking')).alias('list'))
        ...    .select(order_array_of_structs_by_two_fields('list', 'col1', 'col2').alias('sorted_list'))
        ...    .show(truncate=False)
        ... )
        +----------------------------------------------------------------------------+
        |sorted_list                                                                 |
        +----------------------------------------------------------------------------+
        |[{1.0, 45, First}, {1.0, 45, Second}, {1.0, 125, Third}, {0.5, 233, Fourth}]|
        +----------------------------------------------------------------------------+
        <BLANKLINE>
    """
    return f.expr(
        f"""
        array_sort(
        {array_name},
        (left, right) -> case
                when left.{descending_column} is null and right.{descending_column} is null then 0
                when left.{ascending_column} is null and right.{ascending_column} is null then 0

                when left.{descending_column} is null then 1
                when right.{descending_column} is null then -1

                when left.{ascending_column} is null then 1
                when right.{ascending_column} is null then -1

                when left.{descending_column} < right.{descending_column} then 1
                when left.{descending_column} > right.{descending_column} then -1
                when left.{descending_column} == right.{descending_column} and left.{ascending_column} > right.{ascending_column} then 1
                when left.{descending_column} == right.{descending_column} and left.{ascending_column} < right.{ascending_column} then -1
                when left.{ascending_column} == right.{ascending_column} and left.{descending_column} == right.{descending_column} then 0
        end)
        """
    )


def map_column_by_dictionary(col: Column, mapping_dict: dict[str, Any]) -> Column:
    """Map column values to dictionary values by key.

    Missing consequence label will be converted to None, unmapped consequences will be mapped as None.

    Args:
        col (Column): Column containing labels to map.
        mapping_dict (dict[str, Any]): Dictionary with mapping key/value pairs.

    Returns:
        Column: Column with mapped values.

    Examples:
        >>> data = [('consequence_1',),('unmapped_consequence',),(None,)]
        >>> m = {'consequence_1': 'SO:000000'}
        >>> (
        ...    spark.createDataFrame(data, ['label'])
        ...    .select('label',map_column_by_dictionary(f.col('label'),m).alias('id'))
        ...    .show()
        ... )
        +--------------------+---------+
        |               label|       id|
        +--------------------+---------+
        |       consequence_1|SO:000000|
        |unmapped_consequence|     NULL|
        |                NULL|     NULL|
        +--------------------+---------+
        <BLANKLINE>
    """
    map_expr = f.create_map(*[f.lit(x) for x in chain(*mapping_dict.items())])

    return map_expr[col]


def pivot_df(
    df: DataFrame,
    pivot_col: str,
    value_col: str,
    grouping_cols: list[Column],
) -> DataFrame:
    """Pivot a dataframe.

    Args:
        df (DataFrame): Dataframe to pivot
        pivot_col (str): Column to pivot on
        value_col (str): Column to pivot
        grouping_cols (list[Column]): Columns to group by

    Returns:
        DataFrame: Pivoted dataframe
    """
    pivot_values = df.select(pivot_col).distinct().rdd.flatMap(lambda x: x).collect()
    return (
        df.groupBy(grouping_cols)
        .pivot(pivot_col)
        .agg({value_col: "first"})
        .select(
            grouping_cols
            + [
                f.when(f.col(x).isNull(), None)
                .otherwise(f.col(x))
                .alias(f"{x}_{value_col}")
                for x in pivot_values
            ],
        )
    )


def get_value_from_row(row: Row, column: str) -> Any:
    """Extract index value from a row if exists.

    Args:
        row (Row): One row from a dataframe
        column (str): column label we want to extract.

    Returns:
        Any: value of the column in the row

    Raises:
        ValueError: if the column is not in the row

    Examples:
        >>> get_value_from_row(Row(geneName="AR", chromosome="X"), "chromosome")
        'X'
        >>> get_value_from_row(Row(geneName="AR", chromosome="X"), "disease")
        Traceback (most recent call last):
        ...
        ValueError: Column disease not found in row Row(geneName='AR', chromosome='X')
    """
    if column not in row:
        raise ValueError(f"Column {column} not found in row {row}")
    return row[column]


def enforce_schema(
    expected_schema: t.ArrayType | t.StructType | Column | str,
) -> Callable[..., Any]:
    """A function to enforce the schema of a function output follows expectation.

    Behavior:
        - Fields that are not present in the expected schema will be dropped.
        - Expected but missing fields will be added with Null values.
        - Fields with incorrect data types will be casted to the expected data type.

    This is a decorator function and expected to be used like this:

    @enforce_schema(spark_schema)
    def my_function() -> t.StructType:
        return ...

    Args:
        expected_schema (t.ArrayType | t.StructType | Column | str): The expected schema of the output.

    Returns:
        Callable[..., Any]: A decorator function.
    """
    T = TypeVar("T", str, Column)

    def decorator(function: Callable[..., T]) -> Callable[..., T]:
        """A decorator function to enforce the schema of a function output follows expectation.

        Args:
            function (Callable[..., T]): The function to be decorated.

        Returns:
            Callable[..., T]: The decorated function.
        """

        @wraps(function)
        def wrapper(*args: str, **kwargs: str) -> Any:
            return f.from_json(f.to_json(function(*args, **kwargs)), expected_schema)

        return wrapper

    return decorator


def rename_all_columns(df: DataFrame, prefix: str) -> DataFrame:
    """Given a prefix, rename all columns of a DataFrame.

    Args:
        df (DataFrame): The DataFrame to be processed.
        prefix (str): The prefix to be added to the column names.

    Returns:
        DataFrame: The DataFrame with all columns renamed.

    Examples:
        >>> data = [('a', 1.2, True),('b', 0.0, False),('c', None, None),]
        >>> prefix = 'prefix_'
        >>> rename_all_columns(spark.createDataFrame(data, ['col1', 'col2', 'col3']), prefix).show()
        +-----------+-----------+-----------+
        |prefix_col1|prefix_col2|prefix_col3|
        +-----------+-----------+-----------+
        |          a|        1.2|       true|
        |          b|        0.0|      false|
        |          c|       NULL|       NULL|
        +-----------+-----------+-----------+
        <BLANKLINE>
    """
    return reduce(
        lambda df, col: df.withColumnRenamed(col, f"{prefix}{col}"),
        df.columns,
        df,
    )


def safe_array_union(
    a: Column, b: Column, fields_order: list[str] | None = None
) -> Column:
    """Merge the content of two optional columns.

    The function assumes the array columns have the same schema.
    If the `fields_order` is passed, the function assumes that it deals with array of structs and sorts the nested
    struct fields by the provided `fields_order` before conducting array_merge.
    If the `fields_order` is not passed and both columns are <array<struct<...>> type then function assumes struct fields have the same order,
    otherwise the function will raise an AnalysisException.

    Args:
        a (Column): One optional array column.
        b (Column): The other optional array column.
        fields_order (list[str] | None): The order of the fields in the struct. Defaults to None.

    Returns:
        Column: array column with merged content.

    Examples:
        >>> data = [(['a'], ['b']), (['c'], None), (None, ['d']), (None, None)]
        >>> (
        ...    spark.createDataFrame(data, ['col1', 'col2'])
        ...    .select(
        ...        safe_array_union(f.col('col1'), f.col('col2')).alias('merged')
        ...    )
        ...    .show()
        ... )
        +------+
        |merged|
        +------+
        |[a, b]|
        |   [c]|
        |   [d]|
        |  NULL|
        +------+
        <BLANKLINE>
        >>> schema="arr2: array<struct<b:int,a:string>>, arr: array<struct<a:string,b:int>>"
        >>> data = [([(1,"a",), (2, "c")],[("a", 1,)]),]
        >>> df = spark.createDataFrame(data=data, schema=schema)
        >>> df.select(safe_array_union(f.col("arr"), f.col("arr2"), fields_order=["a", "b"]).alias("merged")).show()
        +----------------+
        |          merged|
        +----------------+
        |[{a, 1}, {c, 2}]|
        +----------------+
        <BLANKLINE>
        >>> schema="arr2: array<struct<b:int,a:string>>, arr: array<struct<a:string,b:int>>"
        >>> data = [([(1,"a",), (2, "c")],[("a", 1,)]),]
        >>> df = spark.createDataFrame(data=data, schema=schema)
        >>> df.select(safe_array_union(f.col("arr"), f.col("arr2")).alias("merged")).show() # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        pyspark.sql.utils.AnalysisException: ...
    """
    if fields_order:
        # sort the nested struct fields by the provided order
        a = sort_array_struct_by_columns(a, fields_order)
        b = sort_array_struct_by_columns(b, fields_order)
    return f.when(a.isNotNull() & b.isNotNull(), f.array_union(a, b)).otherwise(
        f.coalesce(a, b)
    )


def sort_array_struct_by_columns(column: Column, fields_order: list[str]) -> Column:
    """Sort nested struct fields by provided fields order.

    Args:
        column (Column): Column with array of structs.
        fields_order (list[str]): List of field names to sort by.

    Returns:
        Column: Sorted column.

    Examples:
        >>> schema="arr: array<struct<b:int,a:string>>"
        >>> data = [([(1,"a",), (2, "c")],)]
        >>> fields_order = ["a", "b"]
        >>> df = spark.createDataFrame(data=data, schema=schema)
        >>> df.select(sort_array_struct_by_columns(f.col("arr"), fields_order).alias("sorted")).show()
        +----------------+
        |          sorted|
        +----------------+
        |[{c, 2}, {a, 1}]|
        +----------------+
        <BLANKLINE>
    """
    column_name = extract_column_name(column)
    fields_order_expr = ", ".join([f"x.{field}" for field in fields_order])
    return f.expr(
        f"sort_array(transform({column_name}, x -> struct({fields_order_expr})), False)"
    ).alias(column_name)


def extract_column_name(column: Column) -> str:
    """Extract column name from a column expression.

    Args:
        column (Column): Column expression.

    Returns:
        str: Column name.

    Raises:
        ValueError: If the column name cannot be extracted.

    Examples:
        >>> extract_column_name(f.col('col1'))
        'col1'
        >>> extract_column_name(f.sort_array(f.col('col1')))
        'sort_array(col1, true)'
    """
    pattern = re.compile("^Column<'(?P<name>.*)'>?")

    _match = pattern.search(str(column))
    if not _match:
        raise ValueError(f"Cannot extract column name from {column}")
    return _match.group("name")


def create_empty_column_if_not_exists(
    col_name: str, col_schema: t.DataType = t.NullType()
) -> Column:
    """Create a column if it does not exist in the DataFrame.

    Args:
        col_name (str): The name of the column to be created.
        col_schema (t.DataType): The schema of the column to be created. Defaults to NullType.

    Returns:
        Column: The expression to create the column.

    Examples:
        >>> df = spark.createDataFrame([(1, 2),], ['col1', 'col2'])
        >>> df.select("*", create_empty_column_if_not_exists('col3', t.IntegerType())).show()
        +----+----+----+
        |col1|col2|col3|
        +----+----+----+
        |   1|   2|NULL|
        +----+----+----+
        <BLANKLINE>
    """
    return f.lit(None).cast(col_schema).alias(col_name)


def get_nested_struct_schema(dtype: t.DataType) -> t.StructType:
    """Get the bottom StructType from a nested ArrayType type.

    Args:
        dtype (t.DataType): The nested data structure.

    Returns:
        t.StructType: The nested struct schema.

    Raises:
        TypeError: If the input data type is not a nested struct.

    Examples:
        >>> get_nested_struct_schema(t.ArrayType(t.StructType([t.StructField('a', t.StringType())])))
        StructType([StructField('a', StringType(), True)])

        >>> get_nested_struct_schema(t.ArrayType(t.ArrayType(t.StructType([t.StructField("a", t.StringType())]))))
        StructType([StructField('a', StringType(), True)])
    """
    if isinstance(dtype, t.StructField):
        dtype = dtype.dataType

    match dtype:
        case t.StructType(fields=_):
            return dtype
        case t.ArrayType(elementType=dtype):
            return get_nested_struct_schema(dtype)
        case _:
            raise TypeError("The input data type must be a nested struct.")


def get_struct_field_schema(schema: t.StructType, name: str) -> t.DataType:
    """Get schema for underlying struct field.

    Args:
        schema (t.StructType): Provided schema where the name should be looked in.
        name (str): Name of the field to look in the schema

    Returns:
        t.DataType: Data type of the StructField with provided name

    Raises:
        ValueError: If provided name is not present in the input schema

    Examples:
        >>> get_struct_field_schema(t.StructType([t.StructField("a", t.StringType())]), "a")
        StringType()

        >>> get_struct_field_schema(t.StructType([t.StructField("a", t.StringType())]), "b") # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        ValueError: Provided name b is not present in the schema

    """
    matching_fields = [f for f in schema.fields if f.name == name]
    if not matching_fields:
        raise ValueError("Provided name %s is not present in the schema.", name)
    return matching_fields[0].dataType


def calculate_harmonic_sum(input_array: Column) -> Column:
    """Calculate the harmonic sum of an array.

    Args:
        input_array (Column): input array of doubles

    Returns:
        Column: column of harmonic sums

    Examples:
        >>> from pyspark.sql import Row
        >>> df = spark.createDataFrame([
        ...     Row([0.3, 0.8, 1.0]),
        ...     Row([0.7, 0.2, 0.9]),
        ...     ], ["input_array"]
        ... )
        >>> df.select("*", f.round(calculate_harmonic_sum(f.col("input_array")), 2).alias("harmonic_sum")).show()
        +---------------+------------+
        |    input_array|harmonic_sum|
        +---------------+------------+
        |[0.3, 0.8, 1.0]|        0.75|
        |[0.7, 0.2, 0.9]|        0.67|
        +---------------+------------+
        <BLANKLINE>
    """
    return f.aggregate(
        f.arrays_zip(
            f.sort_array(input_array, False).alias("score"),
            f.sequence(f.lit(1), f.size(input_array)).alias("pos"),
        ),
        f.lit(0.0),
        lambda acc, x: acc
        + x["score"]
        / f.pow(x["pos"], 2)
        / f.lit(sum(1 / ((i + 1) ** 2) for i in range(1000))),
    )


def clean_strings_from_symbols(source: Column) -> Column:
    """To make strings URL-safe and consistent by lower-casing and replace special characters with underscores.

    Args:
        source (Column): Source string

    Returns:
        Column: Cleaned string

    Examples:
        >>> d = [("AbCd-12.2",),("AaBb..123?",),("cDd!@#$%^&*()",),]
        >>> df = spark.createDataFrame(d).toDF("source")
        >>> df.withColumn("cleaned", clean_strings_from_symbols(f.col("source"))).show(truncate=False)
        +-------------+---------+
        |source       |cleaned  |
        +-------------+---------+
        |AbCd-12.2    |abcd-12_2|
        |AaBb..123?   |aabb_123_|
        |cDd!@#$%^&*()|cdd_     |
        +-------------+---------+
        <BLANKLINE>
    """
    characters_to_replace = r"[^a-z0-9-_]+"
    return f.regexp_replace(f.lower(source), characters_to_replace, "_")


def filter_array_struct(
    array_struct: Column | str,
    key_column: Column | str,
    key: Column | str | int | bool | float,
    value_column: Column | str,
) -> Column:
    """Extract a value from an array of structs based on a key.

    This function searches for the predicate `key_column` that matches the `key` within the
    `array_struct` and returns the corresponding `value_column` from the struct with
    the same index as predicate.

    Warning:
        Only the first match will be returned. If there are multiple matches, one need to
        sort the array first.

    Warning:
        The function will not work if the `key_column` or `value_column` are not present in the
        `array_struct` schema.

    Args:
        array_struct (Column | str): The array of structs to be searched.
        key_column (Column | str): The column name to be used as a key.
        key (Column | str | int | bool | float): The key to be searched for.
        value_column (Column | str): The column name to be returned.

    Returns:
        Column: The value_column from the struct from the same array element as the matched key_column.

    Examples:
        >>> data = [([{"a": 1, "b": 2.0, "c": "c", "d": True}, {"a": 3, "b": 4.0, "c": "c", "d": False}], "c")]
        >>> schema = 'col array<struct<a:int,b:float,c:string, d:boolean>>, col2 string'
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show(truncate=False)
        +---------------------------------------+----+
        |col                                    |col2|
        +---------------------------------------+----+
        |[{1, 2.0, c, true}, {3, 4.0, c, false}]|c   |
        +---------------------------------------+----+
        <BLANKLINE>

        >>> df.printSchema()
        root
         |-- col: array (nullable = true)
         |    |-- element: struct (containsNull = true)
         |    |    |-- a: integer (nullable = true)
         |    |    |-- b: float (nullable = true)
         |    |    |-- c: string (nullable = true)
         |    |    |-- d: boolean (nullable = true)
         |-- col2: string (nullable = true)
        <BLANKLINE>

        ** Key can be an int **

        >>> array_struct = "col"
        >>> key_column = "a"
        >>> key = 1
        >>> value_column = "b"
        >>> result = df.select(filter_array_struct(array_struct, key_column, key, value_column))
        >>> result.show()
        +---+
        |  b|
        +---+
        |2.0|
        +---+
        <BLANKLINE>

        ** Key can be a float **

        >>> key_column = "b"
        >>> key = 2.0
        >>> value_column = "a"
        >>> result = df.select(filter_array_struct(array_struct, key_column, key, value_column))
        >>> result.show()
        +---+
        |  a|
        +---+
        |  1|
        +---+
        <BLANKLINE>

        ** Key can be a string **

        >>> key_column = "c"
        >>> key = "c"
        >>> value_column = "a"
        >>> result = df.select(filter_array_struct(array_struct, key_column, key, value_column))

        The first match will be returned, even if array have multiple matches to the key.

        >>> result.show()
        +---+
        |  a|
        +---+
        |  1|
        +---+
        <BLANKLINE>

        ** Key can be a boolean **

        >>> key_column = "d"
        >>> key = True
        >>> value_column = "a"
        >>> result = df.select(filter_array_struct(array_struct, key_column, key, value_column))
        >>> result.show()
        +---+
        |  a|
        +---+
        |  1|
        +---+
        <BLANKLINE>

        ** Key can be a column**

        >>> array_struct = f.col("col")
        >>> key_column = "c"
        >>> key = f.col("col2")
        >>> value_column = "b"
        >>> result = df.select(filter_array_struct(array_struct, key_column, key, value_column))
        >>> result.show()
        +---+
        |  b|
        +---+
        |2.0|
        +---+
        <BLANKLINE>

        ** All paramters are columns **
        >>> array_struct = f.col("col")
        >>> key_column = f.col("c")
        >>> key = f.col("col2")
        >>> value_column = f.col("a")
        >>> result = df.select(filter_array_struct(array_struct, key_column, key, value_column))
        >>> result.show()
        +---+
        |  a|
        +---+
        |  1|
        +---+
        <BLANKLINE>
    """
    if not isinstance(key, Column):
        key = f.lit(key)

    if not isinstance(array_struct, Column):
        array_struct = f.col(array_struct)

    if isinstance(key_column, Column):
        key_column = extract_column_name(key_column)
    if isinstance(value_column, Column):
        value_column = extract_column_name(value_column)

    return (
        f.filter(
            array_struct,
            lambda x: x.getField(key_column) == key,
        )
        .getItem(0)
        .getField(value_column)
        .alias(value_column)
    )
