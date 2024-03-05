"""General test for the matrix module."""

from typing import Iterable

from pyspark.ml.linalg import DenseMatrix, MatrixUDT
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f


def long_ld_to_matrix(
    df: DataFrame, study_key: str, row_key: str, col_key: str, value_key: str
) -> DataFrame:
    """Convert long format LD matrix to DenseMatrix (numpy).

    Args:
        df (DataFrame): The long format dataframe.
        study_key (str): The name of the study column.
        row_key (str): The name of the row column.
        col_key (str): The name of the column column.
        value_key (str): The name of the value column.

    Returns:
        DataFrame: A dataframe containing the matrix.

    Example:
        >>> data = [
        ...     ["s1", 1, "a", "a"],
        ...     ["s1", 2, "b", "a"],
        ...     ["s1", 3, "c", "a"],
        ...     ["s1", 4, "a", "b"],
        ...     ["s1", 5, "b", "b"],
        ...     ["s1", 6, "c", "b"],
        ...     ["s1", 7, "a", "c"],
        ...     ["s1", 8, "b", "c"],
        ...     ["s1", 9, "c", "c"],
        ...     ["s2", 11, "a", "a"],
        ...     ["s2", 12, "a", "b"],
        ...     ["s2", 13, "b", "a"],
        ...     ["s2", 14, "b", "b"],
        ... ]
        >>> cols = ["study", "value", "row", "col"]
        >>> df = spark.createDataFrame(data).toDF(*cols)
        >>> long_ld_to_matrix(df, "study", "row", "col", "value").show(truncate = False)
        +-----+-----+--------------------------------+
        |index|matrix                           |
        +-----+--------------------------------+
        |s1   |DenseMatrix(3, 3, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0], false)|
        |s2   |DenseMatrix(2, 2, [11.0, 12.0, 13.0, 14.0], false)|
        +-----+--------------------------------+
        <BLANKLINE>

    """

    def _vectors_to_matrix(values: Iterable[float], n: int) -> DenseMatrix:
        """Convert  of vectors to a matrix.

        Args:
            values (Iterable[float]): The values of the matrix.
            n (int): The number of rows and columns of the matrix.

        Returns:
            DenseMatrix: A dense matrix.
        """
        return DenseMatrix(n, n, values)

    _vectors_to_matrix_udf = f.udf(
        lambda values, n: _vectors_to_matrix(values, n), MatrixUDT()
    )

    w_study = Window.partitionBy(study_key).orderBy(row_key, col_key)
    w_study_row = Window.partitionBy(study_key).orderBy(col_key)

    return (
        df.withColumn("index", f.collect_list(f.col(col_key)).over(w_study_row))
        .withColumn("matrix", f.collect_list(f.col(value_key)).over(w_study))
        .groupBy(study_key)
        .agg(
            f.max("index").alias("index"),
            _vectors_to_matrix_udf(
                f.max(f.col("matrix")), f.size(f.max(f.col("index")))
            ).alias("matrix"),
        )
    )
