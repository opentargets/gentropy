"""General test for the matrix module."""

from typing import Iterable

from pyspark.ml.linalg import DenseMatrix, MatrixUDT
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("test_matrix").getOrCreate()

# Create a 3 column dataframe containing the long format of a matrix
data = [
    ["s1", 1, "a", "a"],
    ["s1", 2, "b", "a"],
    ["s1", 3, "c", "a"],
    ["s1", 4, "a", "b"],
    ["s1", 5, "b", "b"],
    ["s1", 6, "c", "b"],
    ["s1", 7, "a", "c"],
    ["s1", 8, "b", "c"],
    ["s1", 9, "c", "c"],
    ["s2", 11, "a", "a"],
    ["s2", 12, "a", "b"],
    ["s2", 13, "b", "a"],
    ["s2", 14, "b", "b"],
]
cols = ["study", "value", "row", "col"]
df = spark.createDataFrame(data).toDF(*cols)


def vectors_to_matrix(values: Iterable[float], n: int) -> DenseMatrix:
    """Convert  of vectors to a matrix.

    Args:
        values (Iterable[float]): The values of the matrix.
        n (int): The number of rows and columns of the matrix.

    Returns:
        DenseMatrix: A dense matrix.
    """
    return DenseMatrix(n, n, values)


vectors_to_matrix_udf = f.udf(
    lambda values, n: vectors_to_matrix(values, n), MatrixUDT()
)


w5 = Window.partitionBy("study").orderBy("row", "col")
w6 = Window.partitionBy("study", "row").orderBy("col")
df_m = (
    df.withColumn("index", f.collect_list(f.col("col")).over(w6))
    .withColumn("matrix", f.collect_list(f.col("value")).over(w5))
    .groupBy("study")
    .agg(
        f.max("index").alias("index"),
        vectors_to_matrix_udf(
            f.max(f.col("matrix")), f.size(f.max(f.col("index")))
        ).alias("matrix"),
    )
)
