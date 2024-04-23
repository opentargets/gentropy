"""Coordinate reshape of the matrixes."""
import numpy as np
import pyspark.sql.functions as f
import pyspark.sql.types as t
from gentropy.common.session import Session
from pyspark.ml.linalg import (
    DenseVector,
)
from pyspark.sql import Window

session = Session(
    spark_uri="yarn",
    # start_hail=True,
    extended_spark_conf={
        "spark.sql.shuffle.partitions": "8000",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    },
    # extended_spark_conf={
    #     # "spark.driver.memory": "10g",
    #     # "spark.executor.memory": "14g",
    #     # "spark.memory.fraction", 0.8,
    #     # "spark.kryoserializer.buffer.max": "250m",
    # },
)
# df = session.spark.read.parquet(
#     "/Users/ochoa/Datasets/clusterId=dummy_21_44178639",
# ).withColumn("r", f.col("r").cast(t.FloatType()))
df = (
    session.spark.read.parquet(
        "gs://ot-team/dochoa/ld_exploded_bycluster_25_03_2024.parquet/",
    )
    .withColumn("r", f.col("r").cast(t.FloatType()))
    .filter(f.col("clusterId").contains("dummy_21_3"))
)

# df = session.spark.read.parquet("/Users/ochoa/Downloads/clusterId=dummy_1_145023036")


# def _colpointers(colname: str) -> Column:
#     """Convert row index to row pointers used in SparseMatrix.

#     Args:
#         colname (str): Column of indices to be turned into column pointers

#     Returns:
#         Column: row pointers
#     """
#     return f.expr(
#         f"array_union(transform(array_distinct({colname}), x -> array_position({colname}, x) - 1), array(size(array_distinct({colname}))))"
#     )


# def _row_index_to_row_pointers(rows: np.ndarray[int]) -> list[int]:
#     """Convert row index to row pointers.

#     Args:
#         rows (list[int]): row index

#     Returns:
#         Iterable[int]: row pointers
#     """
#     # You need to use this instead of the length of the unique elements
#     # Rows with no non-zero values are still rows
#     # Even this will miss all-zero rows at the bottom of the matrix
#     n_rows = max(rows) + 1
#     # Create a pointer list
#     indptr = [0] * n_rows
#     # Count the number of values in each row
#     for i in rows:
#         indptr[i] += 1
#     # Do a cumsum
#     for i in range(n_rows - 1):
#         indptr[i + 1] += indptr[i]
#     # Add a zero on the front of the pointer list
#     # If that's the style of indptr you're doing
#     intptr = [0] + indptr

#     return intptr


# def _row_float_index_to_row_pointers(rows: np.ndarray[np.float64]) -> list[int]:
#     """Convert row index to row pointers.

#     Args:
#         rows (list[int]): row index

#     Returns:
#         Iterable[int]: row pointers
#     """
#     rows = rows.astype(np.int64)
#     # You need to use this instead of the length of the unique elements
#     # Rows with no non-zero values are still rows
#     # Even this will miss all-zero rows at the bottom of the matrix
#     n_rows = max(rows) + 1
#     # Create a pointer list
#     indptr = [0] * n_rows
#     # Count the number of values in each row
#     for i in rows:
#         indptr[i] += 1
#     # Do a cumsum
#     for i in range(n_rows - 1):
#         indptr[i + 1] += indptr[i]
#     # Add a zero on the front of the pointer list
#     # If that's the style of indptr you're doing
#     intptr = [0] + indptr

#     return intptr


# @f.udf(returnType=MatrixUDT())
# def _vectors_to_matrix(
#     i: np.ndarray[int], j: np.ndarray[int], r: np.ndarray[np.float64], n: int
# ) -> SparseMatrix | None:
#     """Convert  vectors to a matrix.

#     Args:
#         i (list[int]): column pointers (see scipy csc_matrix documentation)
#         j (list[int]): coordinates for j
#         r (list[float]): r value for i, j
#         n (int): size of the matrix

#     Returns:
#         DenseMatrix: A squared dense matrix.
#     """
#     return SparseMatrix(
#         numRows=n,
#         numCols=n,
#         colPtrs=_row_index_to_row_pointers(i),
#         rowIndices=j,
#         values=r,
#     )


@f.udf(returnType=t.ArrayType(t.ArrayType(t.FloatType())))
def _columns_to_coords(
    i: np.ndarray[int], j: np.ndarray[int], r: np.ndarray[np.float64]
) -> DenseVector:
    """Convert 3 vectors to a scipy coo sparse matrix.

    Args:
        i (list[int]): coordinates for i
        j (list[int]): coordinates for j
        r (list[float]): r value for i, j

    Returns:
        DenseMatrix: A squared dense matrix.
    """
    from scipy.sparse import coo_matrix

    return coo_matrix((r, (i, j))).toarray().tolist()


# indexed_df = df.withColumn("clusterId", f.lit("dummy")).withColumn(
#     "r", f.col("r").cast(t.FloatType())
# )

indexer_df = (
    df.groupBy("clusterId")
    .agg(f.collect_set("variantId_i").alias("indexMap"))
    .withColumn(
        "ldMap",
        f.map_from_arrays(
            f.col("indexMap"), f.sequence(f.lit(0), f.size("indexMap") - 1)
        ),
    )
    .withColumn(
        "tmp",
        f.explode(
            f.arrays_zip(
                f.map_keys("ldMap").alias("variantId"),
                f.map_values("ldMap").alias("idx"),
            )
        ),
    )
    .select(
        "clusterId",
        f.col("tmp.variantId").alias("variantId"),
        f.col("tmp.idx").alias("idx"),
    )
).persist()

w = Window.partitionBy("clusterId").orderBy("clusterId", "i", "j")
out = (
    df.join(
        indexer_df.withColumnRenamed("variantId", "variantId_i"),
        on=[
            "clusterId",
            "variantId_i",
        ],
    )
    .withColumnRenamed("idx", "i")
    .join(
        indexer_df.withColumnRenamed("variantId", "variantId_j"),
        on=[
            "clusterId",
            "variantId_j",
        ],
    )
    .withColumnRenamed("idx", "j")
    .select("clusterId", "i", "j", "r", "variantId_i")
    .sort("clusterId", "i", "j")
    .withColumn("id", f.monotonically_increasing_id())
    .groupBy("clusterId")
    .agg(
        f.sort_array(f.collect_list(f.struct("id", "i", "j", "r"))).alias("struct"),
        f.collect_set("variantId_i").alias("ldMap"),
    )
    .select(
        "clusterId",
        f.map_from_arrays("ldMap", f.sequence(f.lit(0), f.size("ldMap") - 1)).alias(
            "ldMap"
        ),
        _columns_to_coords(
            f.col("struct.i"), f.col("struct.j"), f.col("struct.r")
        ).alias("matrix"),
    )
)

# out.show()
out.printSchema()

out.write.partitionBy("clusterId").parquet(
    "gs://ot-team/dochoa/cluster_matrixes_chr21_36.parquet", mode="overwrite"
)
