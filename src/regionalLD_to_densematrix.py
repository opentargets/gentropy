"""Calculate densematrix from triangle."""


from typing import Iterable

import numpy as np
import pyspark.sql.functions as f
from gentropy.common.session import Session
from pyspark.ml.linalg import DenseMatrix, MatrixUDT

session = Session(
    spark_uri="yarn",
    app_name="regional_ld_to_densematrix",
    extended_spark_conf={
        "spark.sql.shuffle.partitions": "3200",
    },
)


regional_ld = session.spark.read.parquet(
    "gs://ot-team/dochoa/ld_exploded_bycluster_25_03_2024.parquet"
).select(
    f.xxhash64(f.col("variantId_i")).alias("i"),
    f.xxhash64(f.col("variantId_j")).alias("j"),
    f.col("r"),
)


def _vectors_to_matrix(
    i: Iterable[float], j: Iterable[float], r: Iterable[float]
) -> DenseMatrix:
    """Convert  of vectors to a matrix.

    Args:
        i (Iterable[float]): coordinates for i
        j (Iterable[float]): coordinates for j
        r (Iterable[float]): r value for i, j

    Returns:
        DenseMatrix: A squared dense matrix.
    """
    variant_list = list(set(i))
    variant_list.sort()
    variant_list_size = len(variant_list)
    variant_index = {value: i for i, value in enumerate(variant_list)}

    m = np.full((variant_list_size, variant_list_size), np.nan)

    for _i, _j, _r in zip(i, j, r):
        m[variant_index[_i], variant_index[_j]] = _r
        m[variant_index[_j], variant_index[_i]] = _r

    return DenseMatrix(variant_list_size, variant_list_size, m.flatten().tolist())


_vectors_to_matrix_udf = f.udf(
    lambda i, j, r,: _vectors_to_matrix(i, j, r), MatrixUDT()
)


regional_ld.groupBy(f.lit("clusterId")).agg(
    _vectors_to_matrix_udf(
        f.collect_list("i"), f.collect_list("j"), f.collect_list("r")
    ).alias("matrix"),
    f.array_sort(f.collect_set(f.col("i"))).alias("variants"),
).write.parquet("gs://ot-team/dochoa/regional_ld_densematrix_26_03_2024.parquet")
