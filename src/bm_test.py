import os

import hail as hl
import pyspark.sql.functions as f
from gentropy.common.session import Session
from gentropy.dataset.study_locus import StudyLocus
from hail import __file__ as hail_location
from hail.linalg import BlockMatrix
from pyspark.ml.linalg import DenseMatrix, MatrixUDT

session = Session(
    spark_uri="yarn",
    start_hail=True,
    app_name="ld_nfe",
    hail_home=os.path.dirname(hail_location),
    # extended_spark_conf={
    #     "spark.sql.shuffle.partitions": "8000",
    # },
)

hl.init(sc=session.spark.sparkContext)


ld_matrix_path = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.nfe.common.adj.ld.bm"
ld_index_path = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.nfe.common.ld.variant_indices.ht"
grch37_to_grch38_chain_path = (
    "gs://hail-common/references/grch37_to_grch38.over.chain.gz"
)

bm = BlockMatrix.read(ld_matrix_path)

sl = StudyLocus.from_parquet(session, path="gs://ot-team/dochoa/sl_25_3_24.parquet")


@f.udf(returnType=MatrixUDT())
def query_bm(bm: BlockMatrix, start: int, end: int) -> DenseMatrix:
    """Query a block matrix.

    Args:
        bm (BlockMatrix): BlockMatrix
        start (int): start index
        end (int): end index

    Returns:
        BlockMatrix: A block matrix.
    """
    return DenseMatrix(99, 99, bm[1:100, 1:100].to_numpy().flatten())


# Create dataframe with start and end positions
df = session.spark.createDataFrame(
    data=[(1, 100), (300, 400), (600, 700)], schema=["start", "end"]
).withColumn("matrix", query_bm(bm, f.col("start"), f.col("end")))
