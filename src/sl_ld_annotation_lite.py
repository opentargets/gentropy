"""LD annotation for studyLocus."""

import os

import hail as hl
import pyspark.sql.functions as f
from gentropy.common.session import Session
from hail import __file__ as hail_location
from pyspark.sql import Window

WINDOW_SIZE = 500_000
BUFFER = 50_000

session = Session(
    spark_uri="yarn",
    start_hail=True,
    app_name="ld_nfe_lite",
    hail_home=os.path.dirname(hail_location),
    extended_spark_conf={
        "spark.sql.shuffle.partitions": "8000",
    },
)

hl.init(sc=session.spark.sparkContext)


# ld_matrix_path = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.nfe.common.adj.ld.bm"

# ld = (
#     BlockMatrix.read(ld_matrix_path)
#     .entries(keyed=False)
#     .to_spark()
#     .withColumnRenamed("entry", "r")
#     .withColumn("r", f.col("r").cast(FloatType()))
#     .filter(f.col("r") != 0)
# ).persist()

# ld.write.parquet(
#     "gs://ot-team/dochoa/ld_exploded_lite_04_04_2024.parquet",
# )

# # Hail v2.1.1 variants lifted over
# ht = hl.read_table(
#     "gs://gcp-public-data--gnomad/release/2.1.1/liftover_grch38/ht/genomes/gnomad.genomes.r2.1.1.sites.liftover_grch38.ht"
# )
# ht = (
#     ht.select(
#         variantId=hl.str("_").join(
#             [
#                 ht.locus.contig.replace("chr", ""),
#                 hl.str(ht.locus.position),
#                 ht.alleles[0],
#                 ht.alleles[1],
#             ]
#         ),
#         chromosome=ht.locus.contig.replace("chr", ""),
#         position=ht.locus.position,
#         variantIdB37=hl.str("_").join(
#             [
#                 ht.original_locus.contig.replace("chr", ""),
#                 hl.str(ht.original_locus.position),
#                 ht.alleles[0],
#                 ht.alleles[1],
#             ]
#         ),
#     )
#     .select_globals()
#     .to_spark()
#     .drop("locus.contig", "locus.position", "alleles")
# )

# # Hail v2.1.1 variants lifted over
# ld_index_path = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.nfe.common.ld.variant_indices.ht"
# ld_vi = hl.read_table(ld_index_path)

# ld_vi = ld_vi.variantId = (
#     ld_vi.select(
#         variantIdB37=hl.str("_").join(
#             [
#                 ld_vi.locus.contig.replace("chr", ""),
#                 hl.str(ld_vi.locus.position),
#                 ld_vi.alleles[0],
#                 ld_vi.alleles[1],
#             ]
#         ),
#         chromosome=ld_vi.locus.contig.replace("chr", ""),
#         idx=ld_vi.idx,
#     )
#     .select_globals()
#     .to_spark()
#     .drop("locus.contig", "locus.position", "alleles")
# )

# vidx = (
#     ht.join(ld_vi, on=["chromosome", "variantIdB37"], how="inner")
#     .drop("variantIdB37")
#     .repartitionByRange("chromosome", "position")
#     .sortWithinPartitions("chromosome", "position")
# )

# vidx.write.parquet("gs://ot-team/dochoa/indexing_04_04_2024.parquet")


# ## Study locus clumping and regional information
# sl = StudyLocus.from_parquet(session, path="gs://ot-team/dochoa/sl_25_3_24.parquet")

# clumped_sl = (
#     sl.df.withColumn("studyId", f.lit("dummy"))
#     .withColumn(
#         "clusterId",
#         WindowBasedClumping._cluster_peaks(
#             f.col("studyId"), f.col("chromosome"), f.col("position"), BUFFER
#         ),
#     )
#     .groupBy("clusterId")
#     .agg(
#         f.collect_set(f.col("studyLocusId")).alias("studyLocusIds"),
#         (f.first(f.col("position")) - BUFFER - WINDOW_SIZE).alias("start"),
#         (f.first(f.col("position")) + BUFFER + WINDOW_SIZE).alias("end"),
#         f.first("chromosome").alias("chromosome"),
#     )
#     .persist()
# )

# clumped_sl.write.parquet("gs://ot-team/dochoa/sl_cluster_lut_25_03_2024.parquet")

## temp
clumped_sl = session.spark.read.parquet(
    "gs://ot-team/dochoa/sl_cluster_lut_25_03_2024.parquet"
)
vidx = session.spark.read.parquet("gs://ot-team/dochoa/indexing_04_04_2024.parquet")
###

w_c = Window.partitionBy("clusterId").orderBy("idx")
cluster_idxs = (
    vidx.alias("vi")
    .join(
        f.broadcast(clumped_sl.alias("sl_cluster")),
        on=[
            f.col("vi.chromosome") == f.col("sl_cluster.chromosome"),
            f.col("vi.position") > f.col("sl_cluster.start"),
            f.col("vi.position") < f.col("sl_cluster.end"),
        ],
    )
    .select("clusterId", "idx", f.dense_rank().over(w_c).alias("cidx"))
    .persist()
)


ld = session.spark.read.parquet(
    "gs://ot-team/dochoa/ld_exploded_lite_04_04_2024.parquet"
)

ld.alias("ld").join(
    cluster_idxs.alias("cluster"),
    on=[f.col("ld.i") == f.col("cluster.idx")],
    how="inner",
).withColumnRenamed("cidx", "c_i").alias("ld").join(
    cluster_idxs.alias("cluster"),
    on=[
        f.col("ld.j") == f.col("cluster.idx"),
        f.col("ld.clusterId") == f.col("cluster.clusterId"),
    ],
).withColumnRenamed("cidx", "c_j").select(
    "cluster.clusterId", "c_i", "c_j", "r"
).write.partitionBy("clusterId").parquet(
    "gs://ot-team/dochoa/ld_exploded_bycluster_lite_04_04_2024.parquet"
)

# Read LD information
# ld = (
#     session.spark.read.parquet("gs://ot-team/dochoa/ld_exploded_25_03_2024.parquet")
#     .withColumn("position_i", f.split(f.col("variantId_i"), "_")[1].cast("int"))
#     .withColumn("position_j", f.split(f.col("variantId_j"), "_")[1].cast("int"))
# )


# (
#     ld.alias("ld")
#     .join(
#         f.broadcast(clumped_sl.alias("sl")),
#         on=[
#             f.col("sl.chromosome") == f.col("ld.chromosome"),
#             f.col("position_i") > f.col("sl.start"),
#             f.col("position_i") < f.col("sl.end"),
#             f.col("position_j") > f.col("sl.start"),
#             f.col("position_j") < f.col("sl.end"),
#             # f.array_contains("variantIdArray", f.col("variantId_i")),
#             # f.array_contains("variantIdArray", f.col("variantId_j")),
#         ],
#     )
#     .select(
#         "clusterId",
#         f.col("variantId_i"),
#         f.col("variantId_j"),
#         f.col("r"),
#     )
#     .write.partitionBy("clusterId")
#     .parquet("gs://ot-team/dochoa/ld_exploded_bycluster_25_03_2024.parquet")
# )
