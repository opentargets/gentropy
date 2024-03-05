## Trying to get pyspark
##

import pyspark.sql.functions as f
from clump_temp import WindowBasedClumping
from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics

WINDOW_SIZE = 500_000
NUM_STUDIES = 500

session = Session(
    spark_uri="yarn",
    extended_spark_conf={
        "spark.sql.shuffle.partitions": "3200",
    },
)

# Read LD information
ld = (
    session.spark.read.parquet(
        "gs://ot-team/dochoa/study_locus_expl_ld_27_02_2024.parquet"
    )
    .withColumn("position_i", f.split(f.col("variantId_i"), "_")[1].cast("int"))
    .withColumn("position_j", f.split(f.col("variantId_j"), "_")[1].cast("int"))
)

# Summary statistics from GCST006907 (not so big study)
ss = SummaryStatistics.from_parquet(
    session,
    path=["gs://gwas_catalog_data/harmonised_summary_statistics/GCST006907.parquet"],
).exclude_region("6:28510120-33480577")

# Clump imported from refactored function
sl = WindowBasedClumping.clump(
    ss, distance=WINDOW_SIZE, collect_locus=True, collect_locus_distance=WINDOW_SIZE
)


(
    ld.alias("ld")
    .join(
        f.broadcast(
            sl.df.coalesce(1)
            .alias("sl")
            .select("studyLocusId", "chromosome", "position")
        ),
        on=[
            f.col("sl.chromosome") == f.col("ld.chromosome"),
            f.col("position_i") > (f.col("sl.position") - WINDOW_SIZE),
            f.col("position_i") < (f.col("sl.position") + WINDOW_SIZE),
            f.col("position_j") > (f.col("sl.position") - WINDOW_SIZE),
            f.col("position_j") < (f.col("sl.position") + WINDOW_SIZE),
            # f.array_contains(
            #     f.transform("locus", lambda x: x.variantId), f.col("variantId_i")
            # ),
            # f.array_contains(
            #     f.transform("locus", lambda x: x.variantId), f.col("variantId_j")
            # ),
        ],
    )
    .select(
        "studyLocusId",
        f.col("variantId_i"),
        f.col("variantId_j"),
        f.col("r"),
    )
    .write.partitionBy("studyLocusId")
    .parquet("gs://ot-team/dochoa/sl_ld_annotation_4_03_2024.parquet", mode="overwrite")
)

df = session.spark.read.parquet(
    "gs://ot-team/dochoa/sl_ld_annotation_4_03_2024.parquet"
)
#
import pyspark.ml.functions as fml
from pyspark.sql import Window

w_j = Window.partitionBy("studyLocusId").orderBy("position_j")
w_i = Window.partitionBy("studyLocusId").orderBy("position_i")
(
    df.withColumn("position_i", f.split(f.col("variantId_i"), "_")[1].cast("int"))
    .withColumn("position_j", f.split(f.col("variantId_j"), "_")[1].cast("int"))
    .withColumn("vector_j", fml.array_to_vector(f.collect_list(f.col("r")).over(w_j)))
    .groupBy("studyLocusId", "position_i")
    .agg(f.max("vector_j").alias("vector_j"))
    .withColumn("vector_i", f.collect_list(f.col("vector_j")).over(w_i))
    .groupBy("studyLocusId")
    .agg(f.max("vector_i").alias("vector_i"))
    .printSchema()
)


# (
#     ld.alias("ld")
#     .join(
#         f.broadcast(
#             sl.df.coalesce(4)
#             .withColumn("expl_locus", f.explode("locus"))
#             .repartition("studyLocusId", "chromosome")
#             .alias("sl")
#             .select(
#                 "studyLocusId",
#                 "chromosome",
#                 "variantId",
#                 f.col("expl_locus.variantId").alias("tagVariantId"),
#                 "expl_locus.beta",
#             )
#         ),
#         on=[
#             f.col("sl.chromosome") == f.col("ld.chromosome"),
#             f.col("position_i") > (f.col("sl.position") - WINDOW_SIZE),
#             f.col("position_i") < (f.col("sl.position") + WINDOW_SIZE),
#             f.col("position_j") > (f.col("sl.position") - WINDOW_SIZE),
#             f.col("position_j") < (f.col("sl.position") + WINDOW_SIZE),
#         ],
#     )
#     .select(
#         "studyLocusId",
#         f.col("variantId_i"),
#         f.col("variantId_j"),
#         f.col("r"),
#     )
#     .write.partitionBy("studyLocusId")
#     .parquet("gs://ot-team/dochoa/sl_ld_annotation_4_03_2024.parquet", mode="overwrite")
# )
