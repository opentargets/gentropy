"""LD annotation for studyLocus."""
import pyspark.sql.functions as f
from gentropy.common.session import Session
from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.window_based_clumping import WindowBasedClumping

WINDOW_SIZE = 500_000
BUFFER = 50_000

session = Session(
    spark_uri="yarn",
    app_name="clumped_ld_clusters",
    extended_spark_conf={
        "spark.sql.shuffle.partitions": "3200",
    },
)

# Read LD information
ld = (
    session.spark.read.parquet("gs://ot-team/dochoa/ld_exploded_25_03_2024.parquet")
    .withColumn("position_i", f.split(f.col("variantId_i"), "_")[1].cast("int"))
    .withColumn("position_j", f.split(f.col("variantId_j"), "_")[1].cast("int"))
)

sl = StudyLocus.from_parquet(session, path="gs://ot-team/dochoa/sl_25_3_24.parquet")

clumped_sl = (
    sl.df.withColumn("studyId", f.lit("dummy"))
    .withColumn(
        "clusterId",
        WindowBasedClumping._cluster_peaks(
            f.col("studyId"), f.col("chromosome"), f.col("position"), BUFFER
        ),
    )
    .groupBy("clusterId")
    .agg(
        f.collect_set(f.col("studyLocusId")).alias("studyLocusIds"),
        (f.first(f.col("position")) - BUFFER - WINDOW_SIZE).alias("start"),
        (f.first(f.col("position")) + BUFFER + WINDOW_SIZE).alias("end"),
        f.first("chromosome").alias("chromosome"),
    )
    .repartitionByRange("chromosome", "start")
    .sortWithinPartitions("chromosome", "start")
    .persist()
)

clumped_sl.write.parquet("gs://ot-team/dochoa/sl_cluster_lut_25_03_2024.parquet")

(
    ld.alias("ld")
    .join(
        f.broadcast(clumped_sl.alias("sl")),
        on=[
            f.col("sl.chromosome") == f.col("ld.chromosome"),
            f.col("position_i") > f.col("sl.start"),
            f.col("position_i") < f.col("sl.end"),
            f.col("position_j") > f.col("sl.start"),
            f.col("position_j") < f.col("sl.end"),
            # f.array_contains("variantIdArray", f.col("variantId_i")),
            # f.array_contains("variantIdArray", f.col("variantId_j")),
        ],
    )
    .select(
        "clusterId",
        f.col("variantId_i"),
        f.col("variantId_j"),
        f.col("r"),
    )
    .write.partitionBy("clusterId")
    .parquet("gs://ot-team/dochoa/ld_exploded_bycluster_25_03_2024.parquet")
)
