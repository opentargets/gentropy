"""Explode LD matrix to allow and dump it into file."""

## Machine
# gcloud dataproc clusters create do-test-ld \
#         --image-version=2.1 \
#         --project=open-targets-genetics-dev \
#         --region=europe-west1 \
#         --zone="" \
#         --master-machine-type=n1-highmem-8 \
#         --autoscaling-policy="do-ld-explosion" \
#         --enable-component-gateway \
#         --initialization-actions=gs://genetics_etl_python_playground/initialisation/0.0.0/install_dependencies_on_cluster.sh \
# 		--metadata="PACKAGE=gs://genetics_etl_python_playground/initialisation/0.0.0/gentropy-0.0.0-py3-none-any.whl,CONFIGTAR=gs://genetics_etl_python_playground/initialisation/0.0.0/config.tar.gz" \
#         --secondary-worker-boot-disk-size=100GB \
#         --worker-boot-disk-size=2000GB \
#         --num-workers=4 \
#         --worker-machine-type=n1-highmem-8


import os

import hail as hl
import pyspark.sql.functions as f
from gentropy.common.session import Session
from gentropy.datasource.gnomad.ld import GnomADLDMatrix
from hail import __file__ as hail_location
from hail.linalg import BlockMatrix

session = Session(
    spark_uri="yarn",
    start_hail=True,
    hail_home=os.path.dirname(hail_location),
    extended_spark_conf={
        "spark.sql.shuffle.partitions": "3200",
    },
)

hl.init(sc=session.spark.sparkContext)

ld_matrix_path = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.nfe.common.adj.ld.bm"
ld_index_path = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.nfe.common.ld.variant_indices.ht"
grch37_to_grch38_chain_path = (
    "gs://hail-common/references/grch37_to_grch38.over.chain.gz"
)


# 14_192_032
ld_index = GnomADLDMatrix._process_variant_indices(
    hl.read_table(ld_index_path),
    grch37_to_grch38_chain_path,
)

ld = (
    BlockMatrix.read(ld_matrix_path)
    .entries(keyed=False)
    .to_spark()
    .withColumnRenamed("entry", "r")
).persist()

ld.join(
    f.broadcast(
        ld_index.select(
            f.col("variantId").alias("variantId_i"),
            f.col("idx").alias("i"),
        )
    ),
    on=["i"],
).repartitionByRange("j").join(
    f.broadcast(
        ld_index.select(
            f.col("variantId").alias("variantId_j"),
            f.col("idx").alias("j"),
            f.col("chromosome"),
        )
    ),
    on=["j"],
).drop("i", "j").write.partitionBy("chromosome").parquet(
    "gs://ot-team/dochoa/study_locus_expl_ld_27_02_2024.parquet"
)
