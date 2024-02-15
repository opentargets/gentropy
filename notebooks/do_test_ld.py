"""Exploring explosion of the LD matrix to allow finemapping."""

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
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.datasource.gnomad.ld import GnomADLDMatrix
from hail import __file__ as hail_location
from hail.linalg import BlockMatrix
from pyspark.storagelevel import StorageLevel

WINDOW_SIZE = 1_000_000


session = Session(
    spark_uri="yarn",
    start_hail=True,
    hail_home=os.path.dirname(hail_location),
    extended_spark_conf={"spark.driver.memory": "5G"},
)

hl.init(sc=session.spark.sparkContext)

ld_matrix_path = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.nfe.common.adj.ld.bm"
ld_index_path = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.nfe.common.ld.variant_indices.ht"
grch37_to_grch38_chain_path = (
    "gs://hail-common/references/grch37_to_grch38.over.chain.gz"
)


# NFE GWAS Catalog: total 52_081 out of 80_587 of GWAS Catalog
# TODO: add filter by % of NFE individuals
nfe_studies = StudyIndex.from_parquet(
    session,
    "gs://genetics_etl_python_playground/releases/24.01/study_index/gwas_catalog/",
).df.filter(
    f.array_contains(
        "ldPopulationStructure",
        f.struct(
            f.lit("nfe").alias("ldPopulation"), f.lit(1.0).alias("relativeSampleSize")
        ),
    )
)

# 13531 out of 15987 of GWAS Catalog summary statistics are ONLY NFE
studies = nfe_studies.join(
    session.spark.read.parquet(
        "gs://gwas_catalog_data/manifests/gwas_catalog_summary_statistics_included_studies/"
    ),
    on="studyId",
)
study_list = studies.rdd.map(lambda x: x.studyId).collect()

ss = SummaryStatistics.from_parquet(
    session,
    path=[
        "gs://gwas_catalog_data/harmonised_summary_statistics/" + i + ".parquet"
        for i in study_list
    ][0:1000],
).exclude_region("6:28510120-33480577")


# root
#  |-- studyId: string (nullable = true)
#  |-- variantId: string (nullable = true)
#  |-- chromosome: string (nullable = true)
#  |-- position: integer (nullable = true)
#  |-- beta: double (nullable = true)
#  |-- sampleSize: integer (nullable = true)
#  |-- pValueMantissa: float (nullable = true)
#  |-- pValueExponent: integer (nullable = true)
#  |-- effectAlleleFrequencyFromSource: float (nullable = true)
#  |-- standardError: double (nullable = true)

# StudyLocus count in all NFE GWAS Catalog summary statistics: 154_715
sl = ss.window_based_clumping(
    distance=WINDOW_SIZE, locus_collect_distance=WINDOW_SIZE, baseline_significance=1
)

# root
#  |-- studyLocusId: long (nullable = false)
#  |-- studyId: string (nullable = true)
#  |-- variantId: string (nullable = true)
#  |-- chromosome: string (nullable = true)
#  |-- position: integer (nullable = true)
#  |-- beta: double (nullable = true)
#  |-- sampleSize: integer (nullable = true)
#  |-- pValueMantissa: float (nullable = true)
#  |-- pValueExponent: integer (nullable = true)
#  |-- effectAlleleFrequencyFromSource: float (nullable = true)
#  |-- standardError: double (nullable = true)
#  |-- qualityControls: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- locus: array (nullable = false)
#  |    |-- element: struct (containsNull = false)
#  |    |    |-- variantId: string (nullable = true)
#  |    |    |-- beta: double (nullable = true)
#  |    |    |-- pValueMantissa: float (nullable = true)
#  |    |    |-- pValueExponent: integer (nullable = true)
#  |    |    |-- standardError: double (nullable = true)

sl.df.select(f.size("locus").alias("locus_size")).sort(f.col("locus_size").desc()).show(
    100
)

sl_variants = (
    sl.df.select("chromosome", "position", f.explode(f.col("locus")).alias("expl"))
    .select("chromosome", "position", f.col("expl.variantId").alias("variantId"))
    .distinct()
    .repartitionByRange("chromosome", "position")
    .sortWithinPartitions("chromosome", "position")
    .persist(StorageLevel.MEMORY_AND_DISK_DESER)
)

sl_variants.write.parquet("/home/ochoa/gentropy/variants_df.parquet")

# 14_192_032
ld_index = GnomADLDMatrix._process_variant_indices(
    hl.read_table(ld_index_path),
    grch37_to_grch38_chain_path,
)
# root
#  |-- chromosome: string (nullable = true)
#  |-- position: integer (nullable = true)
#  |-- variantId: string (nullable = false)
#  |-- idx: long (nullable = true)

(
    ld_index.repartitionByRange("chromosome", "position")
    .sortWithinPartitions("chromosome", "position")
    .join(f.broadcast(sl_variants), on=["chromosome", "position"])
    .count()
)

# root
#  |-- studyLocusId: long (nullable = false)
#  |-- studyId: string (nullable = true)
#  |-- variantId: string (nullable = true)
#  |-- chromosome: string (nullable = true)
#  |-- position: integer (nullable = true)
#  |-- beta: double (nullable = true)
#  |-- sampleSize: integer (nullable = true)
#  |-- pValueMantissa: float (nullable = true)
#  |-- pValueExponent: integer (nullable = true)
#  |-- effectAlleleFrequencyFromSource: float (nullable = true)
#  |-- standardError: double (nullable = true)
#  |-- qualityControls: array (nullable = true)
#  |    |-- element: string (containsNull = true)
#  |-- locus: array (nullable = false)
#  |    |-- element: struct (containsNull = false)
#  |    |    |-- variantId: string (nullable = true)
#  |    |    |-- beta: double (nullable = true)
#  |    |    |-- pValueMantissa: float (nullable = true)
#  |    |    |-- pValueExponent: integer (nullable = true)
#  |    |    |-- standardError: double (nullable = true)


# Dataset #1: ld_index that is covered by the windows around the clumped variants

# Count <
# ???

ld = (
    BlockMatrix.read(ld_matrix_path)
    .entries(keyed=False)
    .to_spark()
    .withColumnRenamed("entry", "r")
)

# root
#  |-- i: long (nullable = true)
#  |-- j: long (nullable = true)
#  |-- r: double (nullable = true)


#  |-- studyLocusId: long (nullable = false)
#  |-- beta: double (nullable = true)
#  |-- pValueMantissa: float (nullable = true)
#  |-- pValueExponent: integer (nullable = true)
#  |-- standardError: double (nullable = true)


# root
#  |-- chromosome: string (nullable = true)
#  |-- position_i: integer (nullable = true)
#  |-- position_j: integer (nullable = true)
#  |-- variantId_i: string (nullable = false)
#  |-- variantId_j: string (nullable = false)
#  |-- r: double (nullable = true)
# 150B

# Option A
#  |-- studyLocusId: long (nullable = false)
#  |-- chromosome: string (nullable = true)
#  |-- lead_postion: string (nullable = true)

# Option B
#  |-- studyLocusId: long (nullable = false)
#  |-- postion: string (nullable = true)
#  |-- beta: double (nullable = true)

#  |-- tagVariantId: string (nullable = true)
#  |-- zscore: vector (nullable = true)


#  |-- studyLocusId: long (nullable = false)
#  |-- tagVariantId: string (nullable = true)
