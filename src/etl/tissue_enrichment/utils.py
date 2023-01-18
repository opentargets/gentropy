"""Utils to process datasets for tissue enrichment."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def load_snps(spark: SparkSession, in_path: str) -> DataFrame:
    """Util command to read input credible SNPs.

    Args:
        spark (SparkSession): Available spark session.
        in_path (str): Input path to credible SNPs.

    Returns:
        DataFrame: Contains input credible SNPs.
    """
    import_schema = StructType(
        [
            StructField("study_id", StringType(), False),
            StructField("snp_id", StringType(), False),
            StructField("chrom", StringType(), False),
            StructField("pos", IntegerType(), False),
        ]
    )
    snps = spark.read.format("parquet").load(in_path, schema=import_schema)
    return snps


def load_peaks(spark: SparkSession, in_path: str) -> DataFrame:
    """Util command to read input tissue annotations.

    Args:
        spark (SparkSession): Available spark session.
        in_path (str): Input path to tissue annotations.

    Returns:
        DataFrame: Contains tissue annotations and the associated
        signal strength found within each one.
    """
    peaks_wide = spark.read.csv(
        in_path, sep="\t", header=True, inferSchema=True
    ).repartitionByRange("chr", "start", "end")

    return peaks_wide
