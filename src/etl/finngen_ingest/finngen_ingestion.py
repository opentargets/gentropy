"""FinnGen study table ingestion."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.request import urlopen

from pyspark.sql import functions as f

from etl.common.ontology import ontoma_udf

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession


def ingest_finngen_studies(
    etl: ETLSession,
    phenotype_table_url: str,
    finngen_release_prefix: str,
    sumstat_url_prefix: str,
    sumstat_url_suffix: str,
) -> DataFrame:
    """This function ingests study level metadata from FinnGen.

    The following information is aggregated/extracted:
    - Study ID in the special format (FINNGEN_R7_*)
    - Trait name (for example, Amoebiasis)
    - Number of cases and controls
    - Link to the summary statistics location

    Some fields are also populated as constants, such as study type and the initial sample size.

    Args:
        etl (ETLSession): ETLSession.
        phenotype_table_url (str): URL to the FinnGen API where the phenotype table can be ingested.
        finngen_release_prefix (str): FinnGen release prefix. This is used for the study ID construction.
        sumstat_url_prefix (str): URL prefix for constructing the sumstat file download path.
        sumstat_url_suffix (str): URL suffix for constructing the sumstat file download path.

    Returns:
        DataFrame: Parsed and annotated FinnGen study table.
    """
    # Read the JSON data from the URL.
    json_data = urlopen(phenotype_table_url).read().decode("utf-8")
    rdd = etl.spark.sparkContext.parallelize([json_data])
    df = etl.spark.read.json(rdd)

    # Select the desired columns.
    df = df.select("phenocode", "phenostring", "num_cases", "num_controls")

    # Rename the columns.
    df = df.withColumnRenamed("phenocode", "id")
    df = df.withColumnRenamed("phenostring", "traitFromSource")
    df = df.withColumnRenamed("num_cases", "nCases")
    df = df.withColumnRenamed("num_controls", "nControls")

    # Transform the column values.
    df = df.withColumn("id", f.concat(f.lit(finngen_release_prefix), df["id"]))
    df = df.withColumn("nSamples", df["nCases"] + df["nControls"])
    df = df.withColumn(
        "summarystatsLocation",
        f.concat(f.lit(sumstat_url_prefix), df["id"], f.lit(sumstat_url_suffix)),
    )

    # Set constant value columns.
    df = df.withColumn("type", f.lit("gwas"))
    df = df.withColumn(
        "initialSampleSize", f.lit("309,154 (173,746 females and 135,408 males)")
    )
    df = df.withColumn("hasSumstats", f.lit(True))

    # Compute the EFO mapping iformation.
    df = df.withColumn("efos", ontoma_udf(df["traitFromSource"]))

    return df
