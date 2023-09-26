"""Datasource ingestion: FinnGen."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.request import urlopen

import pyspark
import pyspark.sql.functions as f

from otg.common.schemas import parse_spark_schema
from otg.dataset.study_index import StudyIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


@dataclass
class StudyIndexFinnGen(StudyIndex):
    """Study index dataset from FinnGen.

    The following information is aggregated/extracted:

    - Study ID in the special format (FINNGEN_R9_*)
    - Trait name (for example, Amoebiasis)
    - Number of cases and controls
    - Link to the summary statistics location

    Some fields are also populated as constants, such as study type and the initial sample size.
    """

    @classmethod
    def get_schema(cls: type[StudyIndexFinnGen]) -> StructType:
        """Provides the schema for the StudyIndexFinnGen dataset.

        This method is a duplication from the parent class, but by definition, the use of abstract methods require that every child class implements them.

        Returns:
            StructType: Spark schema for the StudyIndexFinnGen dataset.
        """
        return parse_spark_schema("studies.json")

    @classmethod
    def from_source(
        cls: type[StudyIndexFinnGen],
        finngen_studies: DataFrame,
        finngen_release_prefix: str,
        finngen_sumstat_url_prefix: str,
        finngen_sumstat_url_suffix: str,
    ) -> StudyIndexFinnGen:
        """This function ingests study level metadata from FinnGen.

        Args:
            finngen_studies (DataFrame): FinnGen raw study table
            finngen_release_prefix (str): Release prefix pattern.
            finngen_sumstat_url_prefix (str): URL prefix for summary statistics location.
            finngen_sumstat_url_suffix (str): URL prefix suffix for summary statistics location.

        Returns:
            StudyIndexFinnGen: Parsed and annotated FinnGen study table.
        """
        return cls(
            _df=finngen_studies.select(
                f.concat(f.lit(f"{finngen_release_prefix}_"), f.col("phenocode")).alias(
                    "studyId"
                ),
                f.col("phenostring").alias("traitFromSource"),
                f.col("num_cases").alias("nCases"),
                f.col("num_controls").alias("nControls"),
                f.lit(finngen_release_prefix).alias("projectId"),
                f.lit("gwas").alias("studyType"),
                f.lit(True).alias("hasSumstats"),
                f.lit("377,277 (210,870 females and 166,407 males)").alias(
                    "initialSampleSize"
                ),
            )
            .withColumn("nSamples", f.col("nCases") + f.col("nControls"))
            .withColumn(
                "summarystatsLocation",
                f.concat(
                    f.lit(finngen_sumstat_url_prefix),
                    f.col("studyId"),
                    f.lit(finngen_sumstat_url_suffix),
                ),
            ),
            _schema=cls.get_schema(),
        )


def ingest_finngen(
    finngen_phenotype_table_url,
    finngen_release_prefix,
    finngen_sumstat_url_prefix,
    finngen_sumstat_url_suffix,
    finngen_study_index_out,
    spark_write_mode,
):
    """Datasource ingestion: FinnGen.

    Args:
        finngen_phenotype_table_url (str): FinnGen API for fetching the list of studies.
        finngen_release_prefix (str): Release prefix pattern.
        finngen_sumstat_url_prefix (str): URL prefix for summary statistics location.
        finngen_sumstat_url_suffix (str): URL prefix suffix for summary statistics location.
        finngen_study_index_out (str): Output path for the FinnGen study index dataset.
        spark_write_mode (str): Dataframe write mode.
    """
    spark = pyspark.SparkContext()

    # Read the JSON data from the URL.
    json_data = urlopen(finngen_phenotype_table_url).read().decode("utf-8")
    rdd = spark.sparkContext.parallelize([json_data])
    df = spark.read.json(rdd)

    # Parse the study index data.
    finngen_studies = StudyIndexFinnGen.from_source(
        df,
        finngen_release_prefix,
        finngen_sumstat_url_prefix,
        finngen_sumstat_url_suffix,
    )

    # Write the output.
    finngen_studies.df.write.mode(spark_write_mode).parquet(finngen_study_index_out)


if __name__ == "__main__":
    ingest_finngen(*sys.argv[1:])
