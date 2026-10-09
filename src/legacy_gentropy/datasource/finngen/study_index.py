"""Study Index for FinnGen data source."""

from __future__ import annotations

import re
from typing import TypedDict
from urllib.request import urlopen

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from gentropy.dataset.study_index import StudyIndex


class FinngenPrefixMatch(TypedDict):
    """Class to store the output of the validate_release_prefix."""

    prefix: str
    release: str


class FinnGenStudyIndex:
    """Study index dataset from FinnGen.

    The following information is aggregated/extracted:

    - Study ID in the special format (e.g. FINNGEN_R11_*)
    - Trait name (for example, Amoebiasis)
    - Number of cases and controls
    - Link to the summary statistics location
    - EFO mapping from curated EFO mapping file

    Some fields are also populated as constants, such as study type and the initial sample size.
    """

    CONSTANTS = {
        "studyType": "gwas",
        "hasSumstats": True,
        "initialSampleSize": "500,348 (282,064 females and 218,284 males)",
        "pubmedId": "36653562",
    }

    @staticmethod
    def validate_release_prefix(release_prefix: str) -> FinngenPrefixMatch:
        """Validate release prefix passed to finngen StudyIndex.

        Args:
            release_prefix (str): Finngen release prefix, should be a string like FINNGEN_R*.

        Returns:
            FinngenPrefixMatch: Object containing valid prefix and release strings.

        Raises:
            ValueError: when incorrect release prefix is provided.

        This method ensures that the trailing underscore is removed from prefix.
        """
        pattern = re.compile(r"FINNGEN_(?P<release>R\d+){1}_?")
        pattern_match = pattern.match(release_prefix)
        if not pattern_match:
            raise ValueError(
                f"Invalid FinnGen release prefix: {release_prefix}, use the format FINNGEN_R*"
            )
        release = pattern_match.group("release").upper()
        if release_prefix.endswith("_"):
            release_prefix = release_prefix[:-1]
        return FinngenPrefixMatch(prefix=release_prefix, release=release)

    @classmethod
    def from_source(
        cls: type[FinnGenStudyIndex],
        spark: SparkSession,
        finngen_phenotype_table_url: str,
        finngen_release_prefix: str,
        finngen_summary_stats_url_prefix: str,
        finngen_summary_stats_url_suffix: str,
        sample_size: int,
    ) -> StudyIndex:
        """This function ingests study level metadata from FinnGen.

        Args:
            spark (SparkSession): Spark session object.
            finngen_phenotype_table_url (str): URL to the FinnGen phenotype table.
            finngen_release_prefix (str): FinnGen release prefix.
            finngen_summary_stats_url_prefix (str): FinnGen summary stats URL prefix.
            finngen_summary_stats_url_suffix (str): FinnGen summary stats URL suffix.
            sample_size (int): Number of individuals participated in sample collection.

        Returns:
            StudyIndex: Parsed and annotated FinnGen study table.
        """
        json_data = urlopen(finngen_phenotype_table_url).read().decode("utf-8")
        rdd = spark.sparkContext.parallelize([json_data])
        raw_df = spark.read.json(rdd)

        return StudyIndex(
            _df=raw_df.select(
                f.concat(
                    f.concat_ws("_", f.lit(finngen_release_prefix), f.col("phenocode"))
                ).alias("studyId"),
                f.col("phenostring").alias("traitFromSource"),
                f.col("num_cases").cast("integer").alias("nCases"),
                f.col("num_controls").cast("integer").alias("nControls"),
                (f.col("num_cases") + f.col("num_controls"))
                .cast("integer")
                .alias("nSamples"),
                f.array(
                    f.struct(
                        f.lit(sample_size).cast("integer").alias("sampleSize"),
                        f.lit("Finnish").alias("ancestry"),
                    )
                ).alias("discoverySamples"),
                # Cohort label is consistent with GWAS Catalog curation.
                f.array(f.lit("FinnGen")).alias("cohorts"),
                f.concat(
                    f.lit(finngen_summary_stats_url_prefix),
                    f.col("phenocode"),
                    f.lit(finngen_summary_stats_url_suffix),
                ).alias("summarystatsLocation"),
                f.lit(finngen_release_prefix).alias("projectId"),
                *[f.lit(value).alias(key) for key, value in cls.CONSTANTS.items()],
            ).withColumn(
                "ldPopulationStructure",
                StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")),
            ),
            _schema=StudyIndex.get_schema(),
        )
