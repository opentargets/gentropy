"""Study Index for FinnGen data source."""

from __future__ import annotations

import re
from typing import TypedDict
from urllib.request import urlopen

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession

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

    @staticmethod
    def read_efo_curation(session: SparkSession, url: str) -> DataFrame:
        """Read efo curation from provided url.

        Args:
            session (SparkSession): Session to use when reading the mapping file.
            url (str): Url to the mapping file. The file provided should be a tsv file.

        Returns:
            DataFrame: DataFrame with EFO mappings.

        Example of the file can be found in https://raw.githubusercontent.com/opentargets/curation/refs/heads/master/mappings/disease/manual_string.tsv.
        """
        csv_data = urlopen(url).readlines()
        csv_rows = [row.decode("utf8") for row in csv_data]
        rdd = session.sparkContext.parallelize(csv_rows)
        # NOTE: type annotations for spark.read.csv miss the fact that the first param can be [RDD[str]]
        efo_curation_mapping_df = session.read.csv(rdd, header=True, sep="\t")
        return efo_curation_mapping_df

    @staticmethod
    def join_efo_mapping(
        study_index: StudyIndex,
        efo_curation_mapping: DataFrame,
        finngen_release: str,
    ) -> StudyIndex:
        """Add EFO mapping to the Finngen study index table.

        This function performs inner join on table of EFO mappings to the study index table by trait name.
        All studies without EFO traits are dropped. The EFO mappings are then aggregated into lists per
        studyId.

        NOTE: preserve all studyId entries even if they don't have EFO mappings.
        This is to avoid discrepancies between `study_index` and `credible_set` `studyId` column.
        The rows with missing EFO mappings will be dropped in the study_index validation step.

        Args:
            study_index (StudyIndex): Study index table.
            efo_curation_mapping (DataFrame): Dataframe with EFO mappings.
            finngen_release (str): FinnGen release.

        Returns:
            StudyIndex: Study index table with added EFO mappings.
        """
        efo_mappings = (
            efo_curation_mapping.withColumn("STUDY", f.upper(f.col("STUDY")))
            .filter(f.col("STUDY").contains("FINNGEN"))
            .filter(f.upper(f.col("STUDY")).contains(finngen_release))
            .select(
                f.regexp_replace(f.col("SEMANTIC_TAG"), r"^.*/", "").alias(
                    "traitFromSourceMappedId"
                ),
                f.col("PROPERTY_VALUE").alias("traitFromSource"),
            )
        )

        si_df = study_index.df.join(
            efo_mappings, on="traitFromSource", how="left_outer"
        )
        common_cols = [c for c in si_df.columns if c != "traitFromSourceMappedId"]
        si_df = si_df.groupby(common_cols).agg(
            f.collect_list("traitFromSourceMappedId").alias("traitFromSourceMappedIds")
        )
        return StudyIndex(_df=si_df, _schema=StudyIndex.get_schema())

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
