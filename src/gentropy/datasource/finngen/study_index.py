"""Study Index for Finngen data source."""

from __future__ import annotations

import re
from urllib.request import urlopen

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession

from gentropy.config import FinngenStudiesConfig
from gentropy.dataset.study_index import StudyIndex


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

    @staticmethod
    def join_efo_mapping(
        study_index: StudyIndex,
        efo_curation_mapping: DataFrame,
        finngen_release_prefix: str = FinngenStudiesConfig().finngen_release_prefix,
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
            finngen_release_prefix (str): FinnGen release prefix.

        Returns:
            StudyIndex: Study index table with added EFO mappings.

        Raises:
            ValueError: when incorrect release prefix is provided.
        """
        finngen_release_prefix_regex = re.compile(r"FINNGEN_(?P<release>R\d+){1}_?")
        finngen_release_prefix_match = finngen_release_prefix_regex.match(
            finngen_release_prefix
        )
        if not finngen_release_prefix_match:
            raise ValueError(
                f"Invalid FinnGen release prefix: {finngen_release_prefix}, use the format FINNGEN_R*"
            )
        finngen_release = finngen_release_prefix_match.group("release").upper()

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
        finngen_phenotype_table_url: str = FinngenStudiesConfig().finngen_phenotype_table_url,
        finngen_release_prefix: str = FinngenStudiesConfig().finngen_release_prefix,
        finngen_summary_stats_url_prefix: str = FinngenStudiesConfig().finngen_summary_stats_url_prefix,
        finngen_summary_stats_url_suffix: str = FinngenStudiesConfig().finngen_summary_stats_url_suffix,
    ) -> StudyIndex:
        """This function ingests study level metadata from FinnGen.

        Args:
            spark (SparkSession): Spark session object.
            finngen_phenotype_table_url (str): URL to the FinnGen phenotype table.
            finngen_release_prefix (str): FinnGen release prefix.
            finngen_summary_stats_url_prefix (str): FinnGen summary stats URL prefix.
            finngen_summary_stats_url_suffix (str): FinnGen summary stats URL suffix.

        Returns:
            StudyIndex: Parsed and annotated FinnGen study table.
        """
        json_data = urlopen(finngen_phenotype_table_url).read().decode("utf-8")
        rdd = spark.sparkContext.parallelize([json_data])
        raw_df = spark.read.json(rdd)
        return StudyIndex(
            _df=raw_df.select(
                f.concat(f.col("phenocode")).alias("studyId"),
                f.col("phenostring").alias("traitFromSource"),
                f.col("num_cases").cast("integer").alias("nCases"),
                f.col("num_controls").cast("integer").alias("nControls"),
                (f.col("num_cases") + f.col("num_controls"))
                .cast("integer")
                .alias("nSamples"),
                f.lit(finngen_release_prefix).alias("projectId"),
                f.lit("gwas").alias("studyType"),
                f.lit(True).alias("hasSumstats"),
                f.lit("377,277 (210,870 females and 166,407 males)").alias(
                    "initialSampleSize"
                ),
                f.array(
                    f.struct(
                        f.lit(377277).cast("integer").alias("sampleSize"),
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
            ).withColumn(
                "ldPopulationStructure",
                StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")),
            ),
            _schema=StudyIndex.get_schema(),
        )
