"""Study Index for Finngen data source."""

from __future__ import annotations

from urllib.request import urlopen

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from gentropy.config import FinngenStudiesConfig
from gentropy.dataset.study_index import StudyIndex
from gentropy.datasource.finngen.study_index import FinnGenStudyIndex


class FinngenUkbMetaStudyIndex(StudyIndex):
    """Study index dataset from FinnGen UKB meta-analysis."""

    @classmethod
    def from_source(
        cls: type[FinngenUkbMetaStudyIndex],
        spark: SparkSession,
        raw_study_index_path_from_tsv: str,
        efo_curation_mapping_url: str = FinngenStudiesConfig().efo_curation_mapping_url,
    ) -> StudyIndex:
        """This function ingests study level metadata from FinnGen UKB meta-analysis.

        Args:
            spark (SparkSession): Spark session object.
            raw_study_index_path_from_tsv (str): Raw study index path.
            efo_curation_mapping_url (str): URL to the EFO curation mapping file.

        Returns:
            StudyIndex: Parsed and annotated FinnGen UKB meta-analysis study table.
        """
        # Read the raw study index and process.
        study_index_df = spark.read.csv(
            raw_study_index_path_from_tsv, sep="\t", header=True
        ).select(
            f.lit("gwas").alias("studyType"),
            f.lit("FINNGEN_R11_UKB_META").alias("projectId"),
            f.col("_gentropy_study_id").alias("studyId"),
            f.col("name").alias("traitFromSource"),
            f.lit(True).alias("hasSumstats"),
            f.col("_gentropy_summary_stats_link").alias("summarystatsLocation"),
            (
                f.col("fg_n_cases")
                + f.col("ukbb_n_cases")
                + f.col("fg_n_controls")
                + f.col("ukbb_n_controls")
            )
            .cast("integer")
            .alias("nSamples"),
            f.array(
                f.struct(
                    (f.col("fg_n_cases") + f.col("fg_n_controls"))
                    .cast("integer")
                    .alias("sampleSize"),
                    f.lit("Finnish").alias("ancestry"),
                ),
                f.struct(
                    (f.col("ukbb_n_cases") + f.col("ukbb_n_controls"))
                    .cast("integer")
                    .alias("sampleSize"),
                    f.lit("European").alias("ancestry"),
                ),
            ).alias("discoverySamples"),
        )
        # Add population structure.
        study_index_df = study_index_df.withColumn(
            "ldPopulationStructure",
            cls.aggregate_and_map_ancestries(f.col("discoverySamples")),
        )
        # Create study index.
        study_index = StudyIndex(
            _df=study_index_df,
            _schema=StudyIndex.get_schema(),
        )
        # Add EFO mappings.
        csv_data = urlopen(efo_curation_mapping_url).readlines()
        csv_rows = [row.decode("utf8") for row in csv_data]
        rdd = spark.sparkContext.parallelize(csv_rows)
        efo_curation_mapping = spark.read.csv(rdd, header=True, sep="\t")
        study_index = FinnGenStudyIndex.join_efo_mapping(
            study_index,
            efo_curation_mapping,
            finngen_release="R11",
        )
        return study_index
