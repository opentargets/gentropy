"""Variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.common.schemas import parse_spark_schema
from otg.common.utils import column2camel_case
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.common.session import ETLSession


@dataclass
class StudyIndex(Dataset):
    """Study index dataset.

    A study index dataset captures all the metadata for all studies including GWAS and Molecular QTL.
    """

    schema: StructType = parse_spark_schema("studies.json")

    @classmethod
    def from_parquet(cls: type[StudyIndex], etl: ETLSession, path: str) -> StudyIndex:
        """Initialise StudyIndex from parquet file.

        Args:
            etl (ETLSession): ETL session
            path (str): Path to parquet file

        Returns:
            StudyIndex: Study index dataset
        """
        return super().from_parquet(etl, path, cls.schema)

    def study_type_lut(self: StudyIndex) -> DataFrame:
        """Return a lookup table of study type.

        Returns:
            DataFrame: A dataframe containing `studyId` and `studyType` columns.
        """
        return self.df.select("studyId", "studyType")


@dataclass
class StudyIndexGWASCatalog(StudyIndex):
    """Study index dataset from GWAS Catalog.

    The following information is harmonised from the GWAS Catalog:

    - All publication related information retained.
    - Mapped measured and background traits parsed.
    - Flagged if harmonized summary statistics datasets available.
    - If available, the ftp path to these files presented.
    - Ancestries from the discovery and replication stages are structured with sample counts.
    - Case/control counts extracted.
    - The number of samples with European ancestry extracted.

    """

    @classmethod
    def _parse_study_table(
        cls: type[StudyIndexGWASCatalog], catalog_studies: DataFrame
    ) -> StudyIndexGWASCatalog:
        """Harmonise GWASCatalog study table with `StudyIndex` schema.

        Args:
            catalog_studies (DataFrame): GWAS Catalog study table

        Returns:
            StudyIndexGWASCatalog:
        """
        return cls(
            _df=catalog_studies.select(
                f.col("STUDY ACCESSION").alias("studyId"),  # Study index id
                f.col("STUDY ACCESSION").alias("projectId"),  # GWAS Catalog study id
                f.col("PUBMED ID").alias("pubmedId"),
                f.col("FIRST AUTHOR").alias("publicationFirstAuthor"),
                f.col("DATE").alias("publicationDate"),
                f.col("JOURNAL").alias("publicationJournal"),
                f.col("STUDY").alias("publicationTitle"),
                f.col("DISEASE/TRAIT").alias("traitFromSource"),
                f.col("INITIAL SAMPLE SIZE").alias("initialSampleSize"),
                cls._parse_efos(f.col("MAPPED_TRAIT_URI")).alias(
                    "traitFromSourceMappedIds"
                ),
                cls._parse_efos(f.col("MAPPED BACKGROUND TRAIT URI")).alias(
                    "backgroundTraitFromSourceMappedIds"
                ),
            )
        )

    @classmethod
    def from_source(
        cls: type[StudyIndexGWASCatalog],
        catalog_studies: DataFrame,
        ancestry_file: DataFrame,
        sumstats_lut: DataFrame,
    ) -> StudyIndexGWASCatalog:
        """This function ingests study level metadata from the GWAS Catalog.

        Args:
            catalog_studies (DataFrame): GWAS Catalog raw study table
            ancestry_file (DataFrame): GWAS Catalog ancestry table.
            sumstats_lut (DataFrame): GWAS Catalog summary statistics list.

        Returns:
            StudyIndexGWASCatalog: Parsed and annotated GWAS Catalog study table.
        """
        # Read GWAS Catalogue raw data
        return (
            cls._parse_study_table(catalog_studies)
            ._annotate_ancestries(ancestry_file)
            ._annotate_sumstats_info(sumstats_lut)
            ._annotate_discovery_sample_sizes()
        )

    def update_study_id(
        self: StudyIndexGWASCatalog, study_annotation: DataFrame
    ) -> None:
        """Update studyId with a dataframe containing study.

        Args:
            study_annotation (DataFrame): Dataframe containing `updatedStudyId`, `traitFromSource`, `traitFromSourceMappedIds` and key column `studyId`.
        """
        self.df = (
            self._df.alias("studyIndex")
            .join(study_annotation.alias("studyAnnotation"), on="studyId", how="left")
            .withColumn(
                "studyIndex.studyId",
                f.coalesce("studyAnnotation.updatedStudyId", "studyIndex.studyId"),
            )
            .withColumn(
                "studyIndex.traitFromSource",
                f.coalesce(
                    "studyAnnotation.traitFromSource", "studyIndex.traitFromSource"
                ),
            )
            .withColumn(
                "studyIndex.traitFromSourceMappedIds",
                f.coalesce(
                    "studyAnnotation.traitFromSourceMappedIds",
                    "studyIndex.traitFromSourceMappedIds",
                ),
            )
            .select("studyIndex.*")
        )
        self.validate_schema()

    def _annotate_ancestries(
        self: StudyIndexGWASCatalog, ancestry_lut: DataFrame
    ) -> StudyIndexGWASCatalog:
        """Extracting sample sizes and ancestry information.

        This function parses the ancestry data. Also get counts for the europeans in the same
        discovery stage.

        Args:
            ancestry_lut (DataFrame): Ancestry table as downloaded from the GWAS Catalog

        Returns:
            StudyIndexGWASCatalog: Slimmed and cleaned version of the ancestry annotation.
        """
        ancestry = (
            ancestry_lut
            # Convert column headers to camelcase:
            .transform(
                lambda df: df.select(
                    *[f.expr(column2camel_case(x)) for x in df.columns]
                )
            )
        )

        # Get a high resolution dataset on experimental stage:
        ancestry_stages = (
            ancestry.groupBy("projectId")
            .pivot("stage")
            .agg(
                f.collect_set(
                    f.struct(
                        f.col("numberOfIndividuals").alias("sampleSize"),
                        f.col("broadAncestralCategory").alias("ancestry"),
                    )
                )
            )
            .withColumnRenamed("initial", "discoverySamples")
            .withColumnRenamed("replication", "replicationSamples")
            .persist()
        )

        # Generate information on the ancestry composition of the discovery stage, and calculate
        # the proportion of the Europeans:
        europeans_deconvoluted = (
            ancestry
            # Focus on discovery stage:
            .filter(f.col("stage") == "initial")
            # Sorting ancestries if European:
            .withColumn(
                "ancestryFlag",
                # Excluding finnish:
                f.when(
                    f.col("initialSampleDescription").contains("Finnish"),
                    f.lit("other"),
                )
                # Excluding Icelandic population:
                .when(
                    f.col("initialSampleDescription").contains("Icelandic"),
                    f.lit("other"),
                )
                # Including European ancestry:
                .when(f.col("broadAncestralCategory") == "European", f.lit("european"))
                # Exclude all other population:
                .otherwise("other"),
            )
            # Grouping by study accession and initial sample description:
            .groupBy("projectId")
            .pivot("ancestryFlag")
            .agg(
                # Summarizing sample sizes for all ancestries:
                f.sum(f.col("numberOfIndividuals"))
            )
            # Do aritmetics to make sure we have the right proportion of european in the set:
            .withColumn(
                "initialSampleCountEuropean",
                f.when(f.col("european").isNull(), f.lit(0)).otherwise(
                    f.col("european")
                ),
            )
            .withColumn(
                "initialSampleCountOther",
                f.when(f.col("other").isNull(), f.lit(0)).otherwise(f.col("other")),
            )
            .withColumn(
                "initialSampleCount",
                f.col("initialSampleCountEuropean") + f.col("other"),
            )
            .drop("european", "other", "initialSampleCountOther")
        )

        parsed_ancestry_lut = ancestry_stages.join(
            europeans_deconvoluted, on="projectId", how="outer"
        )

        return self.df.join(parsed_ancestry_lut, on="projectId", how="left")

    def _annotate_sumstats_info(
        self: StudyIndexGWASCatalog, sumstats_lut: DataFrame
    ) -> StudyIndexGWASCatalog:
        """Annotate summary stat locations.

        Args:
            sumstats_lut (DataFrame): listing GWAS Catalog summary stats paths

        Returns:
            StudyIndexGWASCatalog: including `summarystatsLocation` and `hasSumstats` columns
        """
        gwas_sumstats_base_uri = (
            "ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics"
        )

        parsed_sumstats_lut = sumstats_lut.withColumn(
            "summarystatsLocation",
            f.concat(
                f.lit(gwas_sumstats_base_uri),
                f.regexp_replace(f.col("_c0"), r"^\.\/", ""),
            ),
        ).select(
            f.regexp_extract(f.col("summarystatsLocation"), r"\/(GCST\d+)\/", 1).alias(
                "projectId"
            ),
            "summarystatsLocation",
            f.lit(True).alias("hasSumstats"),
        )

        return self.df.join(parsed_sumstats_lut, on="projectId", how="left").withColumn(
            "hasSumstats", f.coalesce(f.col("hasSumstats"), f.lit(False))
        )

    def _annotate_discovery_sample_sizes(
        self: StudyIndexGWASCatalog,
    ) -> StudyIndexGWASCatalog:
        """Extract the sample size of the discovery stage of the study as annotated in the GWAS Catalog.

        Returns:
            StudyIndexGWASCatalog: df with columns `nCases`, `nControls`, and `nSamples` per `ProjectId`
        """
        sample_size_lut = (
            self.df.select(
                "projectId",
                f.explode_outer(f.split(f.col("initialSampleSize"), r",\s+")).alias(
                    "samples"
                ),
            )
            # Extracting the sample size from the string:
            .withColumn(
                "sampleSize",
                f.regexp_extract(
                    f.regexp_replace(f.col("samples"), ",", ""), r"[0-9,]+", 0
                ).cast(t.IntegerType()),
            )
            .select(
                "projectId",
                "sampleSize",
                f.when(f.col("samples").contains("cases"), f.col("sampleSize"))
                .otherwise(f.lit(0))
                .alias("nCases"),
                f.when(f.col("samples").contains("controls"), f.col("sampleSize"))
                .otherwise(f.lit(0))
                .alias("nControls"),
            )
            # Aggregating sample sizes for all ancestries:
            .groupBy("projectId")
            .agg(
                f.sum("nCases").alias("nCases"),
                f.sum("nControls").alias("nControls"),
                f.sum("sampleSize").alias("nSamples"),
            )
        )

        return self.df.join(sample_size_lut, on="projectId", how="left")
