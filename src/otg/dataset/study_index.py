"""Variant index dataset."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json
from dataclasses import dataclass
from itertools import chain
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Column, Window

from otg import data
from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import column2camel_case
from otg.common.utils import parse_efos
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.common.session import Session


@dataclass
class StudyIndex(Dataset):
    """Study index dataset.

    A study index dataset captures all the metadata for all studies including GWAS and Molecular QTL.
    """

    _schema: StructType = parse_spark_schema("studies.json")

    @classmethod
    def from_parquet(cls: type[StudyIndex], session: Session, path: str) -> StudyIndex:
        """Initialise StudyIndex from parquet file.

        Args:
            session (Session): ETL session
            path (str): Path to parquet file

        Returns:
            StudyIndex: Study index dataset
        """
        return super().from_parquet(session, path, cls._schema)

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

    @staticmethod
    def _gwas_ancestry_to_gnomad(gwas_catalog_ancestry: Column) -> Column:
        """Normalised ancestry column from GWAS Catalog into Gnomad ancestry.

        Args:
            gwas_catalog_ancestry (Column): GWAS Catalog ancestry

        Returns:
            Column: mapped Gnomad ancestry using LUT
        """
        # GWAS Catalog to p-value mapping
        json_dict = json.loads(
            pkg_resources.read_text(
                data, "gwascat_2_gnomad_superpopulation_map.json", encoding="utf-8"
            )
        )
        map_expr = f.create_map(*[f.lit(x) for x in chain(*json_dict.items())])

        return f.transform(gwas_catalog_ancestry, lambda x: map_expr[x])

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
                f.coalesce(
                    f.col("STUDY ACCESSION"), f.monotonically_increasing_id()
                ).alias("studyId"),
                f.coalesce(
                    f.col("STUDY ACCESSION"), f.monotonically_increasing_id()
                ).alias("projectId"),
                f.lit("NOT IMPLEMENTED").alias("studyType"),
                f.col("PUBMED ID").alias("pubmedId"),
                f.col("FIRST AUTHOR").alias("publicationFirstAuthor"),
                f.col("DATE").alias("publicationDate"),
                f.col("JOURNAL").alias("publicationJournal"),
                f.col("STUDY").alias("publicationTitle"),
                f.coalesce(f.col("DISEASE/TRAIT"), f.lit("Unreported")).alias(
                    "traitFromSource"
                ),
                f.col("INITIAL SAMPLE SIZE").alias("initialSampleSize"),
                parse_efos(f.col("MAPPED_TRAIT_URI")).alias("traitFromSourceMappedIds"),
                parse_efos(f.col("MAPPED BACKGROUND TRAIT URI")).alias(
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

    def get_gnomad_ancestry_sample_sizes(self: StudyIndexGWASCatalog) -> DataFrame:
        """Get all studies and their ancestries.

        Returns:
            DataFrame: containing `studyId`, `gnomadPopulation` and `relativeSampleSize` columns
        """
        # Study ancestries
        w_study = Window.partitionBy("studyId")
        return (
            self.df
            # Excluding studies where no sample discription is provided:
            .filter(f.col("discoverySamples").isNotNull())
            # Exploding sample description and study identifier:
            .withColumn("discoverySample", f.explode(f.col("discoverySamples")))
            # Splitting sample descriptions further:
            .withColumn(
                "ancestries",
                f.split(f.col("discoverySample.ancestry"), r",\s(?![^()]*\))"),
            )
            # Dividing sample sizes assuming even distribution
            .withColumn(
                "adjustedSampleSize",
                f.col("discoverySample.sampleSize") / f.size(f.col("ancestries")),
            )
            # mapped to gnomAD superpopulation and exploded
            .withColumn(
                "gnomadPopulation",
                f.explode(
                    StudyIndexGWASCatalog._gwas_ancestry_to_gnomad(f.col("ancestries"))
                ),
            )
            # Group by studies and aggregate for major population:
            .groupBy("studyId", "gnomadPopulation")
            .agg(f.sum(f.col("adjustedSampleSize")).alias("sampleSize"))
            # Calculate proportions for each study
            .withColumn(
                "relativeSampleSize",
                f.col("sampleSize") / f.sum("sampleSize").over(w_study),
            )
            .drop("sampleSize")
        )

    def update_study_id(
        self: StudyIndexGWASCatalog, study_annotation: DataFrame
    ) -> StudyIndexGWASCatalog:
        """Update studyId with a dataframe containing study.

        Args:
            study_annotation (DataFrame): Dataframe containing `updatedStudyId`, `traitFromSource`, `traitFromSourceMappedIds` and key column `studyId`.

        Returns:
            StudyIndexGWASCatalog: Updated study table.
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
        return self

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
            ).withColumnRenamed("studyAccession", "projectId")
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
            .drop(
                "european",
                "other",
                "initialSampleCount",
                "initialSampleCountEuropean",
                "initialSampleCountOther",
            )
        )

        parsed_ancestry_lut = ancestry_stages.join(
            europeans_deconvoluted, on="projectId", how="outer"
        )

        self.df = self.df.join(parsed_ancestry_lut, on="projectId", how="left")
        return self

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

        self.df = (
            self.df.drop("hasSumstats")
            .join(parsed_sumstats_lut, on="projectId", how="left")
            .withColumn(
                "hasSumstats",
                f.when(f.col("hasSumstats"), True)
                .when(f.col("hasSumstats").isNull(), False)
                .otherwise(None),
            )
        )
        return self

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
        self.df = self.df.join(sample_size_lut, on="projectId", how="left")
        return self
