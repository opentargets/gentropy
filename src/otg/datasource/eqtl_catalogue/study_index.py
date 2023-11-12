"""Study Index for eQTL Catalogue data source."""
from __future__ import annotations

from typing import TYPE_CHECKING, List

import pyspark.sql.functions as f

from otg.dataset.study_index import StudyIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.column import Column


class EqtlCatalogueStudyIndex(StudyIndex):
    """Study index dataset from eQTL Catalogue."""

    @staticmethod
    def _all_attributes() -> List[Column]:
        """A helper function to return all study index attribute expressions.

        Returns:
            List[Column]: all study index attribute expressions.
        """
        study_attributes = [
            # Project ID, example: "GTEx_V8".
            f.col("study").alias("projectId"),
            # Partial study ID, example: "GTEx_V8_Adipose_Subcutaneous". This ID will be converted to final only when
            # summary statistics are parsed, because it must also include a gene ID.
            f.concat(f.col("study"), f.lit("_"), f.col("qtl_group")).alias("studyId"),
            # Summary stats location.
            f.col("ftp_path").alias("summarystatsLocation"),
            # Constant value fields.
            f.lit(True).alias("hasSumstats"),
            f.lit("eqtl").alias("studyType"),
        ]
        tissue_attributes = [
            # Human readable tissue label, example: "Adipose - Subcutaneous".
            f.col("tissue_label").alias("traitFromSource"),
            # Ontology identifier for the tissue, for example: "UBERON:0001157".
            f.array(
                f.regexp_replace(
                    f.regexp_replace(
                        f.col("tissue_ontology_id"),
                        "UBER_",
                        "UBERON_",
                    ),
                    "_",
                    ":",
                )
            ).alias("traitFromSourceMappedIds"),
        ]
        sample_attributes = [
            f.lit(838).alias("nSamples"),
            f.lit("838 (281 females and 557 males)").alias("initialSampleSize"),
            f.array(
                f.struct(
                    f.lit(715).cast("long").alias("sampleSize"),
                    f.lit("European American").alias("ancestry"),
                ),
                f.struct(
                    f.lit(103).cast("long").alias("sampleSize"),
                    f.lit("African American").alias("ancestry"),
                ),
                f.struct(
                    f.lit(12).cast("long").alias("sampleSize"),
                    f.lit("Asian American").alias("ancestry"),
                ),
                f.struct(
                    f.lit(16).cast("long").alias("sampleSize"),
                    f.lit("Hispanic or Latino").alias("ancestry"),
                ),
            ).alias("discoverySamples"),
        ]
        publication_attributes = [
            f.lit("32913098").alias("pubmedId"),
            f.lit(
                "The GTEx Consortium atlas of genetic regulatory effects across human tissues"
            ).alias("publicationTitle"),
            f.lit("GTEx Consortium").alias("publicationFirstAuthor"),
            f.lit("publicationDate").alias("2020-09-11"),
            f.lit("Science").alias("publicationJournal"),
        ]
        return (
            study_attributes
            + tissue_attributes
            + sample_attributes
            + publication_attributes
        )

    @classmethod
    def from_source(
        cls: type[EqtlCatalogueStudyIndex],
        eqtl_studies: DataFrame,
    ) -> EqtlCatalogueStudyIndex:
        """Ingest study level metadata from eQTL Catalogue.

        Args:
            eqtl_studies (DataFrame): ingested but unprocessed eQTL Catalogue studies.

        Returns:
            EqtlCatalogueStudyIndex: preliminary processed study index for eQTL Catalogue studies.
        """
        return EqtlCatalogueStudyIndex(
            _df=eqtl_studies.select(*cls._all_attributes()).withColumn(
                "ldPopulationStructure",
                cls.aggregate_and_map_ancestries(f.col("discoverySamples")),
            ),
            _schema=cls.get_schema(),
        )

    @classmethod
    def add_gene_id_column(
        cls: type[EqtlCatalogueStudyIndex],
        study_index_df: DataFrame,
        summary_stats_df: DataFrame,
    ) -> EqtlCatalogueStudyIndex:
        """Add a geneId column to the study index and explode.

        While the original list contains one entry per tissue, what we consider as a single study is one mini-GWAS for
        an expression of a _particular gene_ in a particular study.  At this stage we have a study index with partial
        study IDs like "PROJECT_QTLGROUP", and a summary statistics object with full study IDs like
        "PROJECT_QTLGROUP_GENEID", so we need to perform a merge and explosion to obtain our final study index.

        Args:
            study_index_df (DataFrame): preliminary study index for eQTL Catalogue studies.
            summary_stats_df (DataFrame): summary statistics dataframe for eQTL Catalogue data.

        Returns:
            EqtlCatalogueStudyIndex: final study index for eQTL Catalogue studies.
        """
        partial_to_full_study_id = (
            summary_stats_df.select(f.col("studyId"))
            .distinct()
            .select(
                f.col("studyId").alias("fullStudyId"),  # PROJECT_QTLGROUP_GENEID
                f.regexp_extract(f.col("studyId"), r"(.*)_[\_]+", 1).alias(
                    "studyId"
                ),  # PROJECT_QTLGROUP
            )
            .groupBy("studyId")
            .agg(f.collect_list("fullStudyId").alias("fullStudyIdList"))
        )
        study_index_df = (
            study_index_df.join(partial_to_full_study_id, "studyId", "inner")
            .withColumn("fullStudyId", f.explode("fullStudyIdList"))
            .drop("fullStudyIdList")
            .withColumn("geneId", f.regexp_extract(f.col("studyId"), r".*_([\_]+)", 1))
            .drop("fullStudyId")
        )
        return EqtlCatalogueStudyIndex(_df=study_index_df, _schema=cls.get_schema())
