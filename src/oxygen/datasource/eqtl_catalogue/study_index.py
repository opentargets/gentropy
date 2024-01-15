"""Study Index for eQTL Catalogue data source."""
from __future__ import annotations

from typing import TYPE_CHECKING, Type

import pyspark.sql.functions as f

from oxygen.dataset.study_index import StudyIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.column import Column


class EqtlCatalogueStudyIndex:
    """Study index dataset from eQTL Catalogue."""

    study_config = {
        "GTEx_V8": {
            "nSamples": 838,
            "initialSampleSize": "838 (281 females and 557 males)",
            "discoverySamples": [
                {"sampleSize": 715, "ancestry": "European American"},
                {"sampleSize": 103, "ancestry": "African American"},
                {"sampleSize": 12, "ancestry": "Asian American"},
                {"sampleSize": 16, "ancestry": "Hispanic or Latino"},
            ],
            "ldPopulationStructure": [
                {"ldPopulation": "nfe", "relativeSampleSize": 0.85},
                {"ldPopulation": "afr", "relativeSampleSize": 0.12},
                {"ldPopulation": "eas", "relativeSampleSize": 0.01},
                {"ldPopulation": "amr", "relativeSampleSize": 0.02},
            ],
            "pubmedId": "32913098",
            "publicationTitle": "The GTEx Consortium atlas of genetic regulatory effects across human tissues",
            "publicationFirstAuthor": "GTEx Consortium",
            "publicationDate": "2020-09-11",
            "publicationJournal": "Science",
        },
    }

    @classmethod
    def get_study_attribute(
        cls: Type[EqtlCatalogueStudyIndex], attribute_key: str
    ) -> Column:
        """Returns a Column expression that dynamically assigns the attribute based on the study.

        Args:
            attribute_key (str): The attribute key to assign.

        Returns:
            Column: The dynamically assigned attribute.

        Raises:
            ValueError: If the attribute key is not known for the study.
        """
        study_column = f.col("study")
        for study, config in cls.study_config.items():
            attribute_value = config.get(attribute_key)
            if attribute_value is None:
                raise ValueError(
                    f"Unknown attribute key {attribute_key} for study {study}"
                )
            # Convert list of dicts to array of structs
            if isinstance(attribute_value, list) and isinstance(
                attribute_value[0], dict
            ):
                struct_fields = [
                    f.struct(*[f.lit(value).alias(key) for key, value in item.items()])
                    for item in attribute_value
                ]
                attribute_value = f.array(struct_fields)
            # Convert dict to struct
            elif isinstance(attribute_value, dict):
                attribute_value = f.struct(
                    *[f.lit(value).alias(key) for key, value in attribute_value.items()]
                )
        return f.when(study_column == study, attribute_value).alias(attribute_key)

    @classmethod
    def _all_attributes(cls: Type[EqtlCatalogueStudyIndex]) -> list[Column]:
        """A helper function to return all study index attribute expressions.

        Returns:
            list[Column]: A list of all study index attribute expressions.
        """
        study_column = f.col("study")

        static_study_attributes = [
            study_column.alias("projectId"),
            f.concat(study_column, f.lit("_"), f.col("qtl_group")).alias("studyId"),
            f.col("ftp_path").alias("summarystatsLocation"),
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
        dynamic_study_attributes = [
            cls.get_study_attribute("nSamples"),
            cls.get_study_attribute("initialSampleSize"),
            cls.get_study_attribute("discoverySamples"),
            cls.get_study_attribute("ldPopulationStructure"),
            cls.get_study_attribute("pubmedId"),
            cls.get_study_attribute("publicationTitle"),
            cls.get_study_attribute("publicationFirstAuthor"),
            cls.get_study_attribute("publicationDate"),
            cls.get_study_attribute("publicationJournal"),
        ]
        return static_study_attributes + tissue_attributes + dynamic_study_attributes

    @classmethod
    def add_gene_to_study_id(
        cls: type[EqtlCatalogueStudyIndex],
        study_index_df: DataFrame,
        summary_stats_df: DataFrame,
    ) -> StudyIndex:
        """Update the studyId to include gene information from summary statistics. A geneId column is also added.

        While the original list contains one entry per tissue, what we consider as a single study is one mini-GWAS for
        an expression of a _particular gene_ in a particular study.  At this stage we have a study index with partial
        study IDs like "PROJECT_QTLGROUP", and a summary statistics object with full study IDs like
        "PROJECT_QTLGROUP_GENEID", so we need to perform a merge and explosion to obtain our final study index.

        Args:
            study_index_df (DataFrame): preliminary study index for eQTL Catalogue studies.
            summary_stats_df (DataFrame): summary statistics dataframe for eQTL Catalogue data.

        Returns:
            StudyIndex: final study index for eQTL Catalogue studies.
        """
        partial_to_full_study_id = summary_stats_df.select(
            f.col("studyId").alias("fullStudyId"),  # PROJECT_QTLGROUP_GENEID
            f.regexp_extract(f.col("studyId"), r"^(.*)_ENSG\d+", 1).alias(
                "studyId"
            ),  # PROJECT_QTLGROUP
        ).distinct()
        study_index_df = (
            partial_to_full_study_id.join(
                f.broadcast(study_index_df), "studyId", "inner"
            )
            # Change studyId to fullStudyId
            .drop("studyId")
            .withColumnRenamed("fullStudyId", "studyId")
            # Add geneId column
            .withColumn("geneId", f.regexp_extract(f.col("studyId"), r"([^_]+)$", 1))
        )
        return StudyIndex(_df=study_index_df, _schema=StudyIndex.get_schema())

    @classmethod
    def from_source(
        cls: type[EqtlCatalogueStudyIndex],
        eqtl_studies: DataFrame,
    ) -> StudyIndex:
        """Ingest study level metadata from eQTL Catalogue.

        Args:
            eqtl_studies (DataFrame): ingested but unprocessed eQTL Catalogue studies.

        Returns:
            StudyIndex: preliminary processed study index for eQTL Catalogue studies.
        """
        return StudyIndex(
            _df=eqtl_studies.select(*cls._all_attributes()),
            _schema=StudyIndex.get_schema(),
        )
