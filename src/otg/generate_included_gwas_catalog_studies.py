"""Step to process GWAS Catalog associations and study table."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from omegaconf import MISSING
from pyspark.sql import functions as f

from otg.common.session import Session
from otg.dataset.variant_annotation import VariantAnnotation
from otg.datasource.gwas_catalog.associations import (
    GWASCatalogCuratedAssociationsParser,
)
from otg.datasource.gwas_catalog.study_index import (
    StudyIndexGWASCatalog,
    StudyIndexGWASCatalogParser,
    read_curation_table,
)
from otg.datasource.gwas_catalog.study_splitter import GWASCatalogStudySplitter

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


@dataclass
class GWASCatalogInclusionGenerator:
    """GWAS Catalog ingestion step to extract GWASCatalog Study and StudyLocus tables.

    Attributes:
        session (Session): Session object.
        catalog_study_files (list[str]): List of raw GWAS catalog studies file.
        catalog_ancestry_files (list[str]): List of raw ancestry annotations files from GWAS Catalog.
        catalog_sumstats_lut (str): GWAS Catalog summary statistics lookup table.
        catalog_associations_file (str): Raw GWAS catalog associations file.
        variant_annotation_path (str): Input variant annotation path.
        catalog_studies_out (str): Output GWAS catalog studies path.
        catalog_associations_out (str): Output GWAS catalog associations path.
        gwas_catalog_study_curation_file (str | None): file of the curation table. Optional.
    """

    session: Session = MISSING
    # GWAS Catalog sources:
    catalog_study_files: list[str] = MISSING
    catalog_ancestry_files: list[str] = MISSING
    catalog_associations_file: str = MISSING
    # Custom curation:
    gwas_catalog_study_curation_file: str | None = None
    # GnomAD source:
    variant_annotation_path: str = MISSING
    # Dynamic sources:
    harmonised_study_file: str = MISSING
    criteria: str = MISSING
    # Output:
    inclusion_list_path: str = MISSING
    exclusion_list_path: str = MISSING

    @staticmethod
    def flag_eligible_studies(
        study_index: StudyIndexGWASCatalog, criteria: str
    ) -> DataFrame:
        """Apply filter on GWAS Catalog studies based on the provided criteria.

        Args:
            study_index (StudyIndexGWASCatalog): complete study index to be filtered based on the provided filter set
            criteria (str): name of the filter set to be applied.

        Raises:
            ValueError: if the provided filter set is not in the accepted values.

        Returns:
            DataFrame: filtered dataframe containing only eligible studies.
        """
        filters: dict[str, Column] = {
            # Filters applied on studies for ingesting curated associations:
            "curation": (study_index.is_gwas() & study_index.has_mapped_trait()),
            # Filters applied on studies for ingesting summary statistics:
            "summary_stats": (
                study_index.is_gwas()
                & study_index.has_mapped_trait()
                & (~study_index.is_quality_flagged())
                & study_index.has_summarystats()
            ),
        }

        if criteria not in filters:
            raise ValueError(
                f'Wrong value as filter set ({criteria}). Accepted: {",".join(filters.keys())}'
            )

        # Applying the relevant filter to the study:
        return study_index.df.select(
            "studyId",
            "studyType",
            "traitFromSource",
            "traitFromSourceMappedIds",
            "qualityControls",
            "hasSumstats",
            f.when(filters[criteria], f.lit(True))
            .otherwise(f.lit(False))
            .alias("isEligible"),
        )

    @staticmethod
    def process_harmonised_list(studies: list[str], session: Session) -> DataFrame:
        """Generate spark dataframe from the provided list.

        Args:
            studies (list[str]): list of path pointing to harmonised summary statistics.
            session (Session): session

        Returns:
            DataFrame: column name is consistent with original implementatin
        """
        return session.spark.createDataFrame([{"_c0": path} for path in studies])

    def get_gwas_catalog_study_index(
        self: GWASCatalogInclusionGenerator,
    ) -> StudyIndexGWASCatalog:
        """Return GWAS Catalog study index.

        Returns:
            StudyIndexGWASCatalog: Completely processed and fully annotated study index.
        """
        # Extract
        va = VariantAnnotation.from_parquet(self.session, self.variant_annotation_path)
        catalog_studies = self.session.spark.read.csv(
            list(self.catalog_study_files), sep="\t", header=True
        )
        ancestry_lut = self.session.spark.read.csv(
            list(self.catalog_ancestry_files), sep="\t", header=True
        )
        sumstats_lut = self.session.spark.read.csv(
            self.harmonised_study_file, sep="\t", header=False
        )
        catalog_associations = self.session.spark.read.csv(
            self.catalog_associations_file, sep="\t", header=True
        ).persist()
        gwas_catalog_study_curation = read_curation_table(
            self.gwas_catalog_study_curation_file, self.session
        )

        # Transform
        study_index, study_locus = GWASCatalogStudySplitter.split(
            StudyIndexGWASCatalogParser.from_source(
                catalog_studies,
                ancestry_lut,
                sumstats_lut,
            ).annotate_from_study_curation(gwas_catalog_study_curation),
            GWASCatalogCuratedAssociationsParser.from_source(catalog_associations, va),
        )

        return study_index

    def __post_init__(self: GWASCatalogInclusionGenerator) -> None:
        """Run step."""
        # Create study index:
        study_index = self.get_gwas_catalog_study_index()

        # Get study indices for inclusion:
        flagged_studies = self.flag_eligible_studies(study_index, self.criteria)

        # Output inclusion list:
        eligible = (
            flagged_studies.filter(f.col("isEligible")).select("studyId").persist()
        )
        eligible.write.mode(self.session.write_mode).parquet(self.inclusion_list_path)
        # print(f"Eligible studies: {eligible.count()}")

        # Output exclusion list:
        excluded = flagged_studies.filter(~f.col("isEligible")).persist()
        excluded.write.mode(self.session.write_mode).parquet(self.exclusion_list_path)
        # print(f"Excluded studies: {excluded.count()}")
