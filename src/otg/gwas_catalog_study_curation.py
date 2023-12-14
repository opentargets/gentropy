"""Step to process GWAS Catalog associations and study table."""
from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.gwas_catalog.study_index import (
    StudyIndexGWASCatalogParser,
    read_curation_table,
)


@dataclass
class GWASCatalogIngestionStep:
    """GWAS Catalog ingestion step to extract GWASCatalog Study and StudyLocus tables.

    !!!note This step currently only processes the GWAS Catalog curated list of top hits.

    Attributes:
        session (Session): Session object.
        catalog_study_files (list[str]): List of raw GWAS catalog studies file.
        catalog_ancestry_files (list[str]): List of raw ancestry annotations files from GWAS Catalog.
        catalog_sumstats_lut (str): GWAS Catalog summary statistics lookup table.
        catalog_associations_file (str): Raw GWAS catalog associations file.
        variant_annotation_path (str): Input variant annotation path.
        ld_populations (list): List of populations to include.
        catalog_studies_out (str): Output GWAS catalog studies path.
        catalog_associations_out (str): Output GWAS catalog associations path.
    """

    session: Session = MISSING
    catalog_study_files: list[str] = MISSING
    catalog_ancestry_files: list[str] = MISSING
    catalog_sumstats_lut: str = MISSING
    catalog_associations_file: str = MISSING
    gwas_catalog_study_curation_file: str = MISSING
    variant_annotation_path: str = MISSING
    catalog_studies_out: str = MISSING
    gwas_catalog_study_curation_out: str = MISSING

    def __post_init__(self: GWASCatalogIngestionStep) -> None:
        """Run step."""
        catalog_studies = self.session.spark.read.csv(
            self.catalog_study_files, sep="\t", header=True
        )
        ancestry_lut = self.session.spark.read.csv(
            self.catalog_ancestry_files, sep="\t", header=True
        )
        sumstats_lut = self.session.spark.read.csv(
            self.catalog_sumstats_lut, sep="\t", header=False
        )
        gwas_catalog_study_curation = read_curation_table(
            self.gwas_catalog_study_curation_file, self.session
        )

        # Process GWAS Catalog studies and get list of studies for curation:
        (
            StudyIndexGWASCatalogParser.from_source(
                catalog_studies, ancestry_lut, sumstats_lut
            )
            # Adding existing curation:
            .annotate_from_study_curation(gwas_catalog_study_curation)
            # Extract new studies for curation:
            .extract_studies_for_curation(gwas_catalog_study_curation)
            # Save table:
            .toPandas()
            .to_csv(self.gwas_catalog_study_curation_out, sep="\t", index=False)
        )
