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
class GWASCatalogStudyCurationStep:
    """Create an updated curation table for GWAS Catalog study table.

    Attributes:
        session (Session): Session object.
        catalog_study_files (list[str]): List of raw GWAS catalog studies file.
        catalog_ancestry_files (list[str]): List of raw ancestry annotations files from GWAS Catalog.
        catalog_sumstats_lut (str): GWAS Catalog summary statistics lookup table.
        catalog_associations_file (str): Raw GWAS catalog associations file.
        gwas_catalog_study_curation_file (str | None): Path to the original curation table. Optinal
        gwas_catalog_study_curation_out (str): Path for the updated curation table.
    """

    session: Session = MISSING
    catalog_study_files: list[str] = MISSING
    catalog_ancestry_files: list[str] = MISSING
    catalog_sumstats_lut: str = MISSING
    catalog_associations_file: str = MISSING
    gwas_catalog_study_curation_file: str | None = MISSING
    gwas_catalog_study_curation_out: str = MISSING

    def __post_init__(self: GWASCatalogStudyCurationStep) -> None:
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
