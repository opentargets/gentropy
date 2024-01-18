"""Step to update GWAS Catalog study curation file based on newly released GWAS Catalog dataset."""
from __future__ import annotations

from gentropy.common.session import Session
from gentropy.datasource.gwas_catalog.study_index import (
    StudyIndexGWASCatalogParser,
    read_curation_table,
)


class GWASCatalogStudyCurationStep:
    """Annotate GWAS Catalog studies with additional curation and create a curation backlog."""

    def __init__(
        self,
        session: Session,
        catalog_study_files: list[str],
        catalog_ancestry_files: list[str],
        catalog_sumstats_lut: str,
        gwas_catalog_study_curation_out: str,
        gwas_catalog_study_curation_file: str | None,
    ) -> None:
        """Run step to annotate and create backlog.

        Args:
            session (Session): Session object.
            catalog_study_files (list[str]): List of raw GWAS catalog studies file.
            catalog_ancestry_files (list[str]): List of raw ancestry annotations files from GWAS Catalog.
            catalog_sumstats_lut (str): GWAS Catalog summary statistics lookup table.
            gwas_catalog_study_curation_out (str): Path for the updated curation table.
            gwas_catalog_study_curation_file (str | None): Path to the original curation table. Optinal
        """
        catalog_studies = session.spark.read.csv(
            list(catalog_study_files), sep="\t", header=True
        )
        ancestry_lut = session.spark.read.csv(
            list(catalog_ancestry_files), sep="\t", header=True
        )
        sumstats_lut = session.spark.read.csv(
            catalog_sumstats_lut, sep="\t", header=False
        )
        gwas_catalog_study_curation = read_curation_table(
            gwas_catalog_study_curation_file, session
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
            .to_csv(gwas_catalog_study_curation_out, sep="\t", index=False)
        )
