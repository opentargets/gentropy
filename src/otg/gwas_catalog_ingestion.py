"""Step to process GWAS Catalog associations and study table."""
from __future__ import annotations

from otg.common.session import Session
from otg.dataset.variant_annotation import VariantAnnotation
from otg.datasource.gwas_catalog.associations import (
    GWASCatalogCuratedAssociationsParser,
)
from otg.datasource.gwas_catalog.study_index import StudyIndexGWASCatalogParser
from otg.datasource.gwas_catalog.study_splitter import GWASCatalogStudySplitter


class GWASCatalogIngestionStep:
    """GWAS Catalog ingestion step to extract GWASCatalog Study and StudyLocus tables.

    !!!note This step currently only processes the GWAS Catalog curated list of top hits.
    """

    def __init__(
        self,
        session: Session,
        catalog_study_files: list[str],
        catalog_ancestry_files: list[str],
        catalog_sumstats_lut: str,
        catalog_associations_file: str,
        variant_annotation_path: str,
        catalog_studies_out: str,
        catalog_associations_out: str,
    ) -> None:
        """Run GWAS Catalog ingestion step.

        Args:
            session (Session): Session object.
            catalog_study_files (list[str]): List of raw GWAS catalog studies file.
            catalog_ancestry_files (list[str]): List of raw ancestry annotations files from GWAS Catalog.
            catalog_sumstats_lut (str): GWAS Catalog summary statistics lookup table.
            catalog_associations_file (str): Raw GWAS catalog associations file.
            variant_annotation_path (str): Input variant annotation path.
            catalog_studies_out (str): Output GWAS catalog studies path.
            catalog_associations_out (str): Output GWAS catalog associations path.
        """
        # Extract
        va = VariantAnnotation.from_parquet(session, variant_annotation_path)
        catalog_studies = session.spark.read.csv(
            catalog_study_files, sep="\t", header=True
        )
        ancestry_lut = session.spark.read.csv(
            catalog_ancestry_files, sep="\t", header=True
        )
        sumstats_lut = session.spark.read.csv(
            catalog_sumstats_lut, sep="\t", header=False
        )
        catalog_associations = session.spark.read.csv(
            catalog_associations_file, sep="\t", header=True
        ).persist()

        # Transform
        study_index, study_locus = GWASCatalogStudySplitter.split(
            StudyIndexGWASCatalogParser.from_source(
                catalog_studies, ancestry_lut, sumstats_lut
            ),
            GWASCatalogCuratedAssociationsParser.from_source(catalog_associations, va),
        )

        # Load
        study_index.df.write.mode(session.write_mode).parquet(catalog_studies_out)
        study_locus.df.write.mode(session.write_mode).parquet(catalog_associations_out)
