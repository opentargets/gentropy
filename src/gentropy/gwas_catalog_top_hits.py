"""Step to process GWAS Catalog associations and study table."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.config import WindowBasedClumpingStepConfig
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.gwas_catalog.associations import (
    GWASCatalogCuratedAssociationsParser,
)
from gentropy.datasource.gwas_catalog.study_index import (
    StudyIndexGWASCatalogParser,
)
from gentropy.datasource.gwas_catalog.study_splitter import GWASCatalogStudySplitter


class GWASCatalogTopHitIngestionStep:
    """GWAS Catalog ingestion step to extract GWASCatalog top hits."""

    def __init__(
        self,
        session: Session,
        catalog_study_files: list[str],
        catalog_ancestry_files: list[str],
        catalog_associations_file: str,
        variant_annotation_path: str,
        catalog_studies_out: str,
        catalog_associations_out: str,
        distance: int = WindowBasedClumpingStepConfig().distance,
    ) -> None:
        """Run step.

        Args:
            session (Session): Session object.
            catalog_study_files (list[str]): List of raw GWAS catalog studies file.
            catalog_ancestry_files (list[str]): List of raw ancestry annotations files from GWAS Catalog.
            catalog_associations_file (str): Raw GWAS catalog associations file.
            variant_annotation_path (str): Path to GnomAD variants.
            catalog_studies_out (str): Output GWAS catalog studies path.
            catalog_associations_out (str): Output GWAS catalog associations path.
            distance (int): Distance, within which tagging variants are collected around the semi-index.
        """
        # Extract
        gnomad_variants = VariantIndex.from_parquet(session, variant_annotation_path)
        catalog_studies = session.spark.read.csv(
            list(catalog_study_files), sep="\t", header=True
        )
        ancestry_lut = session.spark.read.csv(
            list(catalog_ancestry_files), sep="\t", header=True
        )
        catalog_associations = session.spark.read.csv(
            catalog_associations_file, sep="\t", header=True
        ).persist()

        # Transform
        study_index, study_locus = GWASCatalogStudySplitter.split(
            StudyIndexGWASCatalogParser.from_source(catalog_studies, ancestry_lut),
            GWASCatalogCuratedAssociationsParser.from_source(
                catalog_associations, gnomad_variants
            ),
        )
        # Load
        (
            study_index
            # Flag all studies without sumstats
            .add_no_sumstats_flag()
            # Save dataset:
            .df.write.mode(session.write_mode)
            .parquet(catalog_studies_out)
        )

        (
            study_locus.window_based_clumping(distance)
            .df.write.mode(session.write_mode)
            .parquet(catalog_associations_out)
        )
