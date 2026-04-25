"""Step to process GWAS Catalog associations and study table."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

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


class GWASCatalogTopHitDefaults(BaseModel, frozen=True):
    """Defaults for GWASCatalogTopHitIngestionStep.

    All values are frozen - create a new instance to override.
    """

    catalog_study_files: Annotated[
        list[str], Field(description="List of raw GWAS catalog studies file.")
    ]
    catalog_ancestry_files: Annotated[
        list[str],
        Field(description="List of raw ancestry annotations files from GWAS Catalog."),
    ]
    catalog_associations_file: Annotated[
        str, Field(description="Raw GWAS catalog associations file.")
    ]
    variant_annotation_path: Annotated[
        str, Field(description="Path to GnomAD variants.")
    ]
    catalog_studies_out: Annotated[
        str, Field(description="Output GWAS catalog studies path.")
    ]
    catalog_associations_out: Annotated[
        str, Field(description="Output GWAS catalog associations path.")
    ]


class GWASCatalogTopHitIngestionStep:
    """GWAS Catalog ingestion step to extract GWASCatalog top hits."""

    def __init__(
        self,
        session: Session,
        config: GWASCatalogTopHitDefaults,
        distance: int = WindowBasedClumpingStepConfig().distance,
    ) -> None:
        """Run step.

        Args:
            session (Session): Session object.
            config: Configuration for the step.
            distance (int): Distance, within which tagging variants are collected around the semi-index.
        """
        # Extract
        gnomad_variants = VariantIndex.from_parquet(
            session, config.variant_annotation_path
        )
        catalog_studies = session.spark.read.csv(
            list(config.catalog_study_files), sep="\t", header=True
        )
        ancestry_lut = session.spark.read.csv(
            list(config.catalog_ancestry_files), sep="\t", header=True
        )
        catalog_associations = session.spark.read.csv(
            config.catalog_associations_file, sep="\t", header=True
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
            .parquet(config.catalog_studies_out)
        )

        (
            study_locus.window_based_clumping(distance)
            .df.write.mode(session.write_mode)
            .parquet(config.catalog_associations_out)
        )
