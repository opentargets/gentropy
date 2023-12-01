"""Step to process GWAS Catalog associations and study table."""
from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.dataset.variant_annotation import VariantAnnotation
from otg.datasource.gwas_catalog.associations import GWASCatalogAssociations
from otg.datasource.gwas_catalog.study_index import GWASCatalogStudyIndex
from otg.datasource.gwas_catalog.study_splitter import GWASCatalogStudySplitter


@dataclass
class GWASCatalogStep:
    """GWAS Catalog ingestion step to extract GWASCatalog Study and StudyLocus tables.

    !!!note This step currently only processes the GWAS Catalog curated list of top hits.

    Attributes:
        session (Session): Session object.
        catalog_studies_file (str): Raw GWAS catalog studies file.
        catalog_ancestry_file (str): Ancestry annotations file from GWAS Catalog.
        catalog_sumstats_lut (str): GWAS Catalog summary statistics lookup table.
        catalog_associations_file (str): Raw GWAS catalog associations file.
        variant_annotation_path (str): Input variant annotation path.
        ld_populations (list): List of populations to include.
        catalog_studies_out (str): Output GWAS catalog studies path.
        catalog_associations_out (str): Output GWAS catalog associations path.
    """

    session: Session = MISSING
    catalog_studies_file: str = MISSING
    catalog_ancestry_file: str = MISSING
    catalog_sumstats_lut: str = MISSING
    catalog_associations_file: str = MISSING
    variant_annotation_path: str = MISSING
    catalog_studies_out: str = MISSING
    catalog_associations_out: str = MISSING

    def __post_init__(self: GWASCatalogStep) -> None:
        """Run step."""
        # Extract
        va = VariantAnnotation.from_parquet(self.session, self.variant_annotation_path)
        catalog_studies = self.session.spark.read.csv(
            self.catalog_studies_file, sep="\t", header=True
        )
        ancestry_lut = self.session.spark.read.csv(
            self.catalog_ancestry_file, sep="\t", header=True
        )
        sumstats_lut = self.session.spark.read.csv(
            self.catalog_sumstats_lut, sep="\t", header=False
        )
        catalog_associations = self.session.spark.read.csv(
            self.catalog_associations_file, sep="\t", header=True
        ).persist()

        # Transform
        study_index, study_locus = GWASCatalogStudySplitter.split(
            GWASCatalogStudyIndex.from_source(
                catalog_studies, ancestry_lut, sumstats_lut
            ),
            GWASCatalogAssociations.from_source(catalog_associations, va),
        )

        # Load
        study_index.df.write.mode(self.session.write_mode).parquet(
            self.catalog_studies_out
        )
        study_locus.df.write.mode(self.session.write_mode).parquet(
            self.catalog_associations_out
        )
