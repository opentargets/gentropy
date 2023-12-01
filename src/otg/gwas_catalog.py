"""Step to process GWAS Catalog associations."""
from __future__ import annotations

from dataclasses import dataclass

from omegaconf import MISSING

from otg.common.session import Session
from otg.dataset.ld_index import LDIndex
from otg.dataset.variant_annotation import VariantAnnotation
from otg.datasource.gwas_catalog.associations import GWASCatalogAssociations
from otg.datasource.gwas_catalog.study_index import GWASCatalogStudyIndex
from otg.datasource.gwas_catalog.study_splitter import GWASCatalogStudySplitter
from otg.method.ld import LDAnnotator
from otg.method.pics import PICS


@dataclass
class GWASCatalogStep:
    """GWAS Catalog ingestion step to extract GWASCatalog Study and StudyLocus tables.

    !!!note This step currently only processes the GWAS Catalog curated list of top hits.

    Attributes:
        session (Session): Session object.
        catalog_study_files (list[str]): Raw GWAS catalog studies file.
        catalog_ancestry_file (str): Ancestry annotations file from GWAS Catalog.
        catalog_sumstats_lut (str): GWAS Catalog summary statistics lookup table.
        catalog_associations_file (str): Raw GWAS catalog associations file.
        variant_annotation_path (str): Input variant annotation path.
        ld_populations (list): List of populations to include.
        min_r2 (float): Minimum r2 to consider when considering variants within a window.
        catalog_studies_out (str): Output GWAS catalog studies path.
        catalog_associations_out (str): Output GWAS catalog associations path.
    """

    session: Session = MISSING
    catalog_study_files: list[str] = MISSING
    catalog_ancestry_file: str = MISSING
    catalog_sumstats_lut: str = MISSING
    catalog_associations_file: str = MISSING
    variant_annotation_path: str = MISSING
    ld_index_path: str = MISSING
    min_r2: float = 0.5
    catalog_studies_out: str = MISSING
    catalog_associations_out: str = MISSING

    def __post_init__(self: GWASCatalogStep) -> None:
        """Run step."""
        # Extract
        va = VariantAnnotation.from_parquet(self.session, self.variant_annotation_path)
        catalog_studies = self.session.spark.read.csv(
            self.catalog_study_files, sep="\t", header=True
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
        ld_index = LDIndex.from_parquet(self.session, self.ld_index_path)

        # Transform
        study_index, study_locus = GWASCatalogStudySplitter.split(
            GWASCatalogStudyIndex.from_source(
                catalog_studies, ancestry_lut, sumstats_lut
            ),
            GWASCatalogAssociations.from_source(catalog_associations, va),
        )
        study_locus_ld = LDAnnotator.ld_annotate(
            study_locus, study_index, ld_index
        ).clump()
        finemapped_study_locus = PICS.finemap(study_locus_ld).annotate_credible_sets()

        # Load
        study_index.df.write.mode(self.session.write_mode).parquet(
            self.catalog_studies_out
        )
        finemapped_study_locus.df.write.mode(self.session.write_mode).parquet(
            self.catalog_associations_out
        )
