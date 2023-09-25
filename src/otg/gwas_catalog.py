"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass

import hail as hl

from otg.common.session import Session
from otg.config import GWASCatalogStepConfig
from otg.dataset.ld_index import LDIndex
from otg.dataset.variant_annotation import VariantAnnotation
from otg.datasource.gwas_catalog.associations import GWASCatalogAssociations
from otg.datasource.gwas_catalog.study_index import GWASCatalogStudyIndex
from otg.datasource.gwas_catalog.study_splitter import GWASCatalogStudySplitter
from otg.method.pics import PICS


@dataclass
class GWASCatalogStep(GWASCatalogStepConfig):
    """GWAS Catalog step."""

    session: Session = Session()

    def run(self: GWASCatalogStep) -> None:
        """Run GWAS Catalog ingestion step to extract GWASCatalog Study and StudyLocus tables."""
        hl.init(sc=self.session.spark.sparkContext, log="/dev/null")
        # All inputs:
        # Variant annotation dataset
        va = VariantAnnotation.from_parquet(self.session, self.variant_annotation_path)
        # GWAS Catalog raw study information
        catalog_studies = self.session.spark.read.csv(
            self.catalog_studies_file, sep="\t", header=True
        )
        # GWAS Catalog ancestry information
        ancestry_lut = self.session.spark.read.csv(
            self.catalog_ancestry_file, sep="\t", header=True
        )
        # GWAS Catalog summary statistics information
        sumstats_lut = self.session.spark.read.csv(
            self.catalog_sumstats_lut, sep="\t", header=False
        )
        # GWAS Catalog raw association information
        catalog_associations = self.session.spark.read.csv(
            self.catalog_associations_file, sep="\t", header=True
        )
        # LD index dataset
        ld_index = LDIndex.from_parquet(self.session, self.ld_index_path)

        # Transform:
        # GWAS Catalog study index and study-locus splitted
        study_index, study_locus = GWASCatalogStudySplitter.split(
            GWASCatalogStudyIndex.from_source(
                catalog_studies, ancestry_lut, sumstats_lut
            ),
            GWASCatalogAssociations.from_source(catalog_associations, va),
        )

        # Annotate LD information and clump associations dataset
        study_locus = study_locus.annotate_ld(study_index, ld_index).clump()

        # Fine-mapping LD-clumped study-locus using PICS
        finemapped_study_locus = PICS.finemap(study_locus).annotate_credible_sets()

        # Write:
        study_index.df.write.mode(self.session.write_mode).parquet(
            self.catalog_studies_out
        )
        finemapped_study_locus.df.write.mode(self.session.write_mode).parquet(
            self.catalog_associations_out
        )
