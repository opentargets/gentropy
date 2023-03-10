"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from otg.common.gwas_catalog_splitter import GWASCatalogSplitter
from otg.config import GWASCatalogStepConfig
from otg.dataset.study_index import StudyIndexGWASCatalog
from otg.dataset.study_locus import StudyLocusGWASCatalog
from otg.dataset.variant_annotation import VariantAnnotation
from otg.method.pics import PICS

if TYPE_CHECKING:
    from otg.common.session import Session


@dataclass
class GWASCatalogStep(GWASCatalogStepConfig):
    """GWAS Catalog step."""

    session: Session = SparkSession.builder.getOrCreate()

    def run(self: GWASCatalogStep) -> None:
        """Run variant annotation step."""
        # All inputs:
        # Variant annotation dataset
        va = VariantAnnotation.from_parquet(self.etl, self.variant_annotation_path)
        # GWAS Catalog raw study information
        catalog_studies = self.etl.spark.read.csv(
            self.catalog_studies_file, sep="\t", header=True
        )
        # GWAS Catalog ancestry information
        ancestry_lut = self.etl.spark.read.csv(
            self.catalog_ancestry_file, sep="\t", header=True
        )
        # GWAS Catalog summary statistics information
        sumstats_lut = self.etl.spark.read.csv(
            self.catalog_sumstats_lut, sep="\t", header=False
        )
        # GWAS Catalog raw association information
        catalog_associations = self.etl.spark.read.csv(
            self.catalog_associations_file, sep="\t", header=True
        )

        # Transform:
        # GWAS Catalog study index and study-locus splitted
        study_index, study_locus = GWASCatalogSplitter.split(
            StudyIndexGWASCatalog.from_source(
                catalog_studies, ancestry_lut, sumstats_lut
            ),
            StudyLocusGWASCatalog.from_source(catalog_associations, va),
        )

        # Annotate LD information
        study_locus = study_locus.annotate_ld(
            self.etl,
            catalog_studies,
            self.ld_populations,
            self.ld_index_template,
            self.ld_matrix_template,
            self.self.min_r2,
        )

        # Fine-mapping LD-clumped study-locus using PICS
        finemapped_study_locus = (
            PICS.finemap(study_locus).annotate_credible_sets().clump()
        )

        # Write:
        study_index.df.write.mode(self.etl.write_mode).parquet(self.catalog_studies_out)
        finemapped_study_locus.df.write.mode(self.etl.write_mode).parquet(
            self.catalog_associations_out
        )
