"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.common.gwas_catalog_splitter import GWASCatalogSplitter
from otg.dataset.study_index import StudyIndexGWASCatalog
from otg.dataset.study_locus import StudyLocusGWASCatalog
from otg.dataset.variant_annotation import VariantAnnotation

if TYPE_CHECKING:
    from otg.common.session import ETLSession


@dataclass
class GWASCatalogStep:
    """Variant annotation step.

    Variant annotation step produces a dataset of the type `VariantAnnotation` derived from gnomADs `gnomad.genomes.vX.X.X.sites.ht` Hail's table. This dataset is used to validate variants and as a source of annotation.
    """

    etl: ETLSession
    id: str = "gwas_catalog"

    def run(self: GWASCatalogStep) -> None:
        """Run variant annotation step."""
        self.etl.logger.info(f"Executing {self.id} step")

        gwas_study_file = ""
        ancestry_file = ""
        summarystats_list = ""
        gwas_association_file = ""
        variant_annotation_path = ""
        ld_populations = ""
        min_r2 = 0.5

        # All inputs:
        # Variant annotation dataset
        va = VariantAnnotation.from_parquet(self.etl, variant_annotation_path)
        # GWAS Catalog raw study information
        catalog_studies = self.etl.spark.read.csv(
            gwas_study_file, sep="\t", header=True
        )
        # GWAS Catalog ancestry information
        ancestry_lut = self.etl.spark.read.csv(ancestry_file, sep="\t", header=True)
        # GWAS Catalog summary statistics information
        sumstats_lut = self.etl.spark.read.csv(
            summarystats_list, sep="\t", header=False
        )
        # GWAS Catalog raw association information
        catalog_associations = self.etl.spark.read.csv(
            gwas_association_file, sep="\t", header=True
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
            self.etl, catalog_studies, ld_populations, min_r2
        )
