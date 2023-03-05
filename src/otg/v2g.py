"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import TYPE_CHECKING

from otg.common.Liftover import LiftOverSpark
from otg.dataset.gene_index import GeneIndex
from otg.dataset.intervals import Intervals
from otg.dataset.v2g import V2G
from otg.dataset.variant_annotation import VariantAnnotation
from otg.dataset.variant_index import VariantIndex

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from otg.common.session import ETLSession


@dataclass
class V2GStep:
    """Variant-to-gene (V2G) step.

    This step aims to generate a dataset that contains multiple pieces of evidence supporting the functional association of specific variants with genes. Some of the evidence types include:

    1. Chromatin interaction experiments, e.g. Promoter Capture Hi-C (PCHi-C).
    2. In silico functional predictions, e.g. Variant Effect Predictor (VEP) from Ensembl.
    3. Distance between the variant and each gene's canonical transcription start site (TSS).

    """

    etl: ETLSession
    variant_index: DictConfig
    variant_annotation: DictConfig
    gene_index: DictConfig
    vep_consequences_path: str
    liftover_chain_file_path: str
    approved_biotypes: list[str]
    anderson_path: str
    javierre_path: str
    jung_path: str
    thurnman_path: str
    output_path: str
    liftover_max_length_difference: int = 100
    max_distance: int = 500_000

    id: str = "variant2gene"

    def run(self: V2GStep) -> None:
        """Run V2G dataset generation."""
        self.etl.logger.info(f"Executing {self.id} step")

        # Filter gene index by approved biotypes to define V2G gene universe
        gene_index_filtered = GeneIndex.from_parquet(
            self.etl, self.gene_index.path
        ).filter_by_biotypes(self.approved_biotypes)

        vi = VariantIndex.from_parquet(self.etl, self.variant_index.path).persist()
        va = VariantAnnotation.from_parquet(self.etl, self.variant_annotation.path)

        # Variant annotation reduced to the variant index to define V2G variant universe
        va_slimmed = va.filter_by_variant_df(vi, ["id", "chromosome"]).persist()

        # lift over variants to hg38
        lift = LiftOverSpark(
            self.liftover_chain_file_path, self.liftover_max_length_difference
        )

        v2g_datasets = [
            va.slimmed.get_distance_to_tss(gene_index_filtered, self.max_distance),
            # variant effects
            va_slimmed.get_most_severe_variant_consequence(
                self.vep_consequences_path, gene_index_filtered
            ),
            va_slimmed.get_polyphen_v2g(gene_index_filtered),
            va_slimmed.get_sift_v2g(gene_index_filtered),
            va_slimmed.get_plof_v2g(gene_index_filtered),
            # intervals
            Intervals.parse_andersson(
                self.etl, self.anderson_path, gene_index_filtered, lift
            ).v2g(vi),
            Intervals.parse_javierre(
                self.etl, self.javierre_path, gene_index_filtered, lift
            ).v2g(vi),
            Intervals.parse_jung(
                self.etl, self.jung_path, gene_index_filtered, lift
            ).v2g(vi),
            Intervals.parse_thurnman(
                self.etl, self.thurnman_path, gene_index_filtered, lift
            ).v2g(vi),
        ]

        # merge all V2G datasets
        v2g = V2G(
            df=reduce(
                lambda x, y: x.unionByName(y, allowMissingColumns=True),
                [dataset.df for dataset in v2g_datasets],
            ).repartition("chromosome")
        )

        (
            v2g.df.write.partitionBy("chromosome")
            .mode(self.etl.write_mode)
            .parquet(self.output_path)
        )
