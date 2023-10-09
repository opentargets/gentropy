"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from functools import reduce

from otg.common.Liftover import LiftOverSpark
from otg.common.session import Session
from otg.config import V2GStepConfig
from otg.dataset.gene_index import GeneIndex
from otg.dataset.v2g import V2G
from otg.dataset.variant_annotation import VariantAnnotation
from otg.dataset.variant_index import VariantIndex
from otg.datasource.intervals.andersson import IntervalsAndersson
from otg.datasource.intervals.javierre import IntervalsJavierre
from otg.datasource.intervals.jung import IntervalsJung
from otg.datasource.intervals.thurnman import IntervalsThurnman


@dataclass
class V2GStep(V2GStepConfig):
    """Variant-to-gene (V2G) step.

    This step aims to generate a dataset that contains multiple pieces of evidence supporting the functional association of specific variants with genes. Some of the evidence types include:

    1. Chromatin interaction experiments, e.g. Promoter Capture Hi-C (PCHi-C).
    2. In silico functional predictions, e.g. Variant Effect Predictor (VEP) from Ensembl.
    3. Distance between the variant and each gene's canonical transcription start site (TSS).

    """

    session: Session = Session()

    def run(self: V2GStep) -> None:
        """Run V2G dataset generation."""
        # Filter gene index by approved biotypes to define V2G gene universe
        gene_index_filtered = GeneIndex.from_parquet(
            self.session, self.gene_index_path
        ).filter_by_biotypes(self.approved_biotypes)

        vi = VariantIndex.from_parquet(self.session, self.variant_index_path).persist()
        va = VariantAnnotation.from_parquet(self.session, self.variant_annotation_path)
        vep_consequences = self.session.spark.read.csv(
            self.vep_consequences_path, sep="\t", header=True
        )

        # Variant annotation reduced to the variant index to define V2G variant universe
        va_slimmed = va.filter_by_variant_df(vi.df, ["id", "chromosome"]).persist()

        # lift over variants to hg38
        lift = LiftOverSpark(
            self.liftover_chain_file_path, self.liftover_max_length_difference
        )

        # Expected andersson et al. schema:
        v2g_datasets = [
            va_slimmed.get_distance_to_tss(gene_index_filtered, self.max_distance),
            # variant effects
            va_slimmed.get_most_severe_vep_v2g(vep_consequences, gene_index_filtered),
            va_slimmed.get_polyphen_v2g(gene_index_filtered),
            va_slimmed.get_sift_v2g(gene_index_filtered),
            va_slimmed.get_plof_v2g(gene_index_filtered),
            # intervals
            IntervalsAndersson.parse(
                IntervalsAndersson.read_andersson(self.session, self.anderson_path),
                gene_index_filtered,
                lift,
            ).v2g(vi),
            IntervalsJavierre.parse(
                IntervalsJavierre.read_javierre(self.session, self.javierre_path),
                gene_index_filtered,
                lift,
            ).v2g(vi),
            IntervalsJung.parse(
                IntervalsJung.read_jung(self.session, self.jung_path),
                gene_index_filtered,
                lift,
            ).v2g(vi),
            IntervalsThurnman.parse(
                IntervalsThurnman.read_thurnman(self.session, self.thurnman_path),
                gene_index_filtered,
                lift,
            ).v2g(vi),
        ]

        # merge all V2G datasets
        v2g = V2G(
            _df=reduce(
                lambda x, y: x.unionByName(y, allowMissingColumns=True),
                [dataset.df for dataset in v2g_datasets],
            ).repartition("chromosome")
        )
        # write V2G dataset
        (
            v2g.df.write.partitionBy("chromosome")
            .mode(self.session.write_mode)
            .parquet(self.v2g_path)
        )
