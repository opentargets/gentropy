"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from functools import reduce

import pyspark.sql.functions as f

from otg.common.Liftover import LiftOverSpark
from otg.common.session import Session
from otg.config import V2GStepConfig
from otg.dataset.gene_index import GeneIndex
from otg.dataset.intervals import Intervals
from otg.dataset.v2g import V2G
from otg.dataset.variant_annotation import VariantAnnotation
from otg.dataset.variant_index import VariantIndex
from otg.datasource.intervals.andersson import IntervalsAndersson
from otg.datasource.intervals.javierre import IntervalsJavierre
from otg.datasource.intervals.jung import IntervalsJung
from otg.datasource.intervals.thurman import IntervalsThurman


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
        # Read
        gene_index = GeneIndex.from_parquet(self.session, self.gene_index_path)
        vi = VariantIndex.from_parquet(self.session, self.variant_index_path).persist()
        va = VariantAnnotation.from_parquet(self.session, self.variant_annotation_path)
        vep_consequences = self.session.spark.read.csv(
            self.vep_consequences_path, sep="\t", header=True
        ).select(
            f.element_at(f.split("Accession", r"/"), -1).alias(
                "variantFunctionalConsequenceId"
            ),
            f.col("Term").alias("label"),
            f.col("v2g_score").cast("double").alias("score"),
        )
        anderson = IntervalsAndersson.read_andersson(
            self.session.spark, self.anderson_path
        )
        javierre = IntervalsJavierre.read_javierre(
            self.session.spark, self.javierre_path
        )
        jung = IntervalsJung.read_jung(self.session.spark, self.jung_path)
        thurman = IntervalsThurman.read_thurman(self.session.spark, self.thurman_path)

        # Transform
        lift = LiftOverSpark(
            # lift over variants to hg38
            self.liftover_chain_file_path,
            self.liftover_max_length_difference,
        )
        gene_index_filtered = gene_index.filter_by_biotypes(
            # Filter gene index by approved biotypes to define V2G gene universe
            list(self.approved_biotypes)
        )
        va_slimmed = va.filter_by_variant_df(
            # Variant annotation reduced to the variant index to define V2G variant universe
            vi.df
        ).persist()
        intervals_datasets = Intervals.collect_interval_data(
            anderson, javierre, jung, thurman, gene_index_filtered, lift
        )

        # Expected andersson et al. schema:
        v2g_datasets = [
            va_slimmed.get_distance_to_tss(gene_index_filtered, self.max_distance),
            # variant effects
            va_slimmed.get_most_severe_vep_v2g(vep_consequences, gene_index_filtered),
            va_slimmed.get_polyphen_v2g(gene_index_filtered),
            va_slimmed.get_sift_v2g(gene_index_filtered),
            va_slimmed.get_plof_v2g(gene_index_filtered),
            intervals_datasets.v2g(vi),
        ]

        # merge all V2G datasets
        v2g = V2G(
            _df=reduce(
                lambda x, y: x.unionByName(y, allowMissingColumns=True),
                [dataset.df for dataset in v2g_datasets],
            ).repartition("chromosome"),
            _schema=V2G.get_schema(),
        )
        # write V2G dataset
        (
            v2g.df.write.partitionBy("chromosome")
            .mode(self.session.write_mode)
            .parquet(self.v2g_path)
        )
