"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass, field
from functools import reduce
from typing import Dict, List

import pyspark.sql.functions as f
from omegaconf import MISSING

from otg.common.Liftover import LiftOverSpark
from otg.common.session import Session
from otg.dataset.gene_index import GeneIndex
from otg.dataset.intervals import Intervals
from otg.dataset.v2g import V2G
from otg.dataset.variant_annotation import VariantAnnotation
from otg.dataset.variant_index import VariantIndex


@dataclass
class V2GStep:
    """Variant-to-gene (V2G) step.

    This step aims to generate a dataset that contains multiple pieces of evidence supporting the functional association of specific variants with genes. Some of the evidence types include:

    1. Chromatin interaction experiments, e.g. Promoter Capture Hi-C (PCHi-C).
    2. In silico functional predictions, e.g. Variant Effect Predictor (VEP) from Ensembl.
    3. Distance between the variant and each gene's canonical transcription start site (TSS).

    Attributes:
        variant_index_path (str): Input variant index path.
        variant_annotation_path (str): Input variant annotation path.
        gene_index_path (str): Input gene index path.
        vep_consequences_path (str): Input VEP consequences path.
        liftover_chain_file_path (str): Path to GRCh37 to GRCh38 chain file.
        liftover_max_length_difference: Maximum length difference for liftover.
        max_distance (int): Maximum distance to consider.
        approved_biotypes (list[str]): List of approved biotypes.
        intervals (dict): Dictionary of interval sources.
        v2g_path (str): Output V2G path.
    """

    session: Session = Session()

    variant_index_path: str = MISSING
    variant_annotation_path: str = MISSING
    gene_index_path: str = MISSING
    vep_consequences_path: str = MISSING
    liftover_chain_file_path: str = MISSING
    liftover_max_length_difference: int = 100
    max_distance: int = 500_000
    approved_biotypes: List[str] = field(
        default_factory=lambda: [
            "protein_coding",
            "3prime_overlapping_ncRNA",
            "antisense",
            "bidirectional_promoter_lncRNA",
            "IG_C_gene",
            "IG_D_gene",
            "IG_J_gene",
            "IG_V_gene",
            "lincRNA",
            "macro_lncRNA",
            "non_coding",
            "sense_intronic",
            "sense_overlapping",
        ]
    )
    intervals: Dict[str, str] = field(default_factory=dict)
    v2g_path: str = MISSING

    def __post_init__(self: V2GStep) -> None:
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
        intervals = Intervals(
            _df=reduce(
                lambda x, y: x.unionByName(y, allowMissingColumns=True),
                # create interval instances by parsing each source
                [
                    Intervals.from_source(
                        self.session.spark, source_name, source_path, gene_index, lift
                    ).df
                    for source_name, source_path in self.intervals.items()
                ],
            ),
            _schema=Intervals.get_schema(),
        )
        v2g_datasets = [
            va_slimmed.get_distance_to_tss(gene_index_filtered, self.max_distance),
            va_slimmed.get_most_severe_vep_v2g(vep_consequences, gene_index_filtered),
            va_slimmed.get_polyphen_v2g(gene_index_filtered),
            va_slimmed.get_sift_v2g(gene_index_filtered),
            va_slimmed.get_plof_v2g(gene_index_filtered),
            intervals.v2g(vi),
        ]
        v2g = V2G(
            _df=reduce(
                lambda x, y: x.unionByName(y, allowMissingColumns=True),
                [dataset.df for dataset in v2g_datasets],
            ).repartition("chromosome"),
            _schema=V2G.get_schema(),
        )

        # Load
        (
            v2g.df.write.partitionBy("chromosome")
            .mode(self.session.write_mode)
            .parquet(self.v2g_path)
        )
