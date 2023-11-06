"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass, field

import hail as hl
from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.gnomad.variants import GnomADVariants


@dataclass
class VariantAnnotationStep:
    """Variant annotation step.

    Variant annotation step produces a dataset of the type `VariantAnnotation` derived from gnomADs `gnomad.genomes.vX.X.X.sites.ht` Hail's table. This dataset is used to validate variants and as a source of annotation.

    Attributes:
        session (Session): Session object.
        start_hail (bool): Whether to start a Hail session. Defaults to True.
        gnomad_genomes (str): Path to gnomAD genomes hail table.
        chain_38_to_37 (str): Path to GRCh38 to GRCh37 chain file.
        variant_annotation_path (str): Output variant annotation path.
        populations (List[str]): List of populations to include.
    """

    session: Session = Session()
    start_hail: bool = field(
        default=True,
    )

    gnomad_genomes: str = MISSING
    chain_38_to_37: str = MISSING
    variant_annotation_path: str = MISSING
    populations: list[str] = field(
        default_factory=lambda: [
            "afr",  # African-American
            "amr",  # American Admixed/Latino
            "ami",  # Amish ancestry
            "asj",  # Ashkenazi Jewish
            "eas",  # East Asian
            "fin",  # Finnish
            "nfe",  # Non-Finnish European
            "mid",  # Middle Eastern
            "sas",  # South Asian
            "oth",  # Other
        ]
    )

    def __post_init__(self: VariantAnnotationStep) -> None:
        """Run step."""
        # Initialise hail session.
        hl.init(sc=self.session.spark.sparkContext, log="/dev/null")
        # Run variant annotation.
        variant_annotation = GnomADVariants.as_variant_annotation(
            self.gnomad_genomes,
            self.chain_38_to_37,
            self.populations,
        )
        # Write data partitioned by chromosome and position.
        (
            variant_annotation.df.repartition(400, "chromosome")
            .sortWithinPartitions("chromosome", "position")
            .write.partitionBy("chromosome")
            .mode(self.session.write_mode)
            .parquet(self.variant_annotation_path)
        )
