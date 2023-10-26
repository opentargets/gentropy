"""Step to dump a filtered version of a LD matrix (block matrix) as Parquet files."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

import hail as hl
from omegaconf import MISSING

from otg.common.session import Session
from otg.datasource.gnomad.ld import GnomADLDMatrix


@dataclass
class LDIndexStep:
    """LD index step.

    !!! warning "This step is resource intensive"
        Suggested params: high memory machine, 5TB of boot disk, no SSDs.

    Attributes:
        ld_matrix_template (str): Template path for LD matrix from gnomAD.
        ld_index_raw_template (str): Template path for the variant indices correspondance in the LD Matrix from gnomAD.
        min_r2 (float): Minimum r2 to consider when considering variants within a window.
        grch37_to_grch38_chain_path (str): Path to GRCh37 to GRCh38 chain file.
        ld_populations (List[str]): List of population-specific LD matrices to process.
        ld_index_out (str): Output LD index path.
    """

    session: Session = Session()

    ld_matrix_template: str = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.adj.ld.bm"
    ld_index_raw_template: str = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.ld.variant_indices.ht"
    min_r2: float = 0.5
    grch37_to_grch38_chain_path: str = (
        "gs://hail-common/references/grch37_to_grch38.over.chain.gz"
    )
    ld_populations: List[str] = field(
        default_factory=lambda: [
            "afr",  # African-American
            "amr",  # American Admixed/Latino
            "asj",  # Ashkenazi Jewish
            "eas",  # East Asian
            "fin",  # Finnish
            "nfe",  # Non-Finnish European
            "nwe",  # Northwestern European
            "seu",  # Southeastern European
        ]
    )
    ld_index_out: str = MISSING

    def __post_init__(self: LDIndexStep) -> None:
        """Run LD index dump step."""
        hl.init(sc=self.session.spark.sparkContext, log="/dev/null")
        ld_index = GnomADLDMatrix.as_ld_index(
            self.ld_populations,
            self.ld_matrix_template,
            self.ld_index_raw_template,
            self.grch37_to_grch38_chain_path,
            self.min_r2,
        )
        self.session.logger.info(f"Writing LD index to: {self.ld_index_out}")
        (
            ld_index.df.write.partitionBy("chromosome")
            .mode(self.session.write_mode)
            .parquet(f"{self.ld_index_out}")
        )
