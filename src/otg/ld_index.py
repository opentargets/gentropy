"""Step to dump a filtered version of a LD matrix (block matrix) as Parquet files."""
from __future__ import annotations

from dataclasses import dataclass

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
        session (Session): Session object.
        start_hail (bool): Whether to start Hail. Defaults to True.
        min_r2 (float): Minimum r2 to consider when considering variants within a window.
        ld_index_out (str): Output LD index path.
    """

    session: Session = Session()
    start_hail: bool = True
    min_r2: float = 0.5

    ld_index_out: str = MISSING

    def __post_init__(self: LDIndexStep) -> None:
        """Run step."""
        hl.init(sc=self.session.spark.sparkContext, log="/dev/null")
        (
            GnomADLDMatrix()
            .as_ld_index(self.min_r2)
            .df.write.partitionBy("chromosome")
            .mode(self.session.write_mode)
            .parquet(self.ld_index_out)
        )
        self.session.logger.info(f"LD index written to: {self.ld_index_out}")
