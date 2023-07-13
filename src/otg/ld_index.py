"""Step to dump a filtered version of a LD matrix (block matrix) as Parquet files."""
from __future__ import annotations

from dataclasses import dataclass

import hail as hl

from otg.common.session import Session
from otg.config import LDIndexStepConfig
from otg.dataset.ld_index import LDIndex


@dataclass
class LDIndexStep(LDIndexStepConfig):
    """LD index step."""

    session: Session = Session()

    def run(self: LDIndexStep) -> None:
        """Run LD index dump step."""
        hl.init(sc=self.session.spark.sparkContext, log="/dev/null")

        ld_index = LDIndex.from_gnomad(
            self.ld_populations,
            self.ld_matrix_template,
            self.ld_index_raw_template,
            self.grch37_to_grch38_chain_path,
            self.min_r2,
        )
        self.session.logger.info(f"Writing LD index to: {self.ld_index_out}")
        (
            ld_index.df.write.mode(self.session.write_mode).parquet(
                f"{self.ld_index_out}_eur"
            )
        )
