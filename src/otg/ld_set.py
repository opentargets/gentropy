"""Step to dump a filtered version of a LD matrix (block matrix) as Parquet files."""
from __future__ import annotations

from dataclasses import dataclass

import hail as hl

from otg.common.session import Session
from otg.config import LDIndexStepConfig
from otg.dataset.ld_set import LDIndex


@dataclass
class LDIndexStep(LDIndexStepConfig):
    """LD matrix step."""

    session: Session = Session()

    def run(self: LDIndexStep) -> None:
        """Run LD matrix dump step."""
        hl.init(sc=self.session.spark.sparkContext, log="/dev/null")

        for population in self.ld_populations:
            self.session.logger.info(f"Processing population: {population}")
            ld_matrix_path = self.ld_matrix_template.format(POP=population)
            ld_index_raw_path = self.ld_index_raw_template.format(POP=population)
            ld_index = LDIndex.create(
                ld_matrix_path,
                ld_index_raw_path,
                self.grch37_to_grch38_chain_path,
                self.min_r2,
            )
            self.session.logger.info(f"Writing LD index to: {self.ld_index_out}")
            (
                ld_index.write.mode(self.session.write_mode).parquet(
                    f"{self.ld_index_out}_eur"
                )  # noqa: FS002
            )
