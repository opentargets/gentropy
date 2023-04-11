"""Step to generate variant index dataset."""
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
        """Run LD index step."""
        # init hail session
        hl.init(sc=self.session.spark.sparkContext, log="/dev/null")

        for population in self.ld_populations:
            self.session.logger.info(f"Processing population: {population}")
            ld_index = LDIndex.create(
                self.ld_index_raw_template.format(POP=population),
                self.ld_radius,
                self.grch37_to_grch38_chain_path,
            )

            self.session.logger.info(
                f"Writing LD index to: {self.ld_index_template.format(POP=population)}"
            )
            (
                ld_index.df.write.partitionBy("chromosome")
                .mode(self.session.write_mode)
                .parquet(self.ld_index_template.format(POP=population))  # noqa: FS002
            )
