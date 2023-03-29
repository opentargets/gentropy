"""Step to generate variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from otg.config import LDIndexStepConfig
from otg.dataset.ld_index import LDIndex

if TYPE_CHECKING:
    from otg.common.session import Session


@dataclass
class LDIndexStep(LDIndexStepConfig):
    """LD index step."""

    session: Session = SparkSession.builder.getOrCreate()

    def run(self: LDIndexStepConfig) -> None:
        """Run LD index step."""
        for population in self.ld_populations:
            self.etl.logger.info(f"Processing population: {population}")
            ld_index = LDIndex.create(
                self.ld_index_raw_template.format(POP=population),
                self.ld_radius,
                self.grch37_to_grch38_chain_path,
            )

            self.etl.logger.info(
                f"Writing ls index to: {self.ld_index_template.format(POP=population)}"
            )
            (
                ld_index.df.write.partitionBy("chromosome")
                .mode(self.session.write_mode)
                .parquet(self.ld_index_template.format(POP=population))  # noqa: FS002
            )
