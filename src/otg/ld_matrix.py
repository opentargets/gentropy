"""Step to dump a filtered version of a LD matrix (block matrix) as Parquet files."""
from __future__ import annotations

from dataclasses import dataclass

import hail as hl
from hail.linalg import BlockMatrix

from otg.common.session import Session
from otg.config import LDMatrixStepConfig


@dataclass
class LDMatrixStep(LDMatrixStepConfig):
    """LD matrix step."""

    session: Session = Session()

    def run(self: LDMatrixStep) -> None:
        """Run LD matrix dump step."""
        hl.init(sc=self.session.spark.sparkContext, log="/dev/null")

        for population in self.ld_populations:
            self.session.logger.info(f"Processing population: {population}")
            pop_matrix_path = self.ld_matrix_raw_template.format(POP=population)
            bm = BlockMatrix.read(pop_matrix_path)
            table = bm.entries(keyed=False)
            filtered_df = table.filter(hl.abs(table.entry) >= 0.5**0.5).to_spark()
            self.session.logger.info(
                f"Writing LD matrix to: {self.ld_matrix_template.format(POP=population)}"
            )
            (
                filtered_df.withColumnRenamed("entry", "r")
                .write.mode(self.session.write_mode)
                .parquet(self.ld_matrix_template.format(POP=population))  # noqa: FS002
            )
