"""Step to generate variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.dataset.ld_index import LDIndex

if TYPE_CHECKING:
    from otg.common.session import ETLSession


@dataclass
class LDIndexStep:
    """LD index step."""

    etl: ETLSession
    pop_ldindex_path: str
    ld_radius: int
    grch37_to_grch38_chain_path: str
    ld_index_path: str
    id: str = "ld_index"

    def run(self: LDIndexStep) -> None:
        """Step to generate LD index.

        Using a `VariantAnnotation` dataset as a reference, this step creates and writes a dataset of the type `VariantIndex` that includes only variants that have disease-association data with a reduced set of annotations.
        """
        self.etl.logger.info(f"Executing {self.id} step")

        ld_index = LDIndex.create(
            self.pop_ldindex_path, self.ld_radius, self.grch37_to_grch38_chain_path
        )

        self.etl.logger.info(f"Writing variant index to: {self.ld_index_path}")
        (
            ld_index.df.write.partitionBy("chromosome")
            .mode(self.etl.write_mode)
            .parquet(self.ld_index_path)
        )
