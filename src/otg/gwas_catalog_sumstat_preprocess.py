"""Step to generate variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from otg.config import GWASCatalogSumstatsPreprocessConfig

if TYPE_CHECKING:
    from otg.common.session import Session


@dataclass
class GWASCatalogSumstatsPreprocessStep(GWASCatalogSumstatsPreprocessConfig):
    """Step to preprocess GWAS Catalog harmonised summary stats."""

    session: Session = SparkSession.builder.getOrCreate()

    def run(self: GWASCatalogSumstatsPreprocessConfig) -> None:
        """Run Step."""
        # Extract
        print(self.raw_sumstats_path)
        print(self.out_sumstats_path)
        print("WORKING!")
