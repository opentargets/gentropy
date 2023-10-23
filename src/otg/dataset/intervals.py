"""Interval dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset
from otg.dataset.v2g import V2G

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from otg.dataset.variant_index import VariantIndex


@dataclass
class Intervals(Dataset):
    """Intervals dataset links genes to genomic regions based on genome interaction studies."""

    @classmethod
    def get_schema(cls: type[Intervals]) -> StructType:
        """Provides the schema for the Intervals dataset."""
        return parse_spark_schema("intervals.json")

    # def collect_interval_data(
    #     self: Intervals,
    #     gene_index: GeneIndex,
    #     lift: LiftOverSpark,
    # ) -> Intervals:
    #     """Collect interval data from multiple sources.

    #     Args:
    #         sources (list[str]): List of interval sources
    #         gene_index (GeneIndex): Gene index
    #         lift (LiftOverSpark): LiftOverSpark instance to convert coordinats from hg37 to hg38

    #     Returns:
    #         Intervals: Intervals dataset
    #     """
    #     pass

    def v2g(self: Intervals, variant_index: VariantIndex) -> V2G:
        """Convert intervals into V2G by intersecting with a variant index.

        Args:
            variant_index (VariantIndex): Variant index dataset

        Returns:
            V2G: Variant-to-gene evidence dataset
        """
        return V2G(
            _df=(
                self.df.alias("interval")
                .join(
                    variant_index.df.selectExpr(
                        "chromosome as vi_chromosome", "variantId", "position"
                    ).alias("vi"),
                    on=[
                        f.col("vi.vi_chromosome") == f.col("interval.chromosome"),
                        f.col("vi.position").between(
                            f.col("interval.start"), f.col("interval.end")
                        ),
                    ],
                    how="inner",
                )
                .drop("start", "end", "vi_chromosome", "position")
            ),
            _schema=V2G.get_schema(),
        )
