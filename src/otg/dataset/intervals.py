"""Interval dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from otg.common.Liftover import LiftOverSpark
from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset
from otg.dataset.gene_index import GeneIndex
from otg.dataset.v2g import V2G

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

    from otg.dataset.variant_index import VariantIndex


@dataclass
class Intervals(Dataset):
    """Intervals dataset links genes to genomic regions based on genome interaction studies."""

    @classmethod
    def get_schema(cls: type[Intervals]) -> StructType:
        """Provides the schema for the Intervals dataset.

        Returns:
            StructType: Schema for the Intervals dataset
        """
        return parse_spark_schema("intervals.json")

    @classmethod
    def from_source(
        cls: type[Intervals],
        spark: SparkSession,
        source_name: str,
        source_path: str,
        gene_index: GeneIndex,
        lift: LiftOverSpark,
    ) -> Intervals:
        """Collect interval data for a particular source.

        Args:
            spark (SparkSession): Spark session
            source_name (str): Name of the interval source
            source_path (str): Path to the interval source file
            gene_index (GeneIndex): Gene index
            lift (LiftOverSpark): LiftOverSpark instance to convert coordinats from hg37 to hg38

        Returns:
            Intervals: Intervals dataset

        Raises:
            ValueError: If the source name is not recognised
        """
        from otg.datasource.intervals.andersson import IntervalsAndersson
        from otg.datasource.intervals.javierre import IntervalsJavierre
        from otg.datasource.intervals.jung import IntervalsJung
        from otg.datasource.intervals.thurman import IntervalsThurman

        source_to_class = {
            "andersson": IntervalsAndersson,
            "javierre": IntervalsJavierre,
            "jung": IntervalsJung,
            "thurman": IntervalsThurman,
        }

        if source_name not in source_to_class:
            raise ValueError(f"Unknown interval source: {source_name}")

        source_class = source_to_class[source_name]
        data = source_class.read(spark, source_path)
        return source_class.parse(data, gene_index, lift)

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
