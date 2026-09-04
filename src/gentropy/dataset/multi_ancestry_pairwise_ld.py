"""Multi-ancestry pairwise LD dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.pairwise_ld import PairwiseLD

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class MultiAncestryPairwiseLD(Dataset):
    """Global ancestry-aware LD pairs for downstream locus-specific filtering."""

    @classmethod
    def get_schema(cls: type[MultiAncestryPairwiseLD]) -> StructType:
        """Provide the schema for the dataset.

        Returns:
            StructType: Schema for the multi-ancestry pairwise LD dataset.
        """
        return parse_spark_schema("multi_ancestry_pairwise_ld.json")

    def overlap_with_locus(
        self: MultiAncestryPairwiseLD,
        ancestry: str,
        locus_variants: list[str],
    ) -> PairwiseLD:
        """Return one ancestry-specific locus matrix using PairwiseLD.

        Args:
            ancestry (str): Ancestry to project into a PairwiseLD dataset.
            locus_variants (list[str]): Variants defining one locus matrix.

        Returns:
            PairwiseLD: Ancestry-specific pairwise LD for the locus.
        """
        if ancestry not in self.ancestries():
            raise ValueError(f"Unknown ancestry: {ancestry}")
        return PairwiseLD(
            _df=(
                self.df.filter(
                    (f.col("ancestry") == ancestry)
                    & f.col("variantIdI").isin(locus_variants)
                    & f.col("variantIdJ").isin(locus_variants)
                ).drop("ancestry")
            ),
            _schema=PairwiseLD.get_schema(),
            variant_ids=locus_variants,
        )

    def ancestries(self: MultiAncestryPairwiseLD) -> list[str]:
        """Return ancestry labels in deterministic order.

        Returns:
            list[str]: Sorted ancestry labels.
        """
        return [
            row["ancestry"]
            for row in self.df.select("ancestry")
            .distinct()
            .orderBy("ancestry")
            .collect()
        ]
