"""Test the multi-ancestry pairwise LD dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from gentropy.dataset.multi_ancestry_pairwise_ld import MultiAncestryPairwiseLD

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TestMultiAncestryPairwiseLD:
    """Test the combined ancestry-aware pairwise LD contract."""

    def test_supports_different_dimensions_and_extracts_pairwise_ld(
        self, spark: SparkSession
    ) -> None:
        """Each ancestry can have a different square matrix dimension."""
        eur_variants = ["1_100_A_C", "1_200_G_A"]
        afr_variants = ["1_100_A_C", "1_200_G_A", "1_300_C_T"]
        rows = [
            (ancestry, variant_i, variant_j, 1.0 if variant_i == variant_j else 0.2)
            for ancestry, variants in (("EUR", eur_variants), ("AFR", afr_variants))
            for variant_i in variants
            for variant_j in variants
        ]

        dataset = MultiAncestryPairwiseLD(
            _df=spark.createDataFrame(
                rows, ["ancestry", "variantIdI", "variantIdJ", "r"]
            ),
            _schema=MultiAncestryPairwiseLD.get_schema(),
        )

        assert dataset.ancestries() == ["AFR", "EUR"]
        assert dataset.dimensions == {"AFR": (3, 3), "EUR": (2, 2)}
        assert dataset.for_ancestry("EUR").r_to_numpy_matrix().shape == (2, 2)

    def test_rejects_unknown_ancestry(self, spark: SparkSession) -> None:
        """An unknown ancestry cannot be projected to PairwiseLD."""
        dataset = MultiAncestryPairwiseLD(
            _df=spark.createDataFrame(
                [("EUR", "1_100_A_C", "1_100_A_C", 1.0)],
                ["ancestry", "variantIdI", "variantIdJ", "r"],
            ),
            _schema=MultiAncestryPairwiseLD.get_schema(),
        )

        with pytest.raises(ValueError, match="Unknown ancestry"):
            dataset.for_ancestry("AFR")
