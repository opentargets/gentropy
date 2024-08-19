"""Testing pairwise LD dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pytest
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from gentropy.dataset.pairwise_ld import PairwiseLD

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TestPairwiseLD:
    """Test suit for pairwise LD dataset and associated methods."""

    variants = [
        "1_8_A_C",
        "1_9_A_C",
        "1_10_A_C",
        "1_99_A_C",
    ]

    @pytest.fixture(scope="class")
    def mock_pairwise_ld(self: TestPairwiseLD, spark: SparkSession) -> PairwiseLD:
        """Generate a mock pairwise LD dataset.

        Args:
            spark (SparkSession): _description_

        Returns:
            PairwiseLD: _description_
        """
        spark = spark.builder.getOrCreate()

        data = [(v1, v2) for v1 in self.variants for v2 in self.variants]
        return PairwiseLD(
            _df=(
                spark.createDataFrame(data, ["variantIdI", "variantIdJ"])
                .withColumn(
                    "r",
                    f.row_number()
                    .over(Window.partitionBy(f.lit("x")).orderBy("variantIdI"))
                    .cast("double"),
                )
                .withColumn(
                    "r",
                    f.when(f.col("variantIdI") == f.col("variantIdJ"), 1.0).otherwise(
                        f.col("r")
                    ),
                )
                .persist()
            ),
            _schema=PairwiseLD.get_schema(),
        )

    @staticmethod
    def test_pairwise_ld__type(mock_pairwise_ld: PairwiseLD) -> None:
        """Testing type."""
        assert isinstance(mock_pairwise_ld, PairwiseLD)

    def test_pariwise_ld__get_variants(
        self: TestPairwiseLD, mock_pairwise_ld: PairwiseLD
    ) -> None:
        """Testing function that returns list of variants from the LD table.

        Args:
            mock_pairwise_ld (PairwiseLD): _description_
        """
        variant_set_expected = set(self.variants)
        variant_set_from_data = set(mock_pairwise_ld.get_variant_list())

        assert variant_set_from_data == variant_set_expected

    def test_pairwise_ld__r_to_numpy_matrix__type(
        self: TestPairwiseLD, mock_pairwise_ld: PairwiseLD
    ) -> None:
        """Testing the returned numpy array."""
        assert isinstance(mock_pairwise_ld.r_to_numpy_matrix(), np.ndarray)

    def test_pairwise_ld__r_to_numpy_matrix__dimensions(
        self: TestPairwiseLD, mock_pairwise_ld: PairwiseLD
    ) -> None:
        """Testing the returned numpy array."""
        assert mock_pairwise_ld.r_to_numpy_matrix().shape == (
            len(self.variants),
            len(self.variants),
        )

    def test_pairwise_ld__overlap_with_locus(
        self: TestPairwiseLD, mock_pairwise_ld: PairwiseLD
    ) -> None:
        """Testing the returned numpy array."""
        variant_subset = self.variants[1:3]

        assert (
            mock_pairwise_ld.overlap_with_locus(variant_subset).df.count()
            == len(variant_subset) ** 2
        )
