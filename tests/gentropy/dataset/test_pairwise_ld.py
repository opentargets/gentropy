"""Testing pairwise LD dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING

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
