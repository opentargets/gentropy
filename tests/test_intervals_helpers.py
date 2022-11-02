"""Tests on helper functions that extract V2G assignments from chromatin interaction experiments."""
from __future__ import annotations

from typing import TYPE_CHECKING

from pandas.testing import assert_frame_equal

from etl.v2g.intervals.helpers import get_variants_in_interval

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TestGetVariantsInInterval:
    """Test the get_variants_in_interval util."""

    def __init__(self: TestGetVariantsInInterval, spark: SparkSession) -> None:
        """Initialize the test class."""
        self.spark = spark
        self.mock_interval_df = spark.createDataFrame(
            [
                (
                    "20",
                    5647945,
                    5648249,
                    "ENSG00000125772",
                    0.783,
                    "andersson2014",
                    "fantom5",
                    "24670763",
                    "aggregate",
                )
            ],
            [
                "chromosome",
                "start",
                "end",
                "geneId",
                "resourceScore",
                "datasourceId",
                "datatypeId",
                "pmid",
                "biofeature",
            ],
        )

    def test_explosion_of_region_into_v2g(
        self: TestGetVariantsInInterval,
    ) -> None:
        """Tests that a df with data about a region is transformed to a dataframe where all the variants in that region are contained following the V2G model."""
        mock_variants_df = self.spark.createDataFrame(
            [
                ("20_5648222_T_C", "20", 5648222),
                ("20_5648223_T_C", "20", 5648223),
            ],
            ["id", "chromosome", "position"],
        )

        test_df = self.mock_interval_df.transform(
            lambda df: get_variants_in_interval(df, mock_variants_df)
        )
        expected_df = self.spark.createDataFrame(
            [
                (
                    "20_5648222_T_C",
                    "ENSG00000125772",
                    0.783,
                    "andersson2014",
                    "fantom5",
                    "24670763",
                    "aggregate",
                ),
                (
                    "20_5648223_T_C",
                    "ENSG00000125772",
                    0.783,
                    "andersson2014",
                    "fantom5",
                    "24670763",
                    "aggregate",
                ),
            ],
            [
                "variantId",
                "geneId",
                "resourceScore",
                "datasourceId",
                "datatypeId",
                "pmid",
                "biofeature",
            ],
        )
        assert_frame_equal(test_df.toPandas(), expected_df.toPandas(), check_like=True)
