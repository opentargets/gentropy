"""Tests on helper functions that extract V2G assignments from chromatin interaction experiments."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pandas.testing import assert_frame_equal

from otg.v2g.intervals.helpers import get_variants_in_interval

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="class")
def mock_variants_df(spark: SparkSession) -> DataFrame:
    """Creates a mock dataframe with a single interval."""
    return spark.createDataFrame(
        data=[
            # Variants inside the interval
            ("20_5648222_T_C", "20", 5648222),
            ("20_5648223_T_C", "20", 5648223),
            # Variants outside the interval
            ("20_5647944_T_C", "20", 5648300),
        ],
        schema=["variantId", "chromosome", "position"],
    )


class TestGetVariantsInInterval:
    """Test the get_variants_in_interval util."""

    test_input = [
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
        ]
    ]

    expected_output = [
        [
            (
                "20_5648222_T_C",
                "20",
                5648222,
                "ENSG00000125772",
                0.783,
                "andersson2014",
                "fantom5",
                "24670763",
                "aggregate",
            ),
            (
                "20_5648223_T_C",
                "20",
                5648223,
                "ENSG00000125772",
                0.783,
                "andersson2014",
                "fantom5",
                "24670763",
                "aggregate",
            ),
        ]
    ]

    @pytest.mark.parametrize(
        "test_input, expected_output", zip(test_input, expected_output)
    )
    def test_explosion_of_region_into_v2g(
        self: TestGetVariantsInInterval,
        spark: SparkSession,
        test_input: list[tuple],
        expected_output: list[tuple],
        mock_variants_df: DataFrame,
    ) -> None:
        """Tests that a df with data about a region is transformed to a dataframe where all the variants in that region are contained following the V2G model."""
        mock_df = spark.createDataFrame(
            data=test_input,
            schema=[
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

        test_df = mock_df.transform(
            lambda df: get_variants_in_interval(df, mock_variants_df)
        ).toPandas()
        expected_df = spark.createDataFrame(
            data=expected_output,
            schema=[
                "variantId",
                "chromosome",
                "position",
                "geneId",
                "resourceScore",
                "datasourceId",
                "datatypeId",
                "pmid",
                "biofeature",
            ],
        ).toPandas()
        assert_frame_equal(test_df, expected_df, check_like=True)
