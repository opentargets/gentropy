"""Tests on helper functions that extract V2G assignments based on a variant's distance to a gene's TSS."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.types as t
import pytest
from pandas.testing import assert_frame_equal

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from etl.v2g.distance.distance import get_variant_distance_to_gene


class TestGetVariantDistanceToGene:
    """Test the get_variant_distance_to_gene util."""

    test_input = [
        # A variant outside the threshold distance from the gene's TSS in the same chromosome
        [("1_3_X_X", "1", 3)],
        # A variant outside the threshold distance from the gene's TSS in a different chromosome
        [("2_3_X_X", "2", 3)],
        # 2 variants inside the threshold distance from the gene's TSS in the same chromosome
        [("1_8_X_X", "1", 8), ("1_13_X_X", "1", 13)],
        # A variant that is located where the gene's TSS is
        [("1_10_X_X", "1", 10)],
    ]

    expected_output = [
        [],
        [],
        [
            ("1_8_X_X", "ENSGXXXXX", 2, "1"),
            ("1_13_X_X", "ENSGXXXXX", 3, "1"),
        ],
        [("1_10_X_X", "ENSGXXXXX", 0, "1")],
    ]

    @pytest.fixture(scope="class")
    def mock_gene_df(
        self: TestGetVariantDistanceToGene, spark: SparkSession
    ) -> DataFrame:
        """Mock Dataframe for genes."""
        return spark.createDataFrame(
            [("ENSGXXXXX", 1, 10)], ["geneId", "chromosome", "tss"]
        )

    @pytest.fixture(scope="module")
    def expected_df_schema(self: TestGetVariantDistanceToGene) -> t.StructType:
        """Schema of the mock VEP dataframe."""
        return t.StructType(
            [
                t.StructField("variantId", t.StringType(), False),
                t.StructField("geneId", t.StringType(), False),
                t.StructField("distance", t.IntegerType(), True),
                t.StructField("chromosome", t.StringType(), True),
            ]
        )

    @pytest.mark.parametrize(
        "test_input, expected_output", zip(test_input, expected_output)
    )
    def test_get_variant_distance_to_gene(
        self: TestGetVariantDistanceToGene,
        spark: SparkSession,
        test_input: list[tuple],
        expected_output: list[tuple],
        expected_df_schema: t.StructType,
        mock_gene_df: DataFrame,
    ) -> None:
        """Test the get_variant_distance_to_gene util."""
        distance_window = 5
        mock_df = spark.createDataFrame(
            data=test_input, schema=["variantId", "chromosome", "position"]
        )
        test_df = mock_df.transform(
            lambda df: get_variant_distance_to_gene(mock_gene_df, df, distance_window)
        ).toPandas()
        expected_df = spark.createDataFrame(
            data=expected_output,
            schema=expected_df_schema,
        ).toPandas()

        assert_frame_equal(
            test_df,
            expected_df,
            check_like=True,
            check_dtype=False,
        )


# class TestScoreDistance:
#     """Test the score_distance util."""

#     test_input = [(0, 5, 50, 1000)]
#     expected_output = []
