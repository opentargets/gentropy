"""Tests on helper functions that find overlapping signals between credible sets."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.types as t
import pytest
from pandas.testing import assert_frame_equal

from etl.coloc.overlaps import find_gwas_vs_all_overlapping_peaks

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TestFindGwasVsAllOverlappingPeaks:
    """Tests on find_gwas_vs_all_overlapping_peaks."""

    test_input = [
        [
            # Overlapping peak between two GWAS
            ("22", "GCST90002383", "22_28449893_G_C", "22_28026789_C_T", "gwas"),
            ("22", "GCST90002310", "22_28690105_C_T", "22_28026789_C_T", "gwas"),
        ],
        [
            # Overlapping peak between a GWAS and a eQTL
            ("22", "GCST90002383", "22_28449893_G_C", "22_28026789_C_T", "gwas"),
            ("22", "eABC123", "22_28690105_C_T", "22_28026789_C_T", "eqtl"),
        ],
        [
            # Overlapping peak between a pQTL and a eQTL
            ("22", "pABC123", "22_28449893_G_C", "22_28026789_C_T", "pqtl"),
            ("22", "eABC123", "22_28690105_C_T", "22_28026789_C_T", "eqtl"),
        ],
    ]

    expected_output = [
        [
            # Overlapping peak between two GWAS
            (
                "22",
                "GCST90002383",
                "22_28449893_G_C",
                "gwas",
                "22",
                "GCST90002310",
                "22_28690105_C_T",
                "gwas",
            ),
        ],
        [
            # Overlapping peak between a GWAS and a eQTL
            (
                "22",
                "GCST90002383",
                "22_28449893_G_C",
                "gwas",
                "22",
                "eABC123",
                "22_28690105_C_T",
                "eqtl",
            ),
        ],
        [
            # Overlapping peak between a pQTL and a eQTL should not be returned
        ],
    ]

    @pytest.fixture(scope="class")
    def expected_output_schema(self: TestFindGwasVsAllOverlappingPeaks) -> t.StructType:
        """Schema of the mock overlap output dataframe."""
        return t.StructType(
            [
                t.StructField("left_chromosome", t.StringType(), False),
                t.StructField("left_studyId", t.StringType(), False),
                t.StructField("left_leadVariantId", t.StringType(), False),
                t.StructField("left_type", t.StringType(), True),
                t.StructField("right_chromosome", t.StringType(), True),
                t.StructField("right_studyId", t.StringType(), True),
                t.StructField("right_leadVariantId", t.StringType(), True),
                t.StructField("right_type", t.StringType(), True),
            ]
        )

    @pytest.mark.parametrize(
        "test_input, expected_output", zip(test_input, expected_output)
    )
    def test_find_gwas_vs_all_overlapping_peaks(
        self: TestFindGwasVsAllOverlappingPeaks,
        spark: SparkSession,
        test_input: list[tuple],
        expected_output: list[tuple],
        expected_output_schema: t.StructType,
    ) -> None:
        """Test that find_gwas_vs_all_overlapping_peaks returns the expected DataFrame."""
        mock_df = spark.createDataFrame(
            data=test_input,
            schema=[
                "chromosome",
                "studyId",
                "leadVariantId",
                "tagVariantId",
                "type",
            ],
        )
        test_df = find_gwas_vs_all_overlapping_peaks(mock_df).toPandas()
        expected_df = spark.createDataFrame(
            data=expected_output,
            schema=expected_output_schema,
        ).toPandas()

        assert_frame_equal(test_df, expected_df)
