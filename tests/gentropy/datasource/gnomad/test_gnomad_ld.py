"""Tests on LD matrix from GnomAD."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row, SparkSession

from gentropy.datasource.gnomad.ld import GnomADLDMatrix


@pytest.mark.parametrize(
    ("observed", "expected"),
    [
        # Normal scenario: the ld index contains all variants in the matrix
        (
            # Observed
            [
                (0, "varA", "chr1"),
                (1, "varB", "chr1"),
                (2, "varC", "chr1"),
            ],
            # Expected
            [
                (1.0, "varA", "chr1", "varA"),
                (0.7, "varA", "chr1", "varB"),
                (-0.7, "varA", "chr1", "varC"),
            ],
        ),
        # LD index is missing a variant in the matrix
        (
            # Observed
            [
                (0, "varA", "chr1"),
                (1, "varB", "chr1"),
            ],
            # Expected - the missing variant is ignored
            [
                (1.0, "varA", "chr1", "varA"),
                (0.7, "varA", "chr1", "varB"),
            ],
        ),
    ],
)
def test_resolve_variant_indices(
    spark: SparkSession, observed: list[Any], expected: list[Any]
) -> None:
    """Test _resolve_variant_indices."""
    ld_matrix = spark.createDataFrame(
        [
            (0, 0, 1.0),
            (0, 1, 0.7),
            (0, 2, -0.7),
        ],
        ["i", "j", "r"],
    )
    ld_index = spark.createDataFrame(
        observed,
        ["idx", "variantId", "chromosome"],
    )
    expected_df = spark.createDataFrame(
        expected,
        ["r", "variantId_i", "chromosome", "variantId_j"],
    )
    observed_df = GnomADLDMatrix._resolve_variant_indices(ld_index, ld_matrix)
    assert (
        observed_df.orderBy(observed_df["r"].desc()).collect() == expected_df.collect()
    )


class TestGnomADLDMatrixGetLocusBoundaries:
    """Test GnomAD LD methods for locus boundaries."""

    @patch("gentropy.datasource.gnomad.ld.hl.read_table")
    @patch("gentropy.datasource.gnomad.ld.GnomADLDMatrix._filter_liftover_by_locus")
    def test_filter_liftover_integration(
        self, mock_filter_liftover: MagicMock, mock_read_table: MagicMock
    ) -> None:
        """Test that _filter_liftover_by_locus is properly integrated with get_locus_index_boundaries."""
        from gentropy.datasource.gnomad.ld import GnomADLDMatrix

        # Setup
        study_locus_row = Row(chromosome="1", locusStart=1000000, locusEnd=2000000)

        mock_liftover_ht = MagicMock()
        mock_liftover_ht.join = MagicMock()
        mock_filter_liftover.return_value = mock_liftover_ht
        mock_table = MagicMock()
        mock_read_table.return_value = mock_table

        ld_matrix = GnomADLDMatrix(
            ld_index_raw_template="test_path_{POP}",
            liftover_ht_path="test_liftover_path",
        )

        # Execute
        ld_matrix.get_locus_index_boundaries(
            study_locus_row, major_population="test_pop"
        )

        # Verify
        mock_read_table.assert_any_call("test_liftover_path")
        mock_read_table.assert_any_call("test_path_test_pop")
        assert mock_read_table.call_count == 2

        mock_filter_liftover.assert_called_once_with(
            mock_table, "chr1", 1000000, 2000000
        )
        mock_liftover_ht.join.assert_called_once_with(
            mock_read_table.return_value,
            how="inner",
        )
