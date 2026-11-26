"""Tests on LD matrix from GnomAD."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from pyspark.sql import Row

from gentropy.datasource.gnomad.ld import GnomADLDMatrix


class TestGnomADLDMatrixGetLocusBoundaries:
    """Test GnomAD LD methods for locus boundaries."""

    @patch("gentropy.datasource.gnomad.ld.hl.read_table")
    @patch("gentropy.datasource.gnomad.ld.GnomADLDMatrix._filter_liftover_by_locus")
    def test_filter_liftover_integration(
        self, mock_filter_liftover: MagicMock, mock_read_table: MagicMock
    ) -> None:
        """Test that _filter_liftover_by_locus is properly integrated with get_locus_index_boundaries."""
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
