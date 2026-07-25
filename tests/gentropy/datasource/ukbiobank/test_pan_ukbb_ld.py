"""Test suite for the PanUKBBLDMatrix class in the gentropy package."""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pyspark.sql.functions as f
import pytest
from numpy._typing._array_like import NDArray
from pyspark.sql import DataFrame, SparkSession

from gentropy.datasource.pan_ukbb_ld.ld import PanUKBBLDMatrix


@pytest.fixture
def mock_locus_index() -> MagicMock:
    """Create a mock locus index DataFrame for testing."""
    mock_df: MagicMock = MagicMock(spec=DataFrame)

    # Configure the mock DataFrame's select and collect methods
    mock_select: MagicMock = MagicMock(spec=DataFrame)
    mock_df.select.return_value = mock_select

    # Mock rows with "idx" and "alleleOrder" columns
    mock_rows = [
        MagicMock(asDict=lambda: {"idx": 1, "alleleOrder": 1}),
        MagicMock(asDict=lambda: {"idx": 2, "alleleOrder": -1}),
        MagicMock(asDict=lambda: {"idx": 3, "alleleOrder": 1}),
    ]

    # Configure collect to return mock rows
    mock_select.collect.return_value = mock_rows

    return mock_df


@pytest.fixture
def half_matrix() -> NDArray[Any]:
    """Create a test half matrix."""
    return np.array([[1.0, 0.7, 0.3], [0.0, 1.0, 0.5], [0.0, 0.0, 1.0]])


class TestGetNumpyMatrix:
    """Test suite for the get_numpy_matrix method of PanUKBBLDMatrix."""

    def test_load_hail_block_matrix(self) -> None:
        """Test _load_hail_block_matrix correctly reads and filters block matrices."""
        # Create a mock BlockMatrix
        mock_block_matrix: MagicMock = MagicMock()
        mock_filtered: MagicMock = MagicMock()
        mock_numpy: NDArray[Any] = np.array([[1.0, 0.5], [0.5, 1.0]])

        # Configure mocks
        mock_block_matrix.filter.return_value = mock_filtered
        mock_filtered.to_numpy.return_value = mock_numpy

        # Patch BlockMatrix.read to return our mock
        with patch(
            "gentropy.datasource.pan_ukbb_ld.ld.BlockMatrix.read",
            return_value=mock_block_matrix,
        ) as mock_read:
            matrix: PanUKBBLDMatrix = PanUKBBLDMatrix(
                pan_ukbb_bm_path="test_path_{POP}"
            )
            result: NDArray[Any] = matrix._load_hail_block_matrix([1, 2], "EUR")

            mock_read.assert_called_once_with("test_path_EUR")

            # Verify filter was called with correct indices
            mock_block_matrix.filter.assert_called_once_with([1, 2], [1, 2])

            # Verify to_numpy was called and result returned
            mock_filtered.to_numpy.assert_called_once()
            assert result is mock_numpy

    def test_get_outer_allele_order(self, mock_locus_index: MagicMock) -> None:
        """Test _get_outer_allele_order correctly computes outer product of allele orders."""
        matrix: PanUKBBLDMatrix = PanUKBBLDMatrix()

        # Create a proper mock for the select result
        mock_allele_order_select: MagicMock = MagicMock()
        mock_locus_index.select.return_value = mock_allele_order_select

        # Create simple dictionaries that will work with row["alleleOrder"]
        mock_rows = [
            {"alleleOrder": 1},
            {"alleleOrder": -1},
            {"alleleOrder": 1},
        ]

        # Configure collect to return our mock rows
        mock_allele_order_select.collect.return_value = mock_rows

        result: NDArray[Any] = matrix._get_outer_allele_order(mock_locus_index)

        # Verify select was called with "alleleOrder"
        mock_locus_index.select.assert_called_once_with("alleleOrder")

        # Expected outer product of [1, -1, 1]
        expected: NDArray[Any] = np.array([[1, -1, 1], [-1, 1, -1], [1, -1, 1]])

        assert np.array_equal(result, expected)

    def test_construct_ld_matrix(self, half_matrix: NDArray[Any]) -> None:
        """Test _construct_ld_matrix correctly builds a symmetric matrix and applies allele order."""
        matrix: PanUKBBLDMatrix = PanUKBBLDMatrix()

        # Create test outer_allele_order
        outer_allele_order: NDArray[Any] = np.array(
            [[1, -1, 1], [-1, 1, -1], [1, -1, 1]]
        )

        result: NDArray[Any] = matrix._construct_ld_matrix(
            half_matrix, outer_allele_order
        )

        # Expected after applying allele order
        expected_final: NDArray[Any] = np.array(
            [[1.0, -0.7, 0.3], [-0.7, 1.0, -0.5], [0.3, -0.5, 1.0]]
        )

        # Verify diagonal is 1.0
        assert np.all(np.diag(result) == 1.0)

        # Verify result matches expected
        assert np.allclose(result, expected_final)

    def test_get_numpy_matrix_integrates_methods(
        self, mock_locus_index: MagicMock
    ) -> None:
        """Test get_numpy_matrix correctly integrates the other methods."""
        matrix: PanUKBBLDMatrix = PanUKBBLDMatrix()

        # Set up mock return values
        mock_half_matrix: NDArray[Any] = np.array(
            [[1.0, 0.7, 0.3], [0.0, 1.0, 0.5], [0.0, 0.0, 1.0]]
        )

        mock_outer_allele_order: NDArray[Any] = np.array(
            [[1, -1, 1], [-1, 1, -1], [1, -1, 1]]
        )

        mock_final_matrix: NDArray[Any] = np.array(
            [[1.0, -0.7, 0.3], [-0.7, 1.0, -0.5], [0.3, -0.5, 1.0]]
        )

        # Mock the methods
        with (
            patch.object(
                matrix, "_load_hail_block_matrix", return_value=mock_half_matrix
            ) as mock_load,
            patch.object(
                matrix, "_get_outer_allele_order", return_value=mock_outer_allele_order
            ) as mock_get_order,
            patch.object(
                matrix, "_construct_ld_matrix", return_value=mock_final_matrix
            ) as mock_construct,
        ):
            # Create a mock for the select result with proper row objects
            mock_idx_select: MagicMock = MagicMock()
            mock_locus_index.select.return_value = mock_idx_select

            # Create proper row objects with idx values that will be extracted
            mock_rows = [{"idx": 1}, {"idx": 2}, {"idx": 3}]

            # Mock the collect method to return these rows
            mock_idx_select.collect.return_value = mock_rows

            # Call the method
            result: NDArray[Any] = matrix.get_numpy_matrix(mock_locus_index, "EUR")

            # Verify each method was called with correct arguments
            mock_locus_index.select.assert_called_with("idx")
            mock_load.assert_called_once_with([1, 2, 3], "EUR")
            mock_get_order.assert_called_once_with(mock_locus_index)
            mock_construct.assert_called_once_with(
                mock_half_matrix, mock_outer_allele_order
            )

            # Verify result is what was returned from _construct_ld_matrix
            assert result is mock_final_matrix


class TestPreparePanUKBBReference:
    """Test PanUKBB LD reference preparation helpers."""

    def test_prepare_aligned_ld_index_adds_population_and_allele_orientation(
        self, spark: SparkSession
    ) -> None:
        """Prepared PanUKBB index contains signed allele orientation and population."""
        raw_index = spark.createDataFrame(
            [
                ("chr1", 100, ["A", "C"], 2),
                ("chr1", 200, ["G", "T"], 1),
                ("chr2", 300, ["C", "A"], 3),
            ],
            ["contig", "position", "alleles", "idx"],
        ).select(
            f.col("contig").alias("locus.contig"),
            f.col("position").alias("locus.position"),
            "alleles",
            "idx",
        )
        variant_annotation = spark.createDataFrame(
            [
                ("1", 100, "A", "C"),
                ("1", 200, "T", "G"),
            ],
            ["chromosome", "position", "referenceAllele", "alternateAllele"],
        )

        observed = PanUKBBLDMatrix.prepare_aligned_ld_index(
            raw_index=raw_index,
            variant_annotation=variant_annotation,
            population="EUR",
        )

        assert observed.columns == [
            "variantId",
            "chromosome",
            "position",
            "referenceAllele",
            "alternateAllele",
            "idx",
            "population",
            "alleleOrder",
        ]
        assert observed.orderBy("idx").collect() == [
            ("1_200_T_G", "1", 200, "T", "G", 1, "EUR", -1),
            ("1_100_A_C", "1", 100, "A", "C", 2, "EUR", 1),
            ("2_300_C_A", "2", 300, "C", "A", 3, "EUR", 1),
        ]

    def test_filter_ld_index_to_variants_uses_variant_id_and_chromosome(
        self, spark: SparkSession
    ) -> None:
        """Filtered indexes keep only variants from the requested test variant set."""
        ld_index = spark.createDataFrame(
            [
                ("1_100_A_C", "1", 100, "A", "C", 2, "EUR", 1),
                ("1_200_T_G", "1", 200, "T", "G", 1, "EUR", -1),
                ("2_300_C_A", "2", 300, "C", "A", 3, "EUR", 1),
            ],
            [
                "variantId",
                "chromosome",
                "position",
                "referenceAllele",
                "alternateAllele",
                "idx",
                "population",
                "alleleOrder",
            ],
        )
        requested_variants = spark.createDataFrame(
            [
                ("1_100_A_C", "1"),
                ("2_300_C_A", "1"),
            ],
            ["variantId", "chromosome"],
        )

        observed = PanUKBBLDMatrix.filter_ld_index_to_variants(
            ld_index, requested_variants
        )

        assert observed.select("variantId", "chromosome", "idx").collect() == [
            ("1_100_A_C", "1", 2)
        ]

    def test_write_filtered_block_matrix_filters_indices_before_write(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        """Bounded test BlockMatrices are written from filtered indices only."""
        locus_index = spark.createDataFrame(
            [
                ("1_200_T_G", 7),
                ("1_100_A_C", 3),
            ],
            ["variantId", "idx"],
        )
        mock_block_matrix: MagicMock = MagicMock()
        mock_filtered_matrix: MagicMock = MagicMock()
        mock_block_matrix.filter.return_value = mock_filtered_matrix
        output_path = str(tmp_path / "UKBB.EUR.test.ldadj")

        with patch(
            "gentropy.datasource.pan_ukbb_ld.ld.BlockMatrix.read",
            return_value=mock_block_matrix,
        ) as mock_read:
            PanUKBBLDMatrix(
                pan_ukbb_bm_path="gs://panukbb/UKBB.{POP}.ldadj"
            ).write_filtered_block_matrix(
                locus_index=locus_index,
                ancestry="EUR",
                output_path=output_path,
            )

        mock_read.assert_called_once_with("gs://panukbb/UKBB.EUR.ldadj")
        mock_block_matrix.filter.assert_called_once_with([3, 7], [3, 7])
        mock_filtered_matrix.write.assert_called_once_with(output_path, overwrite=True)
