"""Tests on LD matrix from GnomAD."""

from __future__ import annotations

from typing import Any

import hail as hl
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from otg.datasource.gnomad.ld import GnomADLDMatrix


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


class TestGnomADLDMatrix:
    """Test GnomAD LD methods."""

    gnomad_ld_matrix: GnomADLDMatrix
    ld_population: str = "test-pop"
    slice_start_index: int = 1
    slice_end_index: int = 2

    @pytest.fixture(autouse=True, scope="class")
    def _setup(self: TestGnomADLDMatrix, spark: SparkSession) -> None:
        """Initialize hail for the tests below."""
        hl.init(sc=spark.sparkContext, log="/dev/null")
        self.gnomad_ld_matrix = GnomADLDMatrix(
            ld_matrix_template="tests/data_samples/example_{POP}.bm"
        )

    @pytest.fixture(scope="class")
    def matrix_slice(self) -> DataFrame:
        """Return a slice from the LD matrix.

        Returns:
            DataFrame: Melted LD matrix
        """
        return self.gnomad_ld_matrix.get_ld_matrix_slice(
            gnomad_ancestry=self.ld_population,
            start_index=self.slice_start_index,
            end_index=self.slice_end_index,
        ).persist()

    def test_get_ld_matrix_slice__diagonal(
        self: TestGnomADLDMatrix, matrix_slice: DataFrame
    ) -> None:
        """Test LD matrix slice."""
        # Has the returned data ones in the diagonal?
        assert (
            matrix_slice.filter(f.col("idx_i") == f.col("idx_j"))
            .select("r")
            .distinct()
            .collect()[0]["r"]
            == 1.0
        )

    def test_get_ld_matrix_slice__count(
        self: TestGnomADLDMatrix, matrix_slice: DataFrame
    ) -> None:
        """Test LD matrix slice."""
        # As the slicing of the matrix is inclusive, the total number of rows are calculated as follows:
        included_indices = self.slice_end_index - self.slice_start_index + 1
        expected_pariwise_count = included_indices**2

        # Is the returned data has the right number of rows?
        assert matrix_slice.count() == expected_pariwise_count

    def test_get_ld_matrix_slice__type(
        self: TestGnomADLDMatrix, matrix_slice: DataFrame
    ) -> None:
        """Test LD matrix slice."""
        # Is the returned data is a dataframe?
        assert isinstance(matrix_slice, DataFrame)

    def test_get_ld_matrix_slice__symmetry(
        self: TestGnomADLDMatrix, matrix_slice: DataFrame
    ) -> None:
        """Test LD matrix slice."""
        # Testing square matrix completeness and symmetry:
        compared = matrix_slice.join(
            (
                matrix_slice.select(
                    f.col("idx_i").alias("idx_j"),
                    f.col("idx_j").alias("idx_i"),
                    f.col("r").alias("r_sym"),
                )
            ),
            on=["idx_i", "idx_j"],
            how="inner",
        )

        # Is the matrix complete:
        assert compared.count() == matrix_slice.count()

        # Is the matrix symmetric:
        assert compared.filter(f.col("r") == f.col("r_sym")).count() == compared.count()
