"""Test LD annotation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pytest

from otg.method.ld import LDAnnotatorGnomad

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from otg.dataset.ld_index import LDIndex
    from otg.dataset.study_index import StudyIndexGWASCatalog
    from otg.dataset.study_locus import StudyLocus
    from otg.dataset.variant_annotation import VariantAnnotation


def test_variants_in_ld_in_gnomad_pop(
    mock_variant_annotation: VariantAnnotation, mock_ld_index: LDIndex
) -> None:
    """Test function that annotates which variants are in LD in a particular gnomad population."""
    variants_w_ld_info = mock_variant_annotation.df.select(
        "chromosome",
        "variantId",
        "position",
        f.lit("afr").alias("gnomadPopulation"),
        f.rand().alias("r"),
        f.rand().alias("j"),
    )
    expanded_variants_df = LDAnnotatorGnomad.variants_in_ld_in_gnomad_pop(
        variants_w_ld_info, mock_ld_index
    )
    expected_cols = ["variantId", "chromosome", "gnomadPopulation", "tagVariantId", "r"]
    assert set(expanded_variants_df.columns) == set(expected_cols)


class TestVariantCoordinatesInLdIndex:
    """Test function that finds the indices of a particular set of variants in a LDIndex to query it afterwards."""

    def test_schema(self: TestVariantCoordinatesInLdIndex) -> None:
        """Test function that checks the schema of the output of `variant_coordinates_in_ldindex`."""
        expected_cols = [
            "variantId",
            "chromosome",
            "gnomadPopulation",
            "idx",
            "start_idx",
            "stop_idx",
            "i",
        ]
        assert set(self.variants_w_indices_df.columns) == set(expected_cols)

    def test_idx_order(self: TestVariantCoordinatesInLdIndex) -> None:
        """Test function that checks that the indices are ordered."""
        idx_list = (
            self.variants_w_indices_df.select("idx").rdd.flatMap(lambda x: x).collect()
        )
        assert idx_list == sorted(idx_list)

    def test_variants_ld_info(
        self: TestVariantCoordinatesInLdIndex, spark: SparkSession
    ) -> None:
        """Tests that the joining function to annotate coordinates and LD scores for a set of variants works."""
        # scores based on the output of _query_block_matrix
        variants_ld_scores = spark.createDataFrame(
            [(0, 0, 0.8), (0, 1, 1.0), (1, 2, 1.0)],
            ["i", "j", "r"],
        )

        # coordinate based on the output of _variant_coordinates_in_ldindex
        variants_ld_indices = self.variants_w_indices_df

        variants_ld_info = variants_ld_scores.join(
            f.broadcast(variants_ld_indices),
            on="i",
            how="inner",
        )
        expected_cols = [
            "variantId",
            "chromosome",
            "gnomadPopulation",
            "j",
            "i",
            "r",
            "idx",
            "start_idx",
            "stop_idx",
        ]
        assert set(variants_ld_info.columns) == set(expected_cols)

    @pytest.fixture(autouse=True)
    def _setup(
        self: TestVariantCoordinatesInLdIndex,
        mock_study_locus: StudyLocus,
        mock_study_index_gwas_catalog: StudyIndexGWASCatalog,
        mock_ld_index: LDIndex,
    ) -> None:
        """Prepares the data for the tests."""
        # variants_df is the result of extracting the unique locus/ancestry pairs
        self.variants_df = mock_study_locus.unique_study_locus_ancestries(
            studies=mock_study_index_gwas_catalog
        )
        self.variants_w_indices_df = LDAnnotatorGnomad._variant_coordinates_in_ldindex(
            self.variants_df, mock_ld_index
        )
