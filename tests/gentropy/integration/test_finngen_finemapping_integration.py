"""Integration test of the FinnGen fine-mapping ingestion step's orchestration."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gentropy import Session
from gentropy.finngen_finemapping_ingestion import FinnGenFinemappingIngestionStep


class TestFinnGenFinemappingIngestionStepIntegration:
    """Test that FinnGenFinemappingIngestionStep wires the fine-mapping parsing and both writes.

    The parsing into credible sets and their variants is mocked out; its logic is covered by the
    FinnGen datasource tests. Only the step's own orchestration is verified: the release prefix
    and every input handed to the parser, and both outputs written to disk.
    """

    @pytest.mark.step_test
    @patch(
        "gentropy.finngen_finemapping_ingestion.FinnGenFinemapping.from_finngen_susie_finemapping"
    )
    def test_orchestration(
        self,
        mock_from_finngen_susie_finemapping: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """Every input reaches the parser, and credible sets and variants go to their own paths."""
        credible_set_path = tmp_path / "credible_set"
        credible_set_variant_path = tmp_path / "credible_set_variant"
        credible_sets = MagicMock(name="credible_sets")
        credible_sets.df = session.spark.createDataFrame(
            [("cs1", "6_2912516_CCA_C")], "id string, leadVariantId string"
        )
        credible_set_variants = MagicMock(name="credible_set_variants")
        credible_set_variants.df = session.spark.createDataFrame(
            [("cs1", "6_2912516_CCA_C"), ("cs1", "6_2898066_G_T")],
            "credibleSetId string, variantId string",
        )
        mock_from_finngen_susie_finemapping.return_value = (
            credible_sets,
            credible_set_variants,
        )

        FinnGenFinemappingIngestionStep(
            session=session,
            finngen_annotated_variants_path="gs://bucket/annotations/finngen_R12_annotated_variants.gz",
            finngen_finemapping_out=str(credible_set_path),
            finngen_finemapping_credible_set_variant_out=str(credible_set_variant_path),
            finngen_susie_finemapping_snp_files="gs://bucket/finemap/full/susie/*.snp.bgz",
            finngen_susie_finemapping_cs_summary_files="gs://bucket/finemap/summary/*SUSIE.cred.summary.tsv",
            finngen_finemapping_lead_pvalue_threshold=1e-5,
            # The trailing underscore is removed before the prefix is used:
            finngen_release_prefix="FINNGEN_R12_",
        )

        mock_from_finngen_susie_finemapping.assert_called_once_with(
            spark=session.spark,
            finngen_susie_finemapping_snp_files="gs://bucket/finemap/full/susie/*.snp.bgz",
            finngen_susie_finemapping_cs_summary_files="gs://bucket/finemap/summary/*SUSIE.cred.summary.tsv",
            finngen_annotated_variants_path="gs://bucket/annotations/finngen_R12_annotated_variants.gz",
            finngen_release_prefix="FINNGEN_R12",
            lead_pvalue_threshold=1e-5,
        )
        written_credible_sets = session.spark.read.parquet(str(credible_set_path))
        written_variants = session.spark.read.parquet(str(credible_set_variant_path))
        assert [row.id for row in written_credible_sets.collect()] == ["cs1"]
        assert sorted(row.variantId for row in written_variants.collect()) == [
            "6_2898066_G_T",
            "6_2912516_CCA_C",
        ]

    @pytest.mark.step_test
    @patch(
        "gentropy.finngen_finemapping_ingestion.FinnGenFinemapping.from_finngen_susie_finemapping"
    )
    def test_invalid_release_prefix_is_rejected(
        self,
        mock_from_finngen_susie_finemapping: MagicMock,
        session: Session,
        tmp_path: Path,
    ) -> None:
        """A release prefix not shaped like FINNGEN_R* stops the step before any parsing."""
        with pytest.raises(ValueError, match="Invalid FinnGen release prefix"):
            FinnGenFinemappingIngestionStep(
                session=session,
                finngen_annotated_variants_path="gs://bucket/annotations.gz",
                finngen_finemapping_out=str(tmp_path / "credible_set"),
                finngen_finemapping_credible_set_variant_out=str(
                    tmp_path / "credible_set_variant"
                ),
                finngen_release_prefix="R12",
            )
        mock_from_finngen_susie_finemapping.assert_not_called()
