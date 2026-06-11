"""Tests for LocusToGeneTrainTestSplitStep static helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pytest

from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_locus import StudyLocus
from gentropy.l2g import LocusToGeneTrainTestSplitStep

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from gentropy.common.session import Session


class TestWriteSplitStats:
    """Tests for LocusToGeneTrainTestSplitStep._write_split_stats."""

    def test_writes_json_next_to_train_path(self, tmp_path: Path) -> None:
        """Stats are serialised to <train_path>_split_stats.json."""
        stats = {"n_train": 80, "n_test": 20, "n_lost_total": 0}
        train_path = str(tmp_path / "train_data")
        LocusToGeneTrainTestSplitStep._write_split_stats(stats, train_path)
        json_path = tmp_path / "train_data_split_stats.json"
        assert json_path.exists()
        assert json.loads(json_path.read_text()) == stats

    def test_trailing_slash_is_stripped(self, tmp_path: Path) -> None:
        """A trailing slash on train_path does not create a double-underscore filename."""
        stats = {"n_train": 10}
        train_path = str(tmp_path / "train_data") + "/"
        LocusToGeneTrainTestSplitStep._write_split_stats(stats, train_path)
        json_path = tmp_path / "train_data_split_stats.json"
        assert json_path.exists()


class TestParseGoldStandard:
    """Tests for LocusToGeneTrainTestSplitStep._parse_gold_standard."""

    @pytest.fixture()
    def empty_credible_set(self, spark: SparkSession) -> StudyLocus:
        """Minimal StudyLocus (not used by non-OTG paths)."""
        return StudyLocus(
            _df=spark.createDataFrame([], StudyLocus.get_schema()),
            _schema=StudyLocus.get_schema(),
        )

    def test_exact_schema_returns_l2g_gold_standard(
        self,
        session: Session,
        spark: SparkSession,
        tmp_path: Path,
        mock_l2g_gold_standard: L2GGoldStandard,
        empty_credible_set: StudyLocus,
    ) -> None:
        """A parquet whose schema matches L2GGoldStandard is loaded directly."""
        gs_path = str(tmp_path / "gs.parquet")
        mock_l2g_gold_standard.df.write.parquet(gs_path)

        result = LocusToGeneTrainTestSplitStep._parse_gold_standard(
            session=session,
            gold_standard_curation_path=gs_path,
            credible_set=empty_credible_set,
            variant_index_path=None,
            gene_interactions_path=None,
        )
        assert isinstance(result, L2GGoldStandard)

    def test_extra_columns_are_dropped(
        self,
        session: Session,
        spark: SparkSession,
        tmp_path: Path,
        mock_l2g_gold_standard: L2GGoldStandard,
        empty_credible_set: StudyLocus,
    ) -> None:
        """Extra columns are silently dropped; result is still L2GGoldStandard."""
        gs_path = str(tmp_path / "gs_extra.parquet")
        mock_l2g_gold_standard.df.withColumn("extra_col", f.lit("x")).write.parquet(
            gs_path
        )

        result = LocusToGeneTrainTestSplitStep._parse_gold_standard(
            session=session,
            gold_standard_curation_path=gs_path,
            credible_set=empty_credible_set,
            variant_index_path=None,
            gene_interactions_path=None,
        )
        assert isinstance(result, L2GGoldStandard)
        assert "extra_col" not in result.df.columns

    def test_unrecognized_schema_raises_type_error(
        self,
        session: Session,
        spark: SparkSession,
        tmp_path: Path,
        empty_credible_set: StudyLocus,
    ) -> None:
        """A DataFrame with an unrecognized schema raises TypeError.

        Uses a schema that has mandatory L2GGoldStandard columns missing but no
        unexpected columns, so it reaches the fallthrough ``case _:`` branch.
        """
        bad_path = str(tmp_path / "bad.parquet")
        # Only studyLocusId is present — other mandatory columns are absent and
        # there are no unexpected columns, so no case branch matches → TypeError.
        spark.createDataFrame([("loc1",)], "studyLocusId STRING").write.parquet(
            bad_path
        )

        with pytest.raises(TypeError, match="Incorrect gold standard dataset"):
            LocusToGeneTrainTestSplitStep._parse_gold_standard(
                session=session,
                gold_standard_curation_path=bad_path,
                credible_set=empty_credible_set,
                variant_index_path=None,
                gene_interactions_path=None,
            )
