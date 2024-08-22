"""Test Open Targets L2G gold standards data source."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyspark.sql import DataFrame

from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.v2g import V2G
from gentropy.datasource.open_targets.l2g_gold_standard import (
    OpenTargetsL2GGoldStandard,
)

if TYPE_CHECKING:
    from pyspark.sql.session import SparkSession


def test_open_targets_as_l2g_gold_standard(
    sample_l2g_gold_standard: DataFrame,
    mock_v2g: V2G,
) -> None:
    """Test L2G gold standard from OTG curation."""
    assert isinstance(
        OpenTargetsL2GGoldStandard.as_l2g_gold_standard(
            sample_l2g_gold_standard,
            mock_v2g,
        ),
        L2GGoldStandard,
    )


def test_parse_positive_curation(
    sample_l2g_gold_standard: DataFrame,
) -> None:
    """Test parsing curation as the positive set."""
    expected_cols = ["studyLocusId", "studyId", "variantId", "geneId", "sources"]
    df = OpenTargetsL2GGoldStandard.parse_positive_curation(sample_l2g_gold_standard)
    assert df.columns == expected_cols, "GS parsing has a different schema."


class TestExpandGoldStandardWithNegatives:
    """Test expanding positive set with negative set."""

    observed_df: DataFrame
    expected_expanded_gs: DataFrame
    sample_positive_set: DataFrame

    def test_expand_gold_standard_with_negatives_logic(
        self: TestExpandGoldStandardWithNegatives, spark: SparkSession
    ) -> None:
        """Test expanding positive set with negative set coincides with expected results."""
        assert (
            self.observed_df.collect() == self.expected_expanded_gs.collect()
        ), "GS expansion is not as expected."

    def test_expand_gold_standard_with_negatives_same_positives(
        self: TestExpandGoldStandardWithNegatives, spark: SparkSession
    ) -> None:
        """Test expanding positive set with negative set doesn't remove any positives."""
        assert (
            self.observed_df.filter("goldStandardSet == 'positive'").count()
            == self.sample_positive_set.count()
        ), "GS expansion has removed positives."

    @pytest.fixture(autouse=True)
    def _setup(self: TestExpandGoldStandardWithNegatives, spark: SparkSession) -> None:
        """Prepare fixtures for TestExpandGoldStandardWithNegatives."""
        self.sample_positive_set = spark.createDataFrame(
            [
                ("variant1", "gene1", "study1"),
                ("variant2", "gene2", "study1"),
            ],
            ["variantId", "geneId", "studyId"],
        )

        sample_v2g_df = spark.createDataFrame(
            [
                ("variant1", "gene1", 5, "X", "X", "X"),
                ("variant1", "gene3", 10, "X", "X", "X"),
            ],
            [
                "variantId",
                "geneId",
                "distance",
                "chromosome",
                "datatypeId",
                "datasourceId",
            ],
        )

        self.expected_expanded_gs = spark.createDataFrame(
            [
                ("variant1", "study1", "negative", "gene3"),
                ("variant1", "study1", "positive", "gene1"),
                ("variant2", "study1", "positive", "gene2"),
            ],
            ["variantId", "geneId", "goldStandardSet", "studyId"],
        )
        self.observed_df = (
            OpenTargetsL2GGoldStandard.expand_gold_standard_with_negatives(
                self.sample_positive_set,
                V2G(_df=sample_v2g_df, _schema=V2G.get_schema()),
            )
        )
