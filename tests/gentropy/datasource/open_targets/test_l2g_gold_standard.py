"""Test Open Targets L2G gold standards data source."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyspark.sql import DataFrame

from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.v2g import V2G
from gentropy.datasource.open_targets.l2g_gold_standard import (
    OpenTargetsL2GGoldStandard,
)
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader

if TYPE_CHECKING:
    from pyspark.sql.session import SparkSession

    from gentropy.dataset.colocalisation import Colocalisation
    from gentropy.dataset.study_locus import StudyLocus


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


def test_build_feature_matrix(
    mock_l2g_gold_standard: L2GGoldStandard,
    mock_study_locus: StudyLocus,
    mock_colocalisation: Colocalisation,
    mock_study_index: StudyIndex,
) -> None:
    """Test building feature matrix with the eQtlColocH4Maximum feature."""
    features_list = ["eQtlColocH4Maximum"]
    loader = L2GFeatureInputLoader(
        colocalisation=mock_colocalisation, study_index=mock_study_index
    )
    fm = mock_study_locus.build_feature_matrix(features_list, loader)
    assert isinstance(
        mock_l2g_gold_standard.build_feature_matrix(fm), L2GFeatureMatrix
    ), "Feature matrix should be of type L2GFeatureMatrix"
