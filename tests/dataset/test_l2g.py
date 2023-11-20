"""Tests on L2G datasets."""
from __future__ import annotations

from typing import TYPE_CHECKING

from otg.dataset.l2g_feature_matrix import L2GFeatureMatrix
from otg.dataset.l2g_gold_standard import L2GGoldStandard
from otg.dataset.l2g_prediction import L2GPrediction
from otg.dataset.study_locus_overlap import StudyLocusOverlap

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def test_feature_matrix(mock_l2g_feature_matrix: L2GFeatureMatrix) -> None:
    """Test L2G Feature Matrix creation with mock data."""
    assert isinstance(mock_l2g_feature_matrix, L2GFeatureMatrix)


def test_gold_standard(mock_l2g_gold_standard: L2GFeatureMatrix) -> None:
    """Test L2G gold standard creation with mock data."""
    assert isinstance(mock_l2g_gold_standard, L2GGoldStandard)


def test_process_gene_interactions(sample_otp_interactions: DataFrame) -> None:
    """Tests processing of gene interactions from OTP."""
    expected_cols = ["geneIdA", "geneIdB", "score"]
    observed_df = L2GGoldStandard.process_gene_interactions(sample_otp_interactions)
    assert (
        observed_df.columns == expected_cols
    ), "Gene interactions has a different schema."


def test_predictions(mock_l2g_predictions: L2GFeatureMatrix) -> None:
    """Test L2G predictions creation with mock data."""
    assert isinstance(mock_l2g_predictions, L2GPrediction)


def test_filter_unique_associations(spark: SparkSession) -> None:
    """Test filter_unique_associations."""
    mock_l2g_gs_df = spark.createDataFrame(
        [
            (1, "variant1", "gene1", "positive"),
            (
                2,
                "variant2",
                "gene1",
                "negative",
            ),  # in the same locus as sl1 and pointing to same gene, has to be dropped
            (
                3,
                "variant3",
                "gene1",
                "positive",
            ),  # in diff locus as sl1 and pointing to same gene, has to be kept
            (
                4,
                "variant4",
                "gene2",
                "positive",
            ),  # in same locus as sl1 and pointing to diff gene, has to be kept
        ],
        "studyLocusId LONG, variantId STRING, geneId STRING, goldStandardSet STRING",
    )

    mock_sl_overlap_df = spark.createDataFrame(
        [(1, 2, "variant2"), (1, 4, "variant4")],
        "leftStudyLocusId LONG, rightStudyLocusId LONG, tagVariantId STRING",
    )

    expected_df = spark.createDataFrame(
        [
            (1, "variant1", "gene1", "positive"),
            (3, "variant3", "gene1", "positive"),
            (4, "variant4", "gene2", "positive"),
        ],
        "studyLocusId LONG, variantId STRING, geneId STRING, goldStandardSet STRING",
    )

    mock_l2g_gs = L2GGoldStandard(
        _df=mock_l2g_gs_df, _schema=L2GGoldStandard.get_schema()
    )
    mock_sl_overlap = StudyLocusOverlap(
        _df=mock_sl_overlap_df, _schema=StudyLocusOverlap.get_schema()
    )._convert_to_square_matrix()

    observed_df = mock_l2g_gs.filter_unique_associations(mock_sl_overlap).df

    assert observed_df.collect() == expected_df.collect()
