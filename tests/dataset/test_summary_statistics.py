"""Test study index dataset."""
from __future__ import annotations

from otg.dataset.study_locus import StudyLocus
from otg.dataset.summary_statistics import SummaryStatistics


def test_summary_statistics__creation(
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test gene index creation with mock gene index."""
    assert isinstance(mock_summary_statistics, SummaryStatistics)


def test_summary_statistics__pval_filter__return_type(
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test if the p-value filter indeed returns summary statistics object."""
    pval_threshold = 5e-3
    assert isinstance(
        mock_summary_statistics.pvalue_filter(pval_threshold), SummaryStatistics
    )


def test_summary_statistics__window_based_clumping__return_type(
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test if the window-based clumping indeed returns study locus object."""
    assert isinstance(
        mock_summary_statistics.window_based_clumping(250_000), StudyLocus
    )


def test_summary_statistics__from_ukbiobank_summary_stats(
    ukbb_mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test if UKBiobank summary statistics are correctly ingested."""
    ukbiobank_study_id = "NEALE2_100001_raw"
    result = SummaryStatistics.from_ukbiobank_summary_stats(
        ukbb_mock_summary_statistics, ukbiobank_study_id
    )

    # Check if the variantId column is created correctly
    expected_variant_ids = [
        "1_758351_A_G",
        "1_909894_G_T",
        "1_933024_C_T",
        "1_973673_G_A",
        "1_1037956_A_G",
        "1_1055604_G_A",
        "1_1065477_G_A",
        "1_1065797_G_C",
        "1_1148447_T_C",
        "1_1156196_A_C",
    ]

    assert (
        result.df.filter(result.df.variantId.isin(expected_variant_ids)).count() == 10
    )
