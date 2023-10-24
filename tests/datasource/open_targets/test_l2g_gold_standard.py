"""Test Open Targets L2G gold standards data source."""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from otg.dataset.l2g.gold_standard import L2GGoldStandard
from otg.datasource.open_targets.l2g_gold_standard import OpenTargetsL2GGoldStandard

if TYPE_CHECKING:
    from otg.dataset.study_locus_overlap import StudyLocusOverlap
    from otg.dataset.v2g import V2G


def test_open_targets_as_l2g_gold_standard(
    sample_l2g_gold_standard: DataFrame,
    mock_v2g: V2G,
    mock_study_locus_overlap: StudyLocusOverlap,
    sample_otp_interactions: DataFrame,
) -> None:
    """Test L2G gold standard from OTG curation."""
    assert isinstance(
        OpenTargetsL2GGoldStandard.as_l2g_gold_standard(
            sample_l2g_gold_standard,
            mock_v2g,
            mock_study_locus_overlap,
            sample_otp_interactions,
        ),
        L2GGoldStandard,
    )
