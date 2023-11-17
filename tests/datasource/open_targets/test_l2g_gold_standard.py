"""Test Open Targets L2G gold standards data source."""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from otg.dataset.l2g_gold_standard import L2GGoldStandard
from otg.datasource.open_targets.l2g_gold_standard import OpenTargetsL2GGoldStandard

if TYPE_CHECKING:
    from otg.dataset.v2g import V2G


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
