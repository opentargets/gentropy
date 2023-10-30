"""L2G gold standard dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.common.schemas import parse_spark_schema
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.dataset.study_locus_overlap import StudyLocusOverlap
    from otg.dataset.v2g import V2G


@dataclass
class L2GGoldStandard(Dataset):
    """L2G gold standard dataset."""

    @classmethod
    def from_otg_curation(
        cls: type[L2GGoldStandard],
        gold_standard_curation: DataFrame,
        v2g: V2G,
        study_locus_overlap: StudyLocusOverlap,
        interactions: DataFrame,
    ) -> L2GGoldStandard:
        """Initialise L2GGoldStandard from source dataset."""
        from otg.datasource.open_targets.l2g_gold_standard import (
            OpenTargetsL2GGoldStandard,
        )

        return OpenTargetsL2GGoldStandard.as_l2g_gold_standard(
            gold_standard_curation, v2g, study_locus_overlap, interactions
        )

    @classmethod
    def get_schema(cls: type[L2GGoldStandard]) -> StructType:
        """Provides the schema for the L2GGoldStandard dataset."""
        return parse_spark_schema("l2g_gold_standard.json")
