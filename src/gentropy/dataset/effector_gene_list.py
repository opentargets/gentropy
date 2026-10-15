"""Effector Gene List dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class EffectorGeneList(Dataset):
    """Effector Gene List (EGL): trustworthy gene-disease pairs used to label the L2G training set.

    Each row is a distinct ``(diseaseId, targetId)`` pair, built by ``EffectorGeneListStep``
    from rare-variant evidence, clinical precedence and the legacy OTG gold standard.
    """

    @classmethod
    def get_schema(cls: type[EffectorGeneList]) -> StructType:
        """Provides the schema for the EffectorGeneList dataset.

        Returns:
            StructType: Schema for the EffectorGeneList dataset.
        """
        return parse_spark_schema("effector_gene_list.json")
