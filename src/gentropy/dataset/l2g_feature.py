"""L2G Feature Dataset."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from gentropy.dataset.study_locus import StudyLocus


@dataclass
class L2GFeature(Dataset, ABC):
    """Locus-to-gene feature dataset."""

    def __init__(
        self: L2GFeature,
        input_dependency: Any = None,
        credible_set: StudyLocus | None = None,
    ) -> None:
        """Initializes a L2GFeature dataset.

        Args:
            input_dependency (Any): The dependency that the L2GFeature dataset depends on. Defaults to None.
            credible_set (StudyLocus | None): The credible set that the L2GFeature dataset is based on. Defaults to None.
        """
        super().__init__()
        self.input_dependency = input_dependency
        self.credible_set = credible_set

    @classmethod
    def get_schema(cls: type[L2GFeature]) -> StructType:
        """Provides the schema for the L2GFeature dataset.

        Returns:
            StructType: Schema for the L2GFeature dataset
        """
        return parse_spark_schema("l2g_feature.json")

    @classmethod
    @abstractmethod
    def compute(cls: type[L2GFeature]) -> L2GFeature:
        """Computes the L2GFeature dataset.

        Returns:
            L2GFeature: a L2GFeature dataset
        """
        pass
