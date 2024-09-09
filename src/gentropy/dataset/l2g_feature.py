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

    def __post_init__(
        self: L2GFeature,
        feature_dependency: Any = None,
        credible_set: StudyLocus | None = None,
    ) -> None:
        """Initializes a L2GFeature dataset.

        Args:
            feature_dependency (Any): The dependency that the L2GFeature dataset depends on. Defaults to None.
            credible_set (StudyLocus | None): The credible set that the L2GFeature dataset is based on. Defaults to None.
        """
        super().__post_init__()
        self.feature_dependency = feature_dependency
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
    def compute(
        cls: type[L2GFeature], credible_set: StudyLocus, feature_dependency: Any
    ) -> L2GFeature:
        """Computes the L2GFeature dataset.

        Args:
            credible_set (StudyLocus): The credible set that will be used for annotation
            feature_dependency (Any): The dependency that the L2GFeature class needs to compute the feature
        Returns:
            L2GFeature: a L2GFeature dataset
        """
        pass
