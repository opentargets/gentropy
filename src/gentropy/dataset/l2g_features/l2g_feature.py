"""L2G Feature Dataset with a collection of methods that extract features from the gentropy datasets to be fed in L2G."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
    from gentropy.dataset.study_locus import StudyLocus


FeatureDependencyType = type[Any] | Sequence[type[Any]]


@dataclass
class L2GFeature(Dataset, ABC):
    """Locus-to-gene feature dataset that serves as template to generate each of the features that inform about locus to gene assignments."""

    feature_dependency_type: ClassVar[FeatureDependencyType | None] = None

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
        cls: type[L2GFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: Any,
    ) -> L2GFeature:
        """Computes the L2GFeature dataset.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (Any): The dependency that the L2GFeature class needs to compute the feature
        Returns:
            L2GFeature: a L2GFeature dataset

        Raises:
                NotImplementedError: This method must be implemented in the child classes
        """
        raise NotImplementedError("Must be implemented in the child classes")
