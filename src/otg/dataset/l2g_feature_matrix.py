"""Feature matrix of study locus pairs annotated with their functional genomics features."""
from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import TYPE_CHECKING, Type

from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import _convert_from_long_to_wide
from otg.dataset.dataset import Dataset
from otg.method.l2g.feature_factory import StudyLocusFactory

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    # from otg.dataset.colocalisation import Colocalisation
    from otg.dataset.study_index import StudyIndex
    from otg.dataset.study_locus import StudyLocus
    from otg.dataset.v2g import V2G


@dataclass
class L2GFeatureMatrix(Dataset):
    """Dataset with features for Locus to Gene prediction."""

    @classmethod
    def generate_features(
        cls: Type[L2GFeatureMatrix],
        study_locus: StudyLocus,
        study_index: StudyIndex,
        variant_gene: V2G,
        # colocalisation: Colocalisation,
    ) -> L2GFeatureMatrix:
        """Generate features from the OTG datasets."""
        if features_dfs := [
            # Extract features
            # ColocalisationFactory._get_coloc_features(
            #     study_locus, study_index, colocalisation
            # ).df,
            StudyLocusFactory._get_tss_distance_features(study_locus, variant_gene).df,
        ]:
            fm = reduce(
                lambda x, y: x.unionByName(y),
                features_dfs,
            )
        else:
            raise ValueError("No features found")

        # raise error if the feature matrix is empty
        if fm.limit(1).count() != 0:
            return cls(
                _df=_convert_from_long_to_wide(
                    fm, ["studyLocusId", "geneId"], "featureName", "featureValue"
                ),
                _schema=cls.get_schema(),
            )
        raise ValueError("L2G Feature matrix is empty")

    @classmethod
    def get_schema(cls: type[L2GFeatureMatrix]) -> StructType:
        """Provides the schema for the L2gFeatureMatrix dataset."""
        return parse_spark_schema("l2g_feature_matrix.json")

    def fill_na(
        self: L2GFeatureMatrix, value: float = 0.0, subset: list[str] | None = None
    ) -> L2GFeatureMatrix:
        """Fill missing values in a column with a given value."""
        self.df = self._df.fillna(value, subset=subset)
        return self

    def select_features(
        self: L2GFeatureMatrix, features_list: list[str]
    ) -> L2GFeatureMatrix:
        """Select a subset of features from the feature matrix."""
        fixed_rows = ["studyLocusId", "geneId", "goldStandardSet"]
        self.df = self._df.select(fixed_rows + features_list)
        return self

    def train_test_split(
        self: L2GFeatureMatrix, fraction: float
    ) -> tuple[L2GFeatureMatrix, L2GFeatureMatrix]:
        """Split the dataset into training and test sets.

        Args:
            fraction (float): Fraction of the dataset to use for training

        Returns:
            tuple[L2GFeatureMatrix, L2GFeatureMatrix]: Training and test datasets
        """
        train, test = self._df.randomSplit([fraction, 1 - fraction], seed=42)
        return (
            L2GFeatureMatrix(
                _df=train, _schema=L2GFeatureMatrix.get_schema()
            ).persist(),
            L2GFeatureMatrix(_df=test, _schema=L2GFeatureMatrix.get_schema()).persist(),
        )
