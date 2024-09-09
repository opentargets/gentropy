"""Feature matrix of study locus pairs annotated with their functional genomics features."""

from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING, Type

from gentropy.common.spark_helpers import convert_from_long_to_wide
from gentropy.method.l2g.feature_factory import FeatureFactory, L2GFeatureInputLoader

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from gentropy.common.session import Session


class L2GFeatureMatrix:
    """Dataset with features for Locus to Gene prediction."""

    def __init__(
        self,
        _df: DataFrame,
        features_list: list[str] | None = None,
        mode: str = "train",
    ) -> None:
        """Post-initialisation to set the features list. If not provided, all columns except the fixed ones are used.

        Args:
            _df (DataFrame): Feature matrix dataset
            features_list (list[str] | None): List of features to use. If None, all possible features are used.
            mode (str): Mode of the feature matrix. Defaults to "train". Can be either "train" or "predict".

        Raises:
            ValueError: If the mode is neither 'train' nor 'predict'.
        """
        if mode not in ["train", "predict"]:
            raise ValueError("Mode should be either 'train' or 'predict'")

        self.fixed_cols = ["studyLocusId", "geneId"]
        if mode == "train":
            self.fixed_cols.append("goldStandardSet")

        self.features_list = features_list or [
            col for col in _df.columns if col not in self.fixed_cols
        ]
        self._df = _df.selectExpr(
            self.fixed_cols
            + [
                f"CAST({feature} AS FLOAT) AS {feature}"
                for feature in self.features_list
            ]
        )

    @classmethod
    def from_features_list(
        cls: Type[L2GFeatureMatrix],
        session: Session,
        features_list: list[str],
        features_input_loader: L2GFeatureInputLoader,
    ) -> L2GFeatureMatrix:
        """Generate features from the gentropy datasets by calling the feature factory that will instantiate the corresponding features.

        Args:
            session (Session): Session object
            features_list (list[str]): List of objects with 2 keys corresponding to the features to generate: 'name' and 'path'.
            features_input_loader (L2GFeatureInputLoader): Object that contais features input.

        Returns:
            L2GFeatureMatrix: L2G feature matrix dataset
        """
        features_long_df = reduce(
            lambda x, y: x.unionByName(y, allowMissingColumns=True),
            [
                # Compute all features and merge them into a single dataframe
                feature.df
                for feature in FeatureFactory.generate_features(
                    session, features_list, features_input_loader
                )
            ],
        )
        return cls(
            _df=convert_from_long_to_wide(
                features_long_df,
                ["studyLocusId", "geneId"],
                "featureName",
                "featureValue",
            ),
        )

    def calculate_feature_missingness_rate(
        self: L2GFeatureMatrix,
    ) -> dict[str, float]:
        """Calculate the proportion of missing values in each feature.

        Returns:
            dict[str, float]: Dictionary of feature names and their missingness rate.

        Raises:
            ValueError: If no features are found.
        """
        total_count = self._df.count()
        if not self.features_list:
            raise ValueError("No features found")

        return {
            feature: (
                self._df.filter(
                    (self._df[feature].isNull()) | (self._df[feature] == 0)
                ).count()
                / total_count
            )
            for feature in self.features_list
        }

    def fill_na(
        self: L2GFeatureMatrix, value: float = 0.0, subset: list[str] | None = None
    ) -> L2GFeatureMatrix:
        """Fill missing values in a column with a given value.

        Args:
            value (float): Value to replace missing values with. Defaults to 0.0.
            subset (list[str] | None): Subset of columns to consider. Defaults to None.

        Returns:
            L2GFeatureMatrix: L2G feature matrix dataset
        """
        self._df = self._df.fillna(value, subset=subset)
        return self

    def select_features(
        self: L2GFeatureMatrix,
        features_list: list[str] | None,
    ) -> L2GFeatureMatrix:
        """Select a subset of features from the feature matrix.

        Args:
            features_list (list[str] | None): List of features to select

        Returns:
            L2GFeatureMatrix: L2G feature matrix dataset

        Raises:
            ValueError: If no features have been selected.
        """
        if features_list := features_list or self.features_list:
            # cast to float every feature in the features_list
            self._df = self._df.selectExpr(
                self.fixed_cols
                + [
                    f"CAST({feature} AS FLOAT) AS {feature}"
                    for feature in features_list
                ]
            )
            return self
        raise ValueError("features_list cannot be None")
        raise ValueError("features_list cannot be None")
