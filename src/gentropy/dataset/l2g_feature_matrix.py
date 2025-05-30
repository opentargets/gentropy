"""Feature matrix of study locus pairs annotated with their functional genomics features."""

from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from loguru import logger
from pyspark.sql import Window
from pyspark.sql import types as t
from typing_extensions import Self

from gentropy.common.exceptions import L2GFeatureError
from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import convert_from_long_to_wide
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.l2g_features.namespace import L2GFeatureName
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.method.l2g.feature_factory import FeatureFactory, L2GFeatureInputLoader

if TYPE_CHECKING:
    from collections.abc import Collection
    from typing import ClassVar

    from pyspark.sql import DataFrame

    from gentropy.dataset.study_locus import StudyLocus


class L2GFeatureMatrix(Dataset):
    """Dataset with features for Locus to Gene prediction."""

    _requestable_feature_names: ClassVar[Collection[L2GFeatureName]] = set(
        map(str, L2GFeatureName)
    )

    @classmethod
    def get_schema(cls) -> t.StructType:
        """Provide the schema for the l2GFeatureMatrix dataset.

        Returns:
            StructType: The schema of the BiosampleIndex dataset.
        """
        schema = parse_spark_schema("l2g_feature_matrix.json")

        for name in cls._requestable_feature_names:
            schema = schema.add(name, t.FloatType(), nullable=True)
        return schema

    @classmethod
    def build(
        cls: type[L2GFeatureMatrix],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        features_list: list[str],
        features_input_loader: L2GFeatureInputLoader,
    ) -> L2GFeatureMatrix:
        """Generate features from the gentropy datasets by calling the feature factory that will instantiate the corresponding features.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): Study locus pairs to annotate
            features_list (list[str]): List of feature names to be computed.
            features_input_loader (L2GFeatureInputLoader): Object that contais features input.

        Returns:
            L2GFeatureMatrix: L2G feature matrix dataset
        """
        
            def generate_features(
        self: FeatureFactory,
        features_input_loader: L2GFeatureInputLoader,
    ) -> L2GFeatureMatrix:
        """Generates a feature matrix by reading an object with instructions on how to create the features.

        Args:
            features_input_loader (L2GFeatureInputLoader): object with required features dependencies.

        Returns:
            list[L2GFeature]: list of computed features.

        Raises:
            ValueError: If feature not found.
        """
        computed_features = []
        for feature in self.features_list:
            feature = self.compute_feature(feature, features_input_loader)
            computed_features.append()
        return computed_features

        features = FeatureFactory(
            study_loci_to_annotate, features_list
        ).generate_features(features_input_loader)

        features_long_df = reduce(
            lambda x, y: x.unionByName(y, allowMissingColumns=True),
            [
                # Compute all features and merge them into a single dataframe
                feature.df
                for feature in features
            ],
        )
        if isinstance(study_loci_to_annotate, L2GGoldStandard):
            return cls(
                _df=convert_from_long_to_wide(
                    # Add gold standard set to the feature matrix
                    features_long_df.join(
                        study_loci_to_annotate.df.select(
                            "studyLocusId", "geneId", "goldStandardSet"
                        ),
                        ["studyLocusId", "geneId"],
                    ),
                    ["studyLocusId", "geneId", "goldStandardSet"],
                    "featureName",
                    "featureValue",
                ),
                with_gold_standard=True,
            )
        return cls(
            _df=convert_from_long_to_wide(
                features_long_df,
                ["studyLocusId", "geneId"],
                "featureName",
                "featureValue",
            ),
            with_gold_standard=False,
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
        self: L2GFeatureMatrix, na_value: float = 0.0, subset: list[str] | None = None
    ) -> L2GFeatureMatrix:
        """Fill missing values in a column with a given value.

        For features that correspond to gene attributes, missing values are imputed using the mean of the column.

        Args:
            na_value (float): Value to replace missing values with. Defaults to 0.0.
            subset (list[str] | None): Subset of columns to consider. Defaults to None.

        Returns:
            L2GFeatureMatrix: L2G feature matrix dataset
        """
        cols_to_impute = [
            "proteinGeneCount500kb",
            "geneCount500kb",
        ]
        for col in cols_to_impute:
            if col not in self._df.columns:
                continue
            else:
                self._df = self._df.withColumn(
                    col,
                    f.when(
                        f.col(col).isNull(),
                        f.mean(f.col(col)).over(Window.partitionBy("studyLocusId")),
                    ).otherwise(f.col(col)),
                )
        self._df = self._df.fillna(na_value, subset=subset)
        return self

    def select_features(
        self: L2GFeatureMatrix,
        features_list: list[str] | None,
    ) -> L2GFeatureMatrix:
        """Returns a new object with a subset of features from the original feature matrix.

        Args:
            features_list (list[str] | None): List of features to select

        Returns:
            L2GFeatureMatrix: L2G feature matrix dataset

        Raises:
            ValueError: If no features have been selected.
        """
        if features_list := features_list or self.features_list:
            # cast to float every feature in the features_list
            return L2GFeatureMatrix(
                _df=self._df.selectExpr(
                    self.fixed_cols
                    + [
                        f"CAST({feature} AS FLOAT) AS {feature}"
                        for feature in features_list
                    ]
                ),
                features_list=features_list,
                with_gold_standard=self.with_gold_standard,
            )
        raise ValueError("features_list cannot be None")

    def persist(self: Self) -> Self:
        """Persist the feature matrix in memory.

        Returns:
            Self: Persisted Dataset
        """
        self._df = self._df.persist()
        return self
