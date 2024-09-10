"""Collection of methods that extract features from the gentropy datasets to be fed in L2G."""

from __future__ import annotations

from typing import Any, Iterator, Mapping

import pyspark.sql.functions as f

from gentropy.common.session import Session
from gentropy.common.spark_helpers import convert_from_wide_to_long
from gentropy.dataset.l2g_feature import L2GFeature
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.v2g import V2G


class L2GFeatureInputLoader:
    """Loads all input datasets required for the L2GFeature dataset."""

    def __init__(
        self,
        **kwargs: Any,
    ) -> None:
        """Initializes L2GFeatureInputLoader with the provided inputs and returns loaded dependencies as a list.

        Args:
            **kwargs (Any): keyword arguments with the name of the dependency and the dependency itself.
        """
        self.input_dependencies = [v for v in kwargs.values() if v is not None]

    def get_dependency(self, dependency_type: Any) -> Any:
        """Returns the dependency that matches the provided type.

        Args:
            dependency_type (Any): type of the dependency to return.

        Returns:
            Any: dependency that matches the provided type.
        """
        for dependency in self.input_dependencies:
            if isinstance(dependency, dependency_type):
                return dependency

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Make the class iterable, returning the input dependencies list.

        Returns:
            Iterator[dict[str, Any]]: list of input dependencies.
        """
        return iter(self.input_dependencies)

    def __repr__(self) -> str:
        """Return a string representation of the input dependencies.

        Useful for understanding the loader content without having to print the object attribute.

        Returns:
            str: string representation of the input dependencies.
        """
        return repr(self.input_dependencies)


class DistanceTssMinimumFeature(L2GFeature):
    """Minimum distance of all tagging variants to gene TSS."""

    @classmethod
    def compute(
        cls: type[DistanceTssMinimumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: V2G,
    ) -> L2GFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (V2G): Dataset that contains the distance information

        Returns:
                L2GFeature: Feature dataset

        Raises:
            NotImplementedError: Not implemented
        """
        raise NotImplementedError


class DistanceTssMeanFeature(L2GFeature):
    """Average distance of all tagging variants to gene TSS."""

    fill_na_value = 500_000
    feature_dependency = V2G

    @classmethod
    def compute(
        cls: type[DistanceTssMeanFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: V2G,
    ) -> DistanceTssMeanFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (V2G): Dataset that contains the distance information

        Returns:
            DistanceTssMeanFeature: Feature dataset
        """
        agg_expr = f.mean("weightedScore").alias("distanceTssMean")
        # Everything but expresion is common logic
        v2g = feature_dependency.df.filter(f.col("datasourceId") == "canonical_tss")
        wide_df = (
            study_loci_to_annotate.df.withColumn(
                "variantInLocus", f.explode_outer("locus")
            )
            .select(
                "studyLocusId",
                f.col("variantInLocus.variantId").alias("variantInLocusId"),
                f.col("variantInLocus.posteriorProbability").alias(
                    "variantInLocusPosteriorProbability"
                ),
            )
            .join(
                v2g.selectExpr("variantId as variantInLocusId", "geneId", "score"),
                on="variantInLocusId",
                how="inner",
            )
            .withColumn(
                "weightedScore",
                f.col("score") * f.col("variantInLocusPosteriorProbability"),
            )
            .groupBy("studyLocusId", "geneId")
            .agg(agg_expr)
        )
        return cls(
            _df=convert_from_wide_to_long(
                wide_df,
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class FeatureFactory:
    """Factory class for creating features."""

    feature_mapper: Mapping[str, type[L2GFeature]] = {
        # "distanceTssMinimum": DistanceTssMinimumFeature,
        "distanceTssMean": DistanceTssMeanFeature,
    }

    def __init__(
        self: FeatureFactory,
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        features_list: list[str],
    ) -> None:
        """Initializes the factory.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            features_list (list[str]): list of features to compute.
        """
        self.study_loci_to_annotate = study_loci_to_annotate
        self.features_list = features_list

    def generate_features(
        self: FeatureFactory,
        session: Session,
        features_input_loader: L2GFeatureInputLoader,
    ) -> list[L2GFeature]:
        """Generates a feature matrix by reading an object with instructions on how to create the features.

        Args:
            session (Session): session object
            features_input_loader (L2GFeatureInputLoader): object with required features dependencies.

        Returns:
            list[L2GFeature]: list of computed features.

        Raises:
            ValueError: If feature not found.
        """
        computed_features = []
        for feature in self.features_list:
            if feature in self.feature_mapper:
                computed_features.append(
                    self.compute_feature(feature, features_input_loader)
                )
            else:
                raise ValueError(f"Feature {feature} not found.")
        return computed_features

    def compute_feature(
        self: FeatureFactory,
        feature_name: str,
        features_input_loader: L2GFeatureInputLoader,
    ) -> L2GFeature:
        """Instantiates feature class.

        Args:
            feature_name (str): name of the feature
            features_input_loader (L2GFeatureInputLoader): Object that contais features input.

        Returns:
            L2GFeature: instantiated feature object
        """
        # Extract feature class and dependency type
        feature_cls = self.feature_mapper[feature_name]
        feature_input_type = feature_cls.feature_dependency
        return feature_cls.compute(
            study_loci_to_annotate=self.study_loci_to_annotate,
            feature_dependency=features_input_loader.get_dependency(feature_input_type),
        )
