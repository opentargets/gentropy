"""Collection of methods that extract features from the gentropy datasets to be fed in L2G."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

import pyspark.sql.functions as f

from gentropy.common.session import Session
from gentropy.common.spark_helpers import convert_from_wide_to_long
from gentropy.dataset.l2g_feature import L2GFeature
from gentropy.dataset.study_locus import StudyLocus

if TYPE_CHECKING:
    from gentropy.dataset.v2g import V2G


class L2GFeatureInputLoader:
    """Loads all input datasets required for the L2GFeature dataset."""

    def __init__(
        self,
        **kwargs: dict[str, Any],
    ) -> None:
        """Initializes L2GFeatureInputLoader with the provided inputs and returns loaded dependencies as a list."""
        self.input_dependencies = [v for v in kwargs.values() if v is not None]

    def get_dependency(self, dependency_type: Any) -> Any:
        """Returns the dependency that matches the provided type."""
        for dependency in self.input_dependencies:
            if isinstance(dependency, dependency_type):
                return dependency

    def __iter__(self) -> list[Any]:
        """Make the class iterable, returning the input dependencies list."""
        return iter(self.input_dependencies)

    def __repr__(self) -> str:
        """Return a string representation of the input dependencies.

        Useful for understanding the loader content without having to print the object attribute.
        """
        return repr(self.input_dependencies)


class DistanceTssMinimumFeature(L2GFeature):
    """Minimum distance of all tagging variants to gene TSS."""

    @classmethod
    def compute(
        cls: type[DistanceTssMinimumFeature], input_dependency: V2G
    ) -> L2GFeature:
        """Computes the feature.

        Args:
            input_dependency (V2G): V2G dependency

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
        credible_set: StudyLocus,
        feature_dependency: V2G,
    ) -> Any:
        """Computes the feature.

        Args:
            credible_set (StudyLocus): Credible set dependency
            feature_dependency (V2G): Dataset that contains the distance information

        Returns:
            L2GFeature: Feature dataset
        """
        agg_expr = f.mean("weightedScore").alias("distanceTssMean")
        # Everything but expresion is common logic
        v2g = feature_dependency.df.filter(f.col("datasourceId") == "canonical_tss")
        wide_df = (
            credible_set.df.withColumn("variantInLocus", f.explode_outer("locus"))
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
    features_input_loader: L2GFeatureInputLoader

    def __init__(self: FeatureFactory, credible_set: StudyLocus) -> None:
        """Initializes the factory.

        Args:
            credible_set (StudyLocus): credible sets to annotate
        """
        self.credible_set = credible_set

    @classmethod
    def generate_features(
        cls: type[FeatureFactory],
        session: Session,
        features_list: list[dict[str, str]],
        credible_set_path: str,
        features_input_loader: L2GFeatureInputLoader,
    ) -> list[L2GFeature]:
        """Generates a feature matrix by reading an object with instructions on how to create the features.

        Args:
            session (Session): session object
            features_list (list[dict[str, str]]): list of objects with 2 keys: 'name' and 'path'.
            credible_set_path (str | None): path to credible set parquet file.
            features_input_loader (L2GFeatureInputLoader): object with required features dependencies.

        Returns:
            list[L2GFeature]: list of computed features.
        """
        cls.features_input_loader = features_input_loader
        computed_features = []
        for feature in features_list:
            if feature["name"] in cls.feature_mapper:
                computed_features.append(cls.compute_feature(feature["name"]))
            else:
                raise ValueError(f"Feature {feature['name']} not found.")
        return computed_features

    def compute_feature(self: FeatureFactory, feature_name: str) -> L2GFeature:
        """Instantiates feature class.

        Args:
            feature_name (str): name of the feature

        Returns:
            L2GFeature: instantiated feature object
        """
        feature_cls = self.feature_mapper[feature_name]
        # Filter features_input_loader to pass only the dependency that the feature needs
        feature_input_type = feature_cls.feature_dependency
        feature_input = cls.features_input_loader.get_dependency(feature_input_type)
        return feature_cls.compute(feature_input)
