"""Collection of methods that extract distance features from the variant index dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
from pyspark.sql import Window

from gentropy.common.spark_helpers import convert_from_wide_to_long
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.variant_index import VariantIndex

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def _common_distance_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    *,
    variant_index: VariantIndex,
    feature_name: str,
    distance_type: str,
    agg_expr: Column,
) -> DataFrame:
    """Computes the feature.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
        variant_index (VariantIndex): The dataset containing distance to gene information
        feature_name (str): The name of the feature
        distance_type (str): The type of distance to gene
        agg_expr (Column): The expression that aggregate distances into a specific way to define the feature

    Returns:
        DataFrame: Feature dataset
    """
    distances_dataset = variant_index.get_distance_to_gene(distance_type=distance_type)
    return (
        study_loci_to_annotate.df.withColumn("variantInLocus", f.explode_outer("locus"))
        .select(
            "studyLocusId",
            f.col("variantInLocus.variantId").alias("variantInLocusId"),
            f.col("variantInLocus.posteriorProbability").alias(
                "variantInLocusPosteriorProbability"
            ),
        )
        .join(
            distances_dataset.withColumnRenamed(
                "variantId", "variantInLocusId"
            ).withColumnRenamed("targetId", "geneId"),
            on="variantInLocusId",
            how="inner",
        )
        .withColumn(
            "weightedDistance",
            f.col(distance_type) * f.col("variantInLocusPosteriorProbability"),
        )
        .groupBy("studyLocusId", "geneId")
        .agg(agg_expr.alias(feature_name))
    )


def _common_neighbourhood_distance_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    *,
    variant_index: VariantIndex,
    feature_name: str,
    distance_type: str,
    agg_expr: Column,
) -> DataFrame:
    """Calculate the neighbourhood distance feature.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
        variant_index (VariantIndex): The dataset containing distance to gene information
        feature_name (str): The name of the feature
        distance_type (str): The type of distance to gene
        agg_expr (Column): The expression that aggregate distances into a specific way to define the feature

    Returns:
            DataFrame: Feature dataset
    """
    local_feature_name = feature_name.replace("Neighbourhood", "")
    # First compute mean distances to a gene
    local_min = _common_distance_feature_logic(
        study_loci_to_annotate,
        feature_name=local_feature_name,
        distance_type=distance_type,
        agg_expr=agg_expr,
        variant_index=variant_index,
    )
    return (
        # Then compute minimum distance in the vicinity (feature will be the same for any gene associated with a studyLocus)
        local_min.withColumn(
            "regional_minimum",
            f.min(local_feature_name).over(Window.partitionBy("studyLocusId")),
        )
        .withColumn(feature_name, f.col("regional_minimum") - f.col(local_feature_name))
        .drop("regional_minimum")
    )


class DistanceTssMeanFeature(L2GFeature):
    """Average distance of all tagging variants to gene TSS."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "distanceTssMean"

    @classmethod
    def compute(
        cls: type[DistanceTssMeanFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceTssMeanFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceTssMeanFeature: Feature dataset
        """
        agg_expr = f.mean("weightedDistance")
        distance_type = "distanceFromTss"
        return cls(
            _df=convert_from_wide_to_long(
                _common_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class DistanceTssMeanNeighbourhoodFeature(L2GFeature):
    """Minimum mean distance to TSS for all genes in the vicinity of a studyLocus."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "distanceTssMeanNeighbourhood"

    @classmethod
    def compute(
        cls: type[DistanceTssMeanNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceTssMeanNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceTssMeanNeighbourhoodFeature: Feature dataset
        """
        agg_expr = f.mean("weightedDistance")
        distance_type = "distanceFromTss"
        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class DistanceTssMinimumFeature(L2GFeature):
    """Minimum distance of all tagging variants to gene TSS."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "distanceTssMinimum"

    @classmethod
    def compute(
        cls: type[DistanceTssMinimumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceTssMinimumFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceTssMinimumFeature: Feature dataset
        """
        agg_expr = f.mean("weightedDistance")
        distance_type = "distanceFromTss"
        return cls(
            _df=convert_from_wide_to_long(
                _common_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class DistanceTssMinimumNeighbourhoodFeature(L2GFeature):
    """Minimum minimum distance to TSS for all genes in the vicinity of a studyLocus."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "distanceTssMinimumNeighbourhood"

    @classmethod
    def compute(
        cls: type[DistanceTssMinimumNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceTssMinimumNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceTssMinimumNeighbourhoodFeature: Feature dataset
        """
        agg_expr = f.min("weightedDistance")
        distance_type = "distanceFromTss"
        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class DistanceFootprintMeanFeature(L2GFeature):
    """Average distance of all tagging variants to the footprint of a gene."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "distanceFootprintMean"

    @classmethod
    def compute(
        cls: type[DistanceFootprintMeanFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceFootprintMeanFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceFootprintMeanFeature: Feature dataset
        """
        agg_expr = f.mean("weightedDistance")
        distance_type = "distanceFromFootprint"
        return cls(
            _df=convert_from_wide_to_long(
                _common_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class DistanceFootprintMeanNeighbourhoodFeature(L2GFeature):
    """Minimum mean distance to footprint for all genes in the vicinity of a studyLocus."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "distanceFootprintMeanNeighbourhood"

    @classmethod
    def compute(
        cls: type[DistanceFootprintMeanNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceFootprintMeanNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceFootprintMeanNeighbourhoodFeature: Feature dataset
        """
        agg_expr = f.mean("weightedDistance")
        distance_type = "distanceFromFootprint"
        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class DistanceFootprintMinimumFeature(L2GFeature):
    """Minimum distance of all tagging variants to the footprint of a gene."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "DistanceFootprintMinimum"

    @classmethod
    def compute(
        cls: type[DistanceFootprintMinimumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceFootprintMinimumFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceFootprintMinimumFeature: Feature dataset
        """
        agg_expr = f.mean("weightedDistance")
        distance_type = "distanceFromFootprint"
        return cls(
            _df=convert_from_wide_to_long(
                _common_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class DistanceFootprintMinimumNeighbourhoodFeature(L2GFeature):
    """Minimum minimum distance to footprint for all genes in the vicinity of a studyLocus."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "distanceFootprintMinimumNeighbourhood"

    @classmethod
    def compute(
        cls: type[DistanceFootprintMinimumNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceFootprintMinimumNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceFootprintMinimumNeighbourhoodFeature: Feature dataset
        """
        agg_expr = f.min("weightedDistance")
        distance_type = "distanceFromFootprint"
        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )
