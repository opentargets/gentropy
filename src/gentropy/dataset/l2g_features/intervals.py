"""Collection of methods that extract features from the interval datasets."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
from pyspark.sql import Window

from gentropy.common.spark_helpers import convert_from_wide_to_long

# from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.intervals import Intervals
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard

# from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def common_interval_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    *,
    intervals: Intervals,
    feature_name: str,
    interval_source: str,
) -> DataFrame:
    """Computes the feature.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        intervals (Intervals): The dataset containing interval information
        feature_name (str): The name of the feature
        interval_source (str): The datasource of the interval input

    Returns:
            DataFrame: Feature dataset
    """
    # Only implementing mean average interval features.
    agg_expr = f.mean(f.col("weightedIntervalScore"))
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
            intervals.df.filter(f.col("datasourceId") == interval_source)
            .withColumnRenamed("variantId", "variantInLocusId")
            .withColumnRenamed("targetId", "geneId"),
            on=["variantInLocusId", "geneId"],
            how="inner",
        )
        .withColumn(
            "weightedIntervalScore",
            f.col("resourceScore") * f.col("variantInLocusPosteriorProbability"),
        )
        .groupBy("studyLocusId", "geneId")
        .agg(agg_expr.alias(feature_name))
    )


def common_neighbourhood_interval_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    *,
    intervals: Intervals,
    feature_name: str,
    interval_source: str,
) -> DataFrame:
    """Computes the feature.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
        intervals (Intervals): The dataset containing interval information
        feature_name (str): The name of the feature
        interval_source (str): The datasource of the interval input

    Returns:
            DataFrame: Feature dataset
    """
    local_feature_name = feature_name.replace("Neighbourhood", "")
    # First compute mean distances to a gene
    local_max = common_interval_feature_logic(
        study_loci_to_annotate,
        feature_name=local_feature_name,
        intervals=intervals,
        interval_source=interval_source,
    )
    return (
        # Then compute the max score in the vicinity (
        # feature will be the same for any gene associated with a studyLocus)
        local_max.withColumn(
            "regional_maximum",
            f.max(local_feature_name).over(Window.partitionBy("studyLocusId")),
        )
        .withColumn(feature_name, f.col(local_feature_name) - f.col("regional_maximum"))
        .drop("regional_maximum")
    )


class PchicMeanFeature(L2GFeature):
    """Average weighted CHiCAGO scores from studylocus to gene TSS."""

    fill_na_value = 0  # would be 0 if implemented
    feature_dependency_type = Intervals
    feature_name = "pchicMean"

    @classmethod
    def compute(
        cls: type[PchicMeanFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PchicMeanFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            PchicMeanFeature: Feature dataset
        """
        interval_source = "javierre2016"
        return cls(
            _df=convert_from_wide_to_long(
                common_interval_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    interval_source=interval_source,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class PchicMeanNeighbourhoodFeature(L2GFeature):
    """Average weighted CHiCAGO scores from studylocus to gene TSS.

    Proportional to strongest weighted CHiCAGO scores for all genes in the vicinity.
    """

    fill_na_value = 0  # would be 0 if implemented
    feature_dependency_type = Intervals
    feature_name = "pchicMeanNeighbourhood"

    @classmethod
    def compute(
        cls: type[PchicMeanNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PchicMeanNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceFootprintMeanNeighbourhoodFeature: Feature dataset
        """
        interval_source = "javierre2016"
        return cls(
            _df=convert_from_wide_to_long(
                common_neighbourhood_interval_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    interval_source=interval_source,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )
