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
    """Computes the feature with positional overlap.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        intervals (Intervals): The dataset containing interval information
        feature_name (str): The name of the feature
        interval_source (str): The datasource of the interval input

    Returns:
        DataFrame: Feature dataset
    """
    study_loci_exploded = (
        study_loci_to_annotate.df.withColumn("variantInLocus", f.explode_outer("locus"))
        .withColumn(
            "chromosome", f.split(f.col("variantInLocus.variantId"), "_").getItem(0)
        )
        .withColumn(
            "position",
            f.split(f.col("variantInLocus.variantId"), "_").getItem(1).cast("long"),
        )
        .withColumn(
            "posteriorProbability",
            f.col("variantInLocus.posteriorProbability").cast("double"),
        )
        # Filter for PP > 0.001
        .filter(f.col("posteriorProbability") > 0.001)
        .select("studyLocusId", "chromosome", "position", "posteriorProbability")
    )

    intervals_filtered = (
        intervals.df.filter(f.col("datasourceId") == interval_source)
        .select("chromosome", "start", "end", "geneId", "resourceScore")
        .withColumn("start", f.col("start").cast("long"))
        .withColumn("end", f.col("end").cast("long"))
    )

    # Overlapping join:
    joined_data = study_loci_exploded.join(
        intervals_filtered,
        (study_loci_exploded["chromosome"] == intervals_filtered["chromosome"])
        & (study_loci_exploded["position"] >= intervals_filtered["start"])
        & (study_loci_exploded["position"] <= intervals_filtered["end"]),
        how="inner",
    )

    # Compute weighted interval score
    weighted_scores = joined_data.withColumn(
        "weightedIntervalScore", f.col("resourceScore") * f.col("posteriorProbability")
    )

    # Group by studyLocusId and geneId, compute mean weighted interval score
    return weighted_scores.groupBy("studyLocusId", "geneId").agg(
        f.mean("weightedIntervalScore").alias(feature_name)
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
    # First compute mean interval scores to a gene
    local_mean = common_interval_feature_logic(
        study_loci_to_annotate,
        feature_name=local_feature_name,
        intervals=intervals,
        interval_source=interval_source,
    )
    return (
        # Then compute the mean score in the vicinity (
        # feature will be the same for any gene associated with a studyLocus)
        local_mean.withColumn(
            "regional_mean",
            f.mean(local_feature_name).over(Window.partitionBy("studyLocusId")),
        )
        .withColumn(feature_name, f.col(local_feature_name) - f.col("regional_mean"))
        .drop("regional_mean", local_feature_name)
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

    In comparison to the Mean weighted CHiCAGO scores for all genes in the vicinity.
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
            PchicMeanNeighbourhoodFeature: Feature dataset
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


class EnhTssMeanFeature(L2GFeature):
    """Average weighted Enhancer-TSS correlation between studylocus and gene TSS."""

    fill_na_value = 0  # would be 0 if implemented
    feature_dependency_type = Intervals
    feature_name = "enhTssMean"

    @classmethod
    def compute(
        cls: type[EnhTssMeanFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> EnhTssMeanFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            EnhTssMeanFeature: Feature dataset
        """
        interval_source = "andersson2014"
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


class EnhTssMeanNeighbourhoodFeature(L2GFeature):
    """Average weighted Enhancer-TSS correlation from studylocus to gene TSS.

    Compared to the Mean weighted Enhancer-TSS correlation for all genes in the vicinity.
    """

    fill_na_value = 0  # would be 0 if implemented
    feature_dependency_type = Intervals
    feature_name = "enhTssMeanNeighbourhoodFeature"

    @classmethod
    def compute(
        cls: type[EnhTssMeanNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> EnhTssMeanNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            EnhTssMeanNeighbourhoodFeature: Feature dataset
        """
        interval_source = "andersson2014"
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


class DhsPmtrMeanFeature(L2GFeature):
    """Average weighted DHS-promoter correlation between studylocus and gene TSS."""

    fill_na_value = 0  # would be 0 if implemented
    feature_dependency_type = Intervals
    feature_name = "dhsPmtrMean"

    @classmethod
    def compute(
        cls: type[DhsPmtrMeanFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DhsPmtrMeanFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DhsPmtrMeanFeature: Feature dataset
        """
        interval_source = "thurman2012"
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


class DhsPmtrMeanNeighbourhoodFeature(L2GFeature):
    """Average weighted DHS-promoter correlation from studylocus to gene TSS.

    Compared to the Mean weighted DHS-promoter correlation for all genes in the vicinity.
    """

    fill_na_value = 0  # would be 0 if implemented
    feature_dependency_type = Intervals
    feature_name = "dhsPmtrMeanNeighbourhood"

    @classmethod
    def compute(
        cls: type[DhsPmtrMeanNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DhsPmtrMeanNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DhsPmtrMeanNeighbourhoodFeature: Feature dataset
        """
        interval_source = "thurman2012"
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
