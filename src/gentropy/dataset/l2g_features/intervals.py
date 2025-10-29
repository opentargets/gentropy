"""Collection of methods that extract features from the interval datasets."""

from __future__ import annotations

from typing import Any

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window

from gentropy.common.spark import convert_from_wide_to_long
from gentropy.dataset.intervals import Intervals
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_locus import StudyLocus


def e2g_interval_feature_wide_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    *,
    intervals: Intervals,
    base_name: str = "e2gMean",
) -> DataFrame:
    """Computes the feature with positional overlap.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        intervals (Intervals): The dataset containing interval information
        base_name (str): The base name of the feature

    Returns:
      DataFrame: a WIDE DF with studyLocusId, geneId, e2gMean, e2gMeanNeighbourhood, neighbourhood is ratio-centred:
      e2gMeanNeighbourhood = e2gMean / mean(e2gMean within locus)
    """
    sl = study_loci_to_annotate.df.alias("sl")
    iv = intervals.df.alias("iv")

    study_loci_exploded = (
        sl.withColumn("variantInLocus", f.explode_outer("locus"))
        .withColumn("chromosome", f.split("variantInLocus.variantId", "_").getItem(0))
        .withColumn(
            "position", f.split("variantInLocus.variantId", "_").getItem(1).cast("int")
        )
        .withColumn(
            "posteriorProbability",
            f.col("variantInLocus.posteriorProbability").cast("double"),
        )
        .filter(f.col("posteriorProbability") > 0.001)
        .select(
            f.col("studyLocusId").alias("studyLocusId"),
            f.col("chromosome").alias("sl_chromosome"),
            f.col("position").alias("position"),
            f.col("posteriorProbability").alias("pp"),
        )
        .alias("slx")
    )

    intervals_filtered = iv.select(
        f.col("chromosome").alias("iv_chromosome"),
        f.col("start").cast("int").alias("start"),
        f.col("end").cast("int").alias("end"),
        f.col("geneId").alias("geneId"),
        f.col("score").alias("score"),
    ).alias("ivf")

    # Overlap join (note the qualified column names)
    joined = (
        study_loci_exploded.join(
            intervals_filtered,
            (f.col("slx.sl_chromosome") == f.col("ivf.iv_chromosome"))
            & (f.col("position") >= f.col("start"))
            & (f.col("position") <= f.col("end")),
            "inner",
        )
        # Keep ONE set of columns with clear names
        .select(
            f.col("studyLocusId"),
            f.col("slx.sl_chromosome").alias("chromosome"),
            f.col("position"),
            f.col("pp").alias("posteriorProbability"),
            f.col("geneId"),
            f.col("score"),
        )
    )

    # Per-variant per-gene MAX and carry PP
    per_variant_gene = joined.groupBy(
        "studyLocusId", "chromosome", "position", "geneId"
    ).agg(
        f.max("score").alias("maxScore"),
        f.first("posteriorProbability", ignorenulls=True).alias("pp"),
    )

    # Weight & aggregate to gene per locus
    base_df = (
        per_variant_gene.withColumn(
            "weightedIntervalScore", f.col("maxScore") * f.col("pp")
        )
        .groupBy("studyLocusId", "geneId")
        .agg(f.sum("weightedIntervalScore").alias(base_name))
    ).persist()

    # Neighbourhood ratio within locus (safe divide)
    w = Window.partitionBy("studyLocusId")
    with_mean = base_df.withColumn("regional_mean", f.mean(base_name).over(w))
    neigh_ratio = f.when(
        f.col("regional_mean") != 0, f.col(base_name) / f.col("regional_mean")
    ).otherwise(f.lit(0.0))

    wide = with_mean.select(
        "studyLocusId",
        "geneId",
        f.col(base_name).alias(base_name),
        neigh_ratio.alias(f"{base_name}Neighbourhood"),
    )
    return wide


def get_or_make_e2g_wide(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    *,
    feature_dependency: dict[str, Any],
    base_name: str = "e2gMean",
) -> DataFrame:
    """Compute or retrieve from cache the e2g wide feature DataFrame.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        feature_dependency (dict[str, Any]): Dataset that contains the e2g information
        base_name (str): The base name of the feature

    Returns:
        DataFrame: Features dataset
    """
    cache_key = f"_e2g_wide::{base_name}"
    if cache_key not in feature_dependency:
        wide = e2g_interval_feature_wide_logic(
            study_loci_to_annotate,
            intervals=feature_dependency["intervals"],
            base_name=base_name,
        ).persist()
        feature_dependency[cache_key] = wide
    return feature_dependency[cache_key]


class E2gMeanFeature(L2GFeature):
    """e2gMean feature from E2G intervals."""

    feature_dependency_type = Intervals
    feature_name = "e2gMean"

    @classmethod
    def compute(
        cls: type[E2gMeanFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> E2gMeanFeature:
        """Compute e2gMean feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
                that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the e2g information, expecting intervals
        Returns:
            E2gMeanFeature: Computed e2gMean feature.
        """
        wide = get_or_make_e2g_wide(
            study_loci_to_annotate,
            feature_dependency=feature_dependency,
            base_name=cls.feature_name,
        )
        df_long = convert_from_wide_to_long(
            wide.select("studyLocusId", "geneId", cls.feature_name),
            id_vars=("studyLocusId", "geneId"),
            var_name="featureName",
            value_name="featureValue",
            value_vars=(cls.feature_name,),
        )
        return cls(_df=df_long, _schema=cls.get_schema())


class E2gMeanNeighbourhoodFeature(L2GFeature):
    """e2gMeanNeighbourhood feature from E2G intervals."""

    feature_dependency_type = Intervals
    feature_name = "e2gMeanNeighbourhood"

    @classmethod
    def compute(
        cls: type[E2gMeanNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> E2gMeanNeighbourhoodFeature:
        """Compute e2gMeanNeighbourhood feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
                that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the e2g information, expecting intervals
        Returns:
            E2gMeanNeighbourhoodFeature: Computed e2gMeanNeighbourhood feature.
        """
        wide = get_or_make_e2g_wide(
            study_loci_to_annotate,
            feature_dependency=feature_dependency,
            base_name="e2gMean",
        )
        df_long = convert_from_wide_to_long(
            wide.select("studyLocusId", "geneId", cls.feature_name),
            id_vars=("studyLocusId", "geneId"),
            var_name="featureName",
            value_name="featureValue",
            value_vars=(cls.feature_name,),
        )
        return cls(_df=df_long, _schema=cls.get_schema())
