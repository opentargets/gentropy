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


def _explode_interval_bins(
    iv: DataFrame,
    *,
    bin_size: int,
    max_bins_per_interval: int,
) -> DataFrame:
    """Given iv(df): columns [iv_chromosome, start, end, geneId, score].

    Returns columns with interval bins exploded: [iv_chromosome, start, end, geneId, score, iv_bin]

    Args:
        iv (DataFrame): Intervals DataFrame
        bin_size (int): Size of bins for the binned overlap
        max_bins_per_interval (int): Maximum number of bins to explode per interval
    Returns:
        DataFrame: DataFrame with interval bins exploded
    """
    start_bin = (f.col("start") / f.lit(bin_size)).cast("long")
    end_bin = (f.col("end") / f.lit(bin_size)).cast("long")
    n_bins = end_bin - start_bin + f.lit(1)

    df = (
        iv.withColumn("start_bin", start_bin)
        .withColumn("end_bin", end_bin)
        .withColumn("n_bins", n_bins)
        .filter(f.col("n_bins") > 0)
        .filter(f.col("n_bins") <= f.lit(max_bins_per_interval))
        .withColumn("bin_seq", f.sequence(f.col("start_bin"), f.col("end_bin")))
        .withColumn("iv_bin", f.explode("bin_seq"))
        .drop("bin_seq", "start_bin", "end_bin", "n_bins")
    )
    return df


def e2g_interval_feature_wide_logic_binned(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    *,
    intervals: Intervals,
    base_name: str = "e2gMean",
    pp_min: float = 0.001,
    bin_size: int = 50_000,
    max_bins_per_interval: int = 1000,
    repartitions_variants: int | None = None,
    repartitions_intervals: int | None = None,
) -> DataFrame:
    """Computes the feature using a bin accelerated overlap.

      1) Bin variants: var_bin = floor(position / bin_size)
      2) Explode interval bins: iv_bin across [start_bin, end_bin] with safety cap
      3) Join on (chromosome, bin), then exact position filter
      4) Per variant per gene take max(score); weight by PP; sum to gene per locus
      5) Add neighbourhood ratio within locus

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        intervals (Intervals): The dataset containing interval information
        base_name (str): The base name of the feature
        pp_min (float): Minimum posterior probability to consider a variant
        bin_size (int): Size of bins for the binned overlap
        max_bins_per_interval (int): Maximum number of bins to explode per interval
        repartitions_variants (int | None): Number of repartitions for variant side
        repartitions_intervals (int | None): Number of repartitions for interval side

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
        .withColumn("pp", f.col("variantInLocus.posteriorProbability").cast("double"))
        .filter(f.col("pp") >= f.lit(pp_min))
        .select(
            f.col("studyLocusId").alias("studyLocusId"),
            f.col("chromosome").alias("sl_chromosome"),
            f.col("position").alias("position"),
            f.col("pp").alias("pp"),
        )
        .filter(
            f.col("sl_chromosome").isNotNull()
            & f.col("position").isNotNull()
            & f.col("pp").isNotNull()
        )
        .alias("slx")
    )

    # Intervals minimal selection
    intervals_filtered = (
        iv.select(
            f.col("chromosome").alias("iv_chromosome"),
            f.col("start").cast("int").alias("start"),
            f.col("end").cast("int").alias("end"),
            f.col("geneId").alias("geneId"),
            f.col("score").cast("double").alias("score"),
        )
        .filter(f.col("score").isNotNull())
        .alias("ivf")
    )

    # Add bins on both sides
    slx_binned = study_loci_exploded.withColumn(
        "var_bin", (f.col("position") / f.lit(bin_size)).cast("long")
    )
    if repartitions_variants:
        slx_binned = slx_binned.repartition(
            repartitions_variants, "sl_chromosome", "var_bin"
        )
    else:
        slx_binned = slx_binned.repartition("sl_chromosome", "var_bin")

    ivf_binned = _explode_interval_bins(
        intervals_filtered,
        bin_size=bin_size,
        max_bins_per_interval=max_bins_per_interval,
    )
    if repartitions_intervals:
        ivf_binned = ivf_binned.repartition(
            repartitions_intervals, "iv_chromosome", "iv_bin"
        )
    else:
        ivf_binned = ivf_binned.repartition("iv_chromosome", "iv_bin")

    # Bin join then exact positional filter
    joined = (
        slx_binned.alias("cs")
        .join(
            ivf_binned.alias("iv"),
            on=[
                f.col("cs.sl_chromosome") == f.col("iv.iv_chromosome"),
                f.col("cs.var_bin") == f.col("iv.iv_bin"),
            ],
            how="inner",
        )
        .filter(
            (f.col("cs.position") >= f.col("iv.start"))
            & (f.col("cs.position") <= f.col("iv.end"))
        )
        .select(
            f.col("cs.studyLocusId").alias("studyLocusId"),
            f.col("cs.sl_chromosome").alias("chromosome"),
            f.col("cs.position").alias("position"),
            f.col("cs.pp").alias("pp"),
            f.col("iv.geneId").alias("geneId"),
            f.col("iv.score").alias("score"),
        )
    )

    # Per variant per gene max interval score, keep pp
    per_variant_gene = joined.groupBy(
        "studyLocusId", "chromosome", "position", "geneId"
    ).agg(
        f.max("score").alias("maxScore"),
        f.first("pp", ignorenulls=True).alias("pp"),
    )

    # Weight and aggregate to gene per locus
    base_df = (
        per_variant_gene.withColumn(
            "weightedIntervalScore", f.col("maxScore") * f.col("pp")
        )
        .groupBy("studyLocusId", "geneId")
        .agg(f.sum("weightedIntervalScore").alias(base_name))
    ).persist()

    # Neighbourhood ratio within locus, using locus max as the denominator
    w = Window.partitionBy("studyLocusId")
    with_max = base_df.withColumn("regional_max", f.max(base_name).over(w))
    neigh_ratio = f.when(
        f.col("regional_max") != 0, f.col(base_name) / f.col("regional_max")
    ).otherwise(f.lit(0.0))

    wide = with_max.select(
        "studyLocusId",
        "geneId",
        f.col(base_name).alias(base_name),
        neigh_ratio.alias(f"{base_name}Neighbourhood"),
    )
    return wide


def e2g_interval_feature_wide_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    *,
    intervals: Intervals,
    base_name: str = "e2gMean",
    use_binned: bool = True,
    pp_min: float = 0.001,
    bin_size: int = 50_000,
    max_bins_per_interval: int = 200,
    repartitions_variants: int | None = None,
    repartitions_intervals: int | None = None,
) -> DataFrame:
    """Wrapper that defaults to the binned implementation.

    Set use_binned=False to fall back to a plain overlap if ever needed.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        intervals (Intervals): The dataset containing interval information
        base_name (str): The base name of the feature
        use_binned (bool): Whether to use the binned overlap logic
        pp_min (float): Minimum posterior probability to consider a variant
        bin_size (int): Size of bins for the binned overlap
        max_bins_per_interval (int): Maximum number of bins to explode per interval
        repartitions_variants (int | None): Number of repartitions for variant side
        repartitions_intervals (int | None): Number of repartitions for interval side

    Returns:
      DataFrame: a WIDE DF with studyLocusId, geneId, e2gMean, e2gMeanNeighbourhood, neighbourhood is ratio-centred:
      e2gMeanNeighbourhood = e2gMean / mean(e2gMean within locus)
    """
    if use_binned:
        return e2g_interval_feature_wide_logic_binned(
            study_loci_to_annotate,
            intervals=intervals,
            base_name=base_name,
            pp_min=pp_min,
            bin_size=bin_size,
            max_bins_per_interval=max_bins_per_interval,
            repartitions_variants=repartitions_variants,
            repartitions_intervals=repartitions_intervals,
        )

    # Fallback: original plain overlap logic (kept for completeness)
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
        .filter(f.col("posteriorProbability") > f.lit(pp_min))
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

    joined = study_loci_exploded.join(
        intervals_filtered,
        (f.col("slx.sl_chromosome") == f.col("ivf.iv_chromosome"))
        & (f.col("position") >= f.col("start"))
        & (f.col("position") <= f.col("end")),
        "inner",
    ).select(
        f.col("studyLocusId"),
        f.col("slx.sl_chromosome").alias("chromosome"),
        f.col("position"),
        f.col("pp"),
        f.col("geneId"),
        f.col("score"),
    )

    per_variant_gene = joined.groupBy(
        "studyLocusId", "chromosome", "position", "geneId"
    ).agg(
        f.max("score").alias("maxScore"),
        f.first("pp", ignorenulls=True).alias("pp"),
    )

    base_df = (
        per_variant_gene.withColumn(
            "weightedIntervalScore", f.col("maxScore") * f.col("pp")
        )
        .groupBy("studyLocusId", "geneId")
        .agg(f.sum("weightedIntervalScore").alias(base_name))
    ).persist()

    w = Window.partitionBy("studyLocusId")
    with_max = base_df.withColumn("regional_max", f.max(base_name).over(w))
    neigh_ratio = f.when(
        f.col("regional_max") != 0, f.col(base_name) / f.col("regional_max")
    ).otherwise(f.lit(0.0))

    wide = with_max.select(
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
    use_binned: bool = True,
    pp_min: float = 0.001,
    bin_size: int = 50_000,
    max_bins_per_interval: int = 200,
    repartitions_variants: int | None = None,
    repartitions_intervals: int | None = None,
) -> DataFrame:
    """Compute or retrieve the e2g wide feature DataFrame with optional binned join settings.

    The cache key incorporates parameters that affect output.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci
            that will be used for annotation
        feature_dependency (dict[str, Any]): Dataset that contains the e2g information
        base_name (str): The base name of the feature
        use_binned (bool): Whether to use the binned overlap logic
        pp_min (float): Minimum posterior probability to consider a variant
        bin_size (int): Size of bins for the binned overlap
        max_bins_per_interval (int): Maximum number of bins to explode per interval
        repartitions_variants (int | None): Number of repartitions for variant side
        repartitions_intervals (int | None): Number of repartitions for interval side

    Returns:
        DataFrame: Features dataset
    """
    cache_key = f"_e2g_wide::{base_name}::binned={use_binned}::ppmin={pp_min}::bin={bin_size}::cap={max_bins_per_interval}"
    if cache_key not in feature_dependency:
        wide = e2g_interval_feature_wide_logic(
            study_loci_to_annotate,
            intervals=feature_dependency["intervals"],
            base_name=base_name,
            use_binned=use_binned,
            pp_min=pp_min,
            bin_size=bin_size,
            max_bins_per_interval=max_bins_per_interval,
            repartitions_variants=repartitions_variants,
            repartitions_intervals=repartitions_intervals,
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
            use_binned=True,
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
            use_binned=True,
        )
        df_long = convert_from_wide_to_long(
            wide.select("studyLocusId", "geneId", cls.feature_name),
            id_vars=("studyLocusId", "geneId"),
            var_name="featureName",
            value_name="featureValue",
            value_vars=(cls.feature_name,),
        )
        return cls(_df=df_long, _schema=cls.get_schema())
