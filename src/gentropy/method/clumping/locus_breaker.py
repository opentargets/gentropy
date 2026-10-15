"""Locus-breaker clumping: one study locus per block of significant variants.

Window clumping gives every signal a window of fixed width. Locus-breaker
clumping lets the data set the width instead: it keeps the variants that reach
a lenient ``baseline_significance``, and breaks them into blocks wherever two
adjacent variants are more than ``gap`` base pairs apart. Each block becomes one
locus, widened by ``flanking_distance`` on both sides, and led by its most
significant variant.

The algorithm
-------------

Per study and chromosome:

1. **Filter.** Keep the variants with a p-value at most
   ``baseline_significance``. A variant missing its mantissa or exponent cannot
   be ranked, so it is left out.
2. **Break.** Sort by position and start a new block wherever the gap to the
   previous variant is greater than ``gap``, with
   ``gentropy.method.clumping.cluster.cluster``.
3. **Lead.** The most significant variant of a block leads it: smallest
   exponent, then mantissa, with ties broken by position and then variant id. A
   block whose lead is not at least as significant as ``gwas_significance`` is
   dropped.
4. **Bound.** ``locusStart`` is the first position of the block minus
   ``flanking_distance``, cut at 1, the first position of a chromosome;
   ``locusEnd`` is the last position plus ``flanking_distance``.
5. **Split.** A locus wider than ``large_locus_size`` is too large to
   fine-map. Its block is handed to ``window``, a ``WindowClumping``, and
   replaced by the loci it returns: a window around each lead it finds, with
   its own distance and significance. A large locus where window clumping finds
   no lead disappears.

Both thresholds keep a p-value equal to them, like
``gentropy.operation.association.pvalue_filter``. Changes from legacy:

* A tie gave legacy two leads for one block; it now gives one.
* Legacy grouped blocks by their flanked start, so two blocks cut at the start
  of a chromosome merged their ends; each block now keeps its own.
* ``locusStart`` is cut at 1, not 0, as in window clumping.
* Legacy window-clumped the whole genome and kept the leads inside large
  loci, so a stronger signal in a neighbouring locus could explain them away.
  Window clumping now sees only the variants of the large loci.
* Legacy gave a split locus ``large_loci_size // 2`` on each side of its lead;
  it now keeps the window clumping distance.

Example:
    Chromosome 1 breaks into two blocks at the gap of 400 base pairs. The
    variant at 2000 is above the baseline, so it plays no part:

    >>> from gentropy.dataset.summary_statistics import SummaryStatistics
    >>> rows = [
    ...     ("GCST1", "1_1000_A_T", "1", 1000, 0.1, 5000, 1.0, -6, 0.3, 0.01),
    ...     ("GCST1", "1_1100_C_G", "1", 1100, 0.1, 5000, 1.0, -9, 0.3, 0.01),
    ...     ("GCST1", "1_1500_G_A", "1", 1500, 0.1, 5000, 1.0, -8, 0.3, 0.01),
    ...     ("GCST1", "1_2000_T_C", "1", 2000, 0.1, 5000, 1.0, -2, 0.3, 0.01),
    ... ]
    >>> summary_statistics = SummaryStatistics(
    ...     spark.createDataFrame(rows, SummaryStatistics.schema)
    ... )
    >>> method = LocusBreakerClumping(
    ...     baseline_significance=1e-5,
    ...     gap=300,
    ...     gwas_significance=5e-8,
    ...     flanking_distance=100,
    ... )
    >>> loci = method.clump(summary_statistics)
    >>> loci.df.select(
    ...     loci.lead_variant_id, loci.locus_start, loci.locus_end
    ... ).orderBy(loci.locus_start).show()
    +-------------+----------+--------+
    |leadVariantId|locusStart|locusEnd|
    +-------------+----------+--------+
    |   1_1100_C_G|       900|    1200|
    |   1_1500_G_A|      1400|    1600|
    +-------------+----------+--------+
    <BLANKLINE>

    With a ``large_locus_size`` of 250, the first locus, 300 wide, is split. Window
    clumping finds one lead in its block, at 1100, and gives it a window of 50:

    >>> from gentropy.method.clumping.window import WindowClumping
    >>> method = LocusBreakerClumping(
    ...     baseline_significance=1e-5,
    ...     gap=300,
    ...     gwas_significance=5e-8,
    ...     flanking_distance=100,
    ...     large_locus_size=250,
    ...     window=WindowClumping(distance=50, gwas_significance=5e-8),
    ... )
    >>> loci = method.clump(summary_statistics)
    >>> loci.df.select(
    ...     loci.lead_variant_id, loci.locus_start, loci.locus_end
    ... ).orderBy(loci.locus_start).show()
    +-------------+----------+--------+
    |leadVariantId|locusStart|locusEnd|
    +-------------+----------+--------+
    |   1_1100_C_G|      1050|    1150|
    |   1_1500_G_A|      1400|    1600|
    +-------------+----------+--------+
    <BLANKLINE>
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Self, TypeVar

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pydantic import BaseModel, Field

from gentropy.common.stats import split_pvalue
from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.clumping.cluster import cluster
from gentropy.method.clumping.window import WindowClumping
from gentropy.model.core.dataset import Dataset
from gentropy.model.trait.association import Association
from gentropy.model.trait.identifier import HasStudyId
from gentropy.model.trait.locus import VariantLocation
from gentropy.operation.association import pvalue_filter

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

D = TypeVar("D", bound=Dataset[HasStudyId | VariantLocation | Association])


class LocusBreakerClumpingConfig(BaseModel):
    """Parameters of locus-breaker clumping, validated when the method is created.

    Examples:
        >>> LocusBreakerClumpingConfig()
        LocusBreakerClumpingConfig(baseline_significance=1e-05, gap=250000, gwas_significance=1e-08, flanking_distance=100000, large_locus_size=1500000)
        >>> LocusBreakerClumpingConfig(gap=0)  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        pydantic_core._pydantic_core.ValidationError: 1 validation error for LocusBreakerClumpingConfig
        gap
          Input should be greater than 0 ...
    """

    baseline_significance: Annotated[float, Field(gt=0, lt=1)] = 1e-5
    """Largest p-value a variant needs to be part of a block."""
    gap: Annotated[int, Field(gt=0)] = 250_000
    """A gap between adjacent variants greater than this, in base pairs, breaks a block."""
    gwas_significance: Annotated[float, Field(gt=0, lt=1)] = 1e-8
    """Largest p-value the lead of a block needs for the block to become a locus."""
    flanking_distance: Annotated[int, Field(ge=0)] = 100_000
    """Base pairs added on each side of a block."""
    large_locus_size: Annotated[int, Field(gt=0)] = 1_500_000
    """Width above which a locus is split with window clumping."""


class LocusBreakerClumping:
    """Clump summary statistics into study loci shaped by the blocks of significant variants.

    The module docstring describes the algorithm step by step. Create the
    method once with its parameters, then call ``clump`` on each set of summary
    statistics.

    Examples:
        >>> LocusBreakerClumping(gap=100_000).config.gap
        100000
    """

    def __init__(
        self: Self,
        baseline_significance: float = 1e-5,
        gap: int = 250_000,
        gwas_significance: float = 1e-8,
        flanking_distance: int = 100_000,
        large_locus_size: int = 1_500_000,
        window: WindowClumping | None = None,
    ) -> None:
        """Validate the parameters and keep them in ``config``.

        Args:
            baseline_significance (float): Largest p-value a variant needs to be
                part of a block.
            gap (int): A gap between adjacent variants greater than this, in base
                pairs, breaks a block.
            gwas_significance (float): Largest p-value the lead of a block needs
                for the block to become a locus.
            flanking_distance (int): Base pairs added on each side of a block.
            large_locus_size (int): Width above which a locus is split with
                window clumping.
            window (WindowClumping | None): How to split large loci, used as it
                is configured. ``None``, the default, is a window of 500 kb at
                ``gwas_significance``.
        """
        self.config = LocusBreakerClumpingConfig(
            baseline_significance=baseline_significance,
            gap=gap,
            gwas_significance=gwas_significance,
            flanking_distance=flanking_distance,
            large_locus_size=large_locus_size,
        )
        self.window = window or WindowClumping(
            distance=500_000, gwas_significance=gwas_significance
        )

    def clump(
        self: Self,
        associations: Dataset[HasStudyId | VariantLocation | Association],
    ) -> StudyLocus:
        """Make one study locus per block of significant variants, splitting large ones.

        Any dataset with the three traits can be clumped. Columns are read
        through the traits, so the dataset may name them as it likes.

        Args:
            associations (Dataset[HasStudyId | VariantLocation | Association]):
                The variants to clump. ``chromosome`` and ``position`` must be
                filled in; ``variantId`` is not parsed.

        Returns:
            StudyLocus: One row per locus, with ``ldSet`` and ``locus`` left out.
                Loci from blocks carry an empty ``qualityControls``; loci from
                window clumping carry what it gives them.
        """
        baseline = pvalue_filter(associations, self.config.baseline_significance)
        study = HasStudyId.of(baseline)
        location = VariantLocation.of(baseline)
        association = Association.of(baseline)

        variants = (
            baseline.df.select(
                study.study_id.alias(HasStudyId.study_id.name),
                location.chromosome.alias(VariantLocation.chromosome.name),
                location.position.alias(VariantLocation.position.name),
                association.variant_id.alias(Association.variant_id.name),
                association.p_value_mantissa.alias(Association.p_value_mantissa.name),
                association.p_value_exponent.alias(Association.p_value_exponent.name),
            )
            # The filter keeps a null mantissa when the exponent alone passes:
            .filter(
                f.col(Association.p_value_mantissa.name).isNotNull()
                & f.col(Association.p_value_exponent.name).isNotNull()
            )
        )
        flank = self.config.flanking_distance
        blocks = (
            variants.transform(cluster, self.config.gap)
            .transform(self._blocks)
            .filter(
                _at_least_as_significant(
                    f.col("lead")[Association.p_value_mantissa.name],
                    f.col("lead")[Association.p_value_exponent.name],
                    self.config.gwas_significance,
                )
            )
            .withColumns(
                {
                    StudyLocus.locus_start.name: f.greatest(
                        f.lit(1), f.col("firstPosition") - flank
                    ),
                    StudyLocus.locus_end.name: f.col("lastPosition") + flank,
                }
            )
        )
        width = f.col(StudyLocus.locus_end.name) - f.col(StudyLocus.locus_start.name)
        large = width > self.config.large_locus_size
        split = self.window.clump(self._variants_within(baseline, blocks.filter(large)))
        return StudyLocus(
            blocks.filter(~large).transform(self._to_study_locus).unionByName(split.df)
        )

    @staticmethod
    def _variants_within(baseline: D, blocks: DataFrame) -> D:
        """Keep the variants that fall inside one of ``blocks``.

        The blocks were built from these very variants, so a variant between a
        block's first and last position is one of its members.

        Args:
            baseline (D): The variants at the baseline significance.
            blocks (DataFrame): Blocks as ``_blocks`` returns them.

        Returns:
            D: The members of the blocks, as the same dataset type.
        """
        study = HasStudyId.of(baseline)
        location = VariantLocation.of(baseline)
        spans = blocks.select(
            f.col(HasStudyId.study_id.name).alias("blockStudyId"),
            f.col(VariantLocation.chromosome.name).alias("blockChromosome"),
            "firstPosition",
            "lastPosition",
        )
        return type(baseline)(
            baseline.df.join(
                spans,
                (study.study_id == f.col("blockStudyId"))
                & (location.chromosome == f.col("blockChromosome"))
                & location.position.between(
                    f.col("firstPosition"), f.col("lastPosition")
                ),
                "semi",
            )
        )

    @staticmethod
    def _blocks(df: DataFrame) -> DataFrame:
        """Collapse each cluster into one row with its span and its lead.

        Structs compare field by field, so the smallest of ``(exponent,
        mantissa, position, variantId)`` is the most significant variant, with
        ties broken by position and then variant id.

        Args:
            df (DataFrame): Variants with ``studyId``, ``chromosome``,
                ``clusterId``, ``position``, ``variantId``, ``pValueMantissa`` and
                ``pValueExponent``.

        Returns:
            DataFrame: One row per cluster with ``studyId``, ``chromosome``,
                ``firstPosition``, ``lastPosition`` and ``lead``, a struct with
                the lead's p-value, ``position`` and ``variantId``.

        Examples:
            >>> df = spark.createDataFrame(
            ...     [("GCST1", "1", 1, "1_10_A_T", 10, 1.0, -9), ("GCST1", "1", 1, "1_20_C_G", 20, 1.0, -9)],
            ...     "studyId string, chromosome string, clusterId long, variantId string, "
            ...     "position int, pValueMantissa float, pValueExponent int",
            ... )
            >>> LocusBreakerClumping._blocks(df).select(
            ...     "firstPosition", "lastPosition", "lead.variantId"
            ... ).show()
            +-------------+------------+---------+
            |firstPosition|lastPosition|variantId|
            +-------------+------------+---------+
            |           10|          20| 1_10_A_T|
            +-------------+------------+---------+
            <BLANKLINE>
        """
        position = VariantLocation.position.name
        return df.groupBy(
            HasStudyId.study_id.name, VariantLocation.chromosome.name, "clusterId"
        ).agg(
            f.min(position).alias("firstPosition"),
            f.max(position).alias("lastPosition"),
            f.min(
                f.struct(
                    Association.p_value_exponent.name,
                    Association.p_value_mantissa.name,
                    position,
                    Association.variant_id.name,
                )
            ).alias("lead"),
        )

    @staticmethod
    def _to_study_locus(df: DataFrame) -> DataFrame:
        """Build the study locus columns from each bounded block.

        Args:
            df (DataFrame): Blocks as ``_blocks`` returns them, with
                ``locusStart`` and ``locusEnd``.

        Returns:
            DataFrame: The ``StudyLocus`` columns, without ``ldSet``.

        Examples:
            >>> df = spark.createDataFrame(
            ...     [("GCST1", "1", 1, 400, ("1_200_A_T",))],
            ...     "studyId string, chromosome string, locusStart int, "
            ...     "locusEnd int, lead struct<variantId: string>",
            ... )
            >>> LocusBreakerClumping._to_study_locus(df).drop("id").show()
            +-------+----------+----------+--------+-------------+---------------+
            |studyId|chromosome|locusStart|locusEnd|leadVariantId|qualityControls|
            +-------+----------+----------+--------+-------------+---------------+
            |  GCST1|         1|         1|     400|    1_200_A_T|             []|
            +-------+----------+----------+--------+-------------+---------------+
            <BLANKLINE>
        """
        return (
            df.select(
                HasStudyId.study_id.name,
                VariantLocation.chromosome.name,
                StudyLocus.locus_start.name,
                StudyLocus.locus_end.name,
                f.col("lead")[Association.variant_id.name].alias(
                    StudyLocus.lead_variant_id.name
                ),
                f.array()
                .cast(t.ArrayType(t.StringType()))
                .alias(StudyLocus.quality_controls.name),
            )
            .withColumn(
                StudyLocus.id.name,
                StudyLocus.generate_identifier(StudyLocus.ID_COLUMNS),
            )
            .select(
                StudyLocus.id.name,
                StudyLocus.study_id.name,
                StudyLocus.chromosome.name,
                StudyLocus.locus_start.name,
                StudyLocus.locus_end.name,
                StudyLocus.lead_variant_id.name,
                StudyLocus.quality_controls.name,
            )
        )


def _at_least_as_significant(
    mantissa: Column, exponent: Column, pvalue: float
) -> Column:
    """Whether a p-value is at most ``pvalue``, compared as ``pvalue_filter`` does.

    Args:
        mantissa (Column): The p-value's base-10 mantissa.
        exponent (Column): The p-value's base-10 exponent.
        pvalue (float): The threshold, kept when equal.

    Returns:
        Column: True when the p-value passes.

    Examples:
        >>> df = spark.createDataFrame([(1.0, -8), (5.0, -8), (5.1, -8)], "m float, e int")
        >>> [row.ok for row in df.select(_at_least_as_significant(df.m, df.e, 5e-8).alias("ok")).collect()]
        [True, True, False]
    """
    threshold_mantissa, threshold_exponent = split_pvalue(pvalue)
    return (exponent < threshold_exponent) | (
        (exponent == threshold_exponent) & (mantissa <= threshold_mantissa)
    )
