"""Tests for locus-breaker clumping."""

from __future__ import annotations

from typing import TypeAlias

import pytest
from pyspark.sql import SparkSession

from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.clumping.locus_breaker import LocusBreakerClumping
from gentropy.method.clumping.window import WindowClumping

Association: TypeAlias = tuple[str, str, int, float | None, int | None]
"""Study, chromosome, position, p-value mantissa and exponent."""

Locus: TypeAlias = tuple[str, str, int, int, str]
"""Study, chromosome, locus start, locus end and lead variant."""


def _summary_statistics(
    spark: SparkSession, rows: list[Association]
) -> SummaryStatistics:
    """Summary statistics named ``chromosome_position``, other statistics as placeholders.

    Args:
        spark (SparkSession): Spark session.
        rows (list[Association]): The associations.

    Returns:
        SummaryStatistics: The dataset.
    """
    return SummaryStatistics(
        spark.createDataFrame(
            [
                (study, f"{chromosome}_{position}", chromosome, position)
                + (0.1, 5000, mantissa, exponent, 0.3, 0.01)
                for study, chromosome, position, mantissa, exponent in rows
            ],
            SummaryStatistics.schema,
        )
    )


def _loci(loci: StudyLocus) -> list[Locus]:
    """The loci as sorted tuples.

    Args:
        loci (StudyLocus): Clumping result.

    Returns:
        list[Locus]: One tuple per locus.
    """
    return sorted(
        tuple(row)
        for row in loci.df.select(
            loci.study_id,
            loci.chromosome,
            loci.locus_start,
            loci.locus_end,
            loci.lead_variant_id,
        ).collect()
    )


def _clump(
    spark: SparkSession,
    rows: list[Association],
    flanking_distance: int = 2,
    large_locus_size: int = 1_000,
    window: WindowClumping | None = None,
) -> StudyLocus:
    """Clump with a gap of 3 and flanks of 2; by default every block is small.

    Args:
        spark (SparkSession): Spark session.
        rows (list[Association]): The associations.
        flanking_distance (int): Base pairs added on each side of a block.
        large_locus_size (int): Width above which a locus is split.
        window (WindowClumping | None): Window clumping for large loci.

    Returns:
        StudyLocus: The loci.
    """
    method = LocusBreakerClumping(
        baseline_significance=0.05,
        gap=3,
        gwas_significance=1e-3,
        flanking_distance=flanking_distance,
        large_locus_size=large_locus_size,
        window=window,
    )
    return method.clump(_summary_statistics(spark, rows))


def test_each_block_becomes_one_locus_led_by_its_top_variant(
    spark: SparkSession,
) -> None:
    """The legacy example: blocks split on gaps above 3, per study and chromosome.

    Variants above the baseline p-value are left out before the blocks are found,
    and a block whose top variant misses ``gwas_significance`` is dropped. Unlike
    legacy, the first locus starts at 1, the first position of a chromosome.
    """
    rows: list[Association] = [
        ("s1", "1", 1, 1.0, -2),
        ("s1", "1", 2, 1.0, -4),
        ("s1", "1", 3, 1.0, -5),
        ("s1", "1", 4, 1.0, -1),  # above the baseline
        ("s1", "1", 7, 1.0, -4),
        ("s1", "1", 8, 1.0, -2),
        ("s1", "2", 8, 1.0, -2),
        ("s1", "2", 9, 1.0, -3),
        ("s1", "2", 10, 1.0, -5),
        ("s1", "2", 14, 1.0, -2),  # a block whose top is not significant
        ("s2", "2", 8, 1.0, -2),
        ("s2", "2", 9, 1.0, -3),
        ("s2", "2", 10, 1.0, -6),
        ("s2", "2", 14, 1.0, -5),
    ]

    assert _loci(_clump(spark, rows)) == [
        ("s1", "1", 1, 5, "1_3"),
        ("s1", "1", 5, 10, "1_7"),
        ("s1", "2", 6, 12, "2_10"),
        ("s2", "2", 6, 12, "2_10"),
        ("s2", "2", 12, 16, "2_14"),
    ]


def test_loci_are_leads_with_an_identifier_and_no_flags(spark: SparkSession) -> None:
    """Every locus has its own ``id`` and an empty ``qualityControls``."""
    rows: list[Association] = [("s1", "1", 10, 1.0, -5), ("s1", "1", 20, 1.0, -5)]

    loci = _clump(spark, rows)

    assert loci.df.select(loci.id).distinct().count() == 2
    assert [row[0] for row in loci.df.select(loci.quality_controls).collect()] == [
        [],
        [],
    ]


def test_equal_p_values_give_one_lead_by_position(spark: SparkSession) -> None:
    """A tie is broken by position, so a block never has two leads."""
    rows: list[Association] = [("s1", "1", 11, 1.0, -5), ("s1", "1", 10, 1.0, -5)]

    assert _loci(_clump(spark, rows)) == [("s1", "1", 8, 13, "1_10")]


def test_blocks_cut_at_the_chromosome_start_keep_their_own_ends(
    spark: SparkSession,
) -> None:
    """Two blocks that both start at 1 after flanking stay two loci.

    Legacy grouped by the flanked start, so both took the end of the second.
    """
    rows: list[Association] = [("s1", "1", 1, 1.0, -5), ("s1", "1", 6, 1.0, -5)]

    assert _loci(_clump(spark, rows, flanking_distance=10)) == [
        ("s1", "1", 1, 11, "1_1"),
        ("s1", "1", 1, 16, "1_6"),
    ]


def test_a_lead_at_the_significance_threshold_is_kept(spark: SparkSession) -> None:
    """Both thresholds keep a p-value equal to them, like ``pvalue_filter``."""
    rows: list[Association] = [("s1", "1", 10, 1.0, -3), ("s1", "1", 20, 5.0, -2)]

    assert _loci(_clump(spark, rows)) == [("s1", "1", 8, 12, "1_10")]


@pytest.mark.parametrize(
    ("mantissa", "exponent"), [(None, None), (None, -20), (1.0, None)]
)
def test_variants_without_a_p_value_neither_lead_nor_widen_a_block(
    spark: SparkSession, mantissa: float | None, exponent: int | None
) -> None:
    """A variant without a full p-value cannot be ranked, so it is left out."""
    rows: list[Association] = [
        ("s1", "1", 10, mantissa, exponent),
        ("s1", "1", 12, 1.0, -5),
    ]

    assert _loci(_clump(spark, rows)) == [("s1", "1", 10, 14, "1_12")]


def _large_block(strong: dict[int, int]) -> list[Association]:
    """A block from 10 to 30 every 2 base pairs, 8 to 32 once flanked.

    Args:
        strong (dict[int, int]): Exponents of the significant positions; the
            others only reach the baseline.

    Returns:
        list[Association]: The block.
    """
    return [
        ("s1", "1", position, 1.0, strong.get(position, -2))
        for position in range(10, 31, 2)
    ]


def test_a_large_locus_is_split_into_windows_around_its_signals(
    spark: SparkSession,
) -> None:
    """A locus wider than ``large_locus_size`` becomes the window clumping loci inside it.

    Each split locus spans the window clumping distance on each side of its
    lead; a small block next to it stays as it is.
    """
    rows = [*_large_block({12: -6, 26: -5}), ("s1", "1", 100, 1.0, -4)]

    loci = _clump(
        spark,
        rows,
        large_locus_size=10,
        window=WindowClumping(distance=4, gwas_significance=1e-3),
    )

    assert _loci(loci) == [
        ("s1", "1", 8, 16, "1_12"),
        ("s1", "1", 22, 30, "1_26"),
        ("s1", "1", 98, 102, "1_100"),
    ]


def test_a_locus_as_wide_as_the_limit_is_not_split(spark: SparkSession) -> None:
    """Only a locus strictly wider than ``large_locus_size`` is split."""
    rows: list[Association] = [
        ("s1", "1", 10, 1.0, -4),
        ("s1", "1", 13, 1.0, -2),
        ("s1", "1", 16, 1.0, -5),
    ]

    loci = _clump(
        spark,
        rows,
        large_locus_size=10,
        window=WindowClumping(distance=4, gwas_significance=1e-3),
    )

    assert _loci(loci) == [("s1", "1", 8, 18, "1_16")]


def test_a_stronger_signal_outside_a_large_locus_does_not_suppress_its_leads(
    spark: SparkSession,
) -> None:
    """Window clumping only sees the variants of the large loci.

    The signal at 40 is a locus of its own. Legacy clumped the whole genome, so
    it would have explained away the weaker lead at 28, 12 base pairs from it.
    """
    rows = [*_large_block({28: -5}), ("s1", "1", 40, 1.0, -9)]

    loci = _clump(
        spark,
        rows,
        large_locus_size=10,
        window=WindowClumping(distance=20, gwas_significance=1e-3),
    )

    assert _loci(loci) == [("s1", "1", 8, 48, "1_28"), ("s1", "1", 38, 42, "1_40")]


def test_window_clumping_defaults_to_the_gwas_significance(
    spark: SparkSession,
) -> None:
    """Without a window method, large loci are split 500 kb around leads at ``gwas_significance``."""
    rows = _large_block({12: -6})

    assert _loci(_clump(spark, rows, large_locus_size=10)) == [
        ("s1", "1", 1, 500_012, "1_12")
    ]
