"""Tests for PICS fine-mapping."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from gentropy.dataset.study_locus import LDAnnotatedStudyLocus
from gentropy.method.fine_mapping.pics import PICS, _relative_likelihoods

LEAD = "1_100_A_T"


def _likelihoods(
    spark: SparkSession, rows: list[tuple[float, list[float]]], k: float = 6.4
) -> list[list[float]]:
    """Run the likelihood UDF over (lead -log10 p, r2 of each tag) rows.

    Args:
        spark (SparkSession): Spark session.
        rows (list[tuple[float, list[float]]]): One tuple per locus.
        k (float): PICS constant.

    Returns:
        list[list[float]]: The relative likelihood of each tag, per row, in order.
    """
    df = spark.createDataFrame(
        [(i, *row) for i, row in enumerate(rows)],
        "row int, neglogP double, r2 array<double>",
    )
    return [
        row.likelihoods
        for row in df.select(
            "row",
            _relative_likelihoods(f.col("neglogP"), f.col("r2"), f.lit(k)).alias(
                "likelihoods"
            ),
        )
        .orderBy("row")
        .collect()
    ]


@pytest.mark.legacy
def test_likelihoods_normalise_to_dev_posteriors(spark: SparkSession) -> None:
    """Dev's case: a tag at r2 0.8 and one at r2 1.0, with the lead at -log10 p 10."""
    [likelihoods] = _likelihoods(spark, [(10.0, [0.8, 1.0])])

    total = sum(likelihoods)
    assert [value / total for value in likelihoods] == pytest.approx(
        [0.07116959886882368, 0.9288304011311763]
    )


def test_a_perfect_proxy_is_as_likely_as_the_lead(spark: SparkSession) -> None:
    """At r2 1.0 the spread is clamped, so the likelihood is that of the lead, 1."""
    [likelihoods] = _likelihoods(spark, [(10.0, [1.0])])

    assert likelihoods == pytest.approx([1.0])


def test_rows_of_different_lengths_stay_aligned(spark: SparkSession) -> None:
    """A batch of uneven arrays returns each row's likelihoods with its own tags."""
    observed = _likelihoods(spark, [(10.0, [1.0, 0.8]), (10.0, []), (10.0, [0.8])])

    assert [len(row) for row in observed] == [2, 0, 1]
    assert observed[0][1] == pytest.approx(observed[2][0])


def _loci(
    spark: SparkSession,
    tags: list[tuple[str, float | None]],
    lead_exponent: int | None = -10,
) -> LDAnnotatedStudyLocus:
    """One LD annotated locus whose lead has the given tags.

    Args:
        spark (SparkSession): Spark session.
        tags (list[tuple[str, float | None]]): ``ldSet`` as (tag, r2) pairs.
        lead_exponent (int | None): The lead's p-value exponent, with mantissa
            1.0; ``None`` leaves the lead out of ``locus``.

    Returns:
        LDAnnotatedStudyLocus: The locus.
    """
    locus = (
        [(LEAD, 1.0, lead_exponent, 0.3, 1000, 0.1, 0.01)]
        if lead_exponent is not None
        else []
    )
    return LDAnnotatedStudyLocus(
        spark.createDataFrame(
            [("L1", "S1", "1", 1, 1000, LEAD, [], locus, tags)],
            LDAnnotatedStudyLocus.schema,
        )
    )


def _members(pics: PICS, loci: LDAnnotatedStudyLocus) -> list[tuple[str, float]]:
    """The (variantId, posteriorProbability) members of the only fine-mapped locus.

    Args:
        pics (PICS): The method.
        loci (LDAnnotatedStudyLocus): Loci holding one locus.

    Returns:
        list[tuple[str, float]]: Members in their stored order.
    """
    [row] = pics._posteriors(loci).collect()
    return [(m.variantId, m.posteriorProbability) for m in row.members]


@pytest.mark.legacy
def test_posteriors_match_dev(spark: SparkSession) -> None:
    """Dev's case, ordered by posterior; both variants make the 95% set."""
    members = _members(PICS(), _loci(spark, [("tag", 0.8), (LEAD, 1.0)]))

    assert [variant for variant, _ in members] == [LEAD, "tag"]
    assert [pp for _, pp in members] == pytest.approx(
        [0.9288304011311763, 0.07116959886882368]
    )


def test_tags_below_r2_half_or_without_r2_are_left_out(spark: SparkSession) -> None:
    """PICS holds only in strong LD, so weak and unknown tags are not members."""
    loci = _loci(spark, [(LEAD, 1.0), ("weak", 0.4), ("unknown", None)])

    [(variant, posterior)] = _members(PICS(), loci)

    assert variant == LEAD
    assert posterior == pytest.approx(1.0)


def test_variants_are_kept_until_coverage_is_reached(spark: SparkSession) -> None:
    """A variant is kept while the posterior before it is below the coverage."""
    loci = _loci(spark, [("tag", 0.8), (LEAD, 1.0)])

    assert [v for v, _ in _members(PICS(coverage=0.9), loci)] == [LEAD]
    assert [v for v, _ in _members(PICS(coverage=0.95), loci)] == [LEAD, "tag"]


def test_equal_posteriors_are_ordered_by_variant(spark: SparkSession) -> None:
    """Perfect proxies tie with the lead; the cut keeps the first by variant id."""
    loci = _loci(spark, [("1_200_C_G", 1.0), (LEAD, 1.0), ("1_050_G_C", 1.0)])

    members = _members(PICS(coverage=0.5), loci)

    assert [v for v, _ in members] == ["1_050_G_C", LEAD]
    assert [pp for _, pp in members] == pytest.approx([1 / 3, 1 / 3])


def test_a_lead_without_p_value_gives_no_credible_set(spark: SparkSession) -> None:
    """Without the lead's own statistics there is nothing to fine-map from."""
    loci = _loci(spark, [(LEAD, 1.0)], lead_exponent=None)

    assert PICS()._posteriors(loci).count() == 0


def test_an_empty_ld_set_gives_no_credible_set(spark: SparkSession) -> None:
    """LD clumped loci keep an empty ``ldSet`` and have no members."""
    assert PICS()._posteriors(_loci(spark, [])).count() == 0
