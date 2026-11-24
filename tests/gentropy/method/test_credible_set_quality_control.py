"""Tests for credible set quality control."""

from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import SparkSession

from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.method.fine_mapping.credible_set_qc import credible_set_qc


def _credible_set(
    id: str,
    lead: str,
    chromosome: str = "1",
    locus_start: int = 1,
    log_bayes_factor: float = 5.0,
) -> tuple[Any, ...]:
    """A credible set row of study S1.

    Args:
        id (str): Credible set id.
        lead (str): Lead variant id.
        chromosome (str): Chromosome.
        locus_start (int): Locus start; the locus ends 999 bp later.
        log_bayes_factor (float): Credible set log Bayes factor.

    Returns:
        tuple[Any, ...]: The row, in the CredibleSet schema order.
    """
    return (
        id,
        chromosome,
        locus_start,
        locus_start + 999,
        "S1",
        "gwas",
        lead,
        "SuSiE-inf",
        log_bayes_factor,
        None,
        None,
        0.5,
        [],
    )


def _variant(id: str, variant: str, exponent: int | None) -> tuple[Any, ...]:
    """A credible set variant row.

    Args:
        id (str): Credible set id.
        variant (str): Variant id.
        exponent (int | None): p-value exponent, with mantissa 1; None for no p-value.

    Returns:
        tuple[Any, ...]: The row, in the CredibleSetVariant schema order.
    """
    mantissa = None if exponent is None else 1.0
    return (
        id,
        variant,
        mantissa,
        exponent,
        None,
        None,
        0.1,
        0.01,
        0.5,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        [],
    )


@pytest.fixture
def credible_sets(spark: SparkSession) -> tuple[CredibleSet, CredibleSetVariant]:
    """Credible sets A and B, whose leads are in LD, and C on another chromosome.

    Args:
        spark (SparkSession): Spark session.

    Returns:
        tuple[CredibleSet, CredibleSetVariant]: The credible sets and their variants.
    """
    return (
        CredibleSet(
            spark.createDataFrame(
                [
                    _credible_set("A", "1_10_A_T"),
                    _credible_set("B", "1_20_C_G"),
                    _credible_set("C", "2_10_G_A", chromosome="2"),
                ],
                CredibleSet.get_schema(),
            )
        ),
        CredibleSetVariant(
            spark.createDataFrame(
                [
                    _variant("A", "1_10_A_T", -20),
                    _variant("A", "1_11_A_T", None),
                    _variant("B", "1_20_C_G", -10),
                    _variant("B", "1_21_C_G", -9),
                    _variant("C", "2_10_G_A", -10),
                ],
                CredibleSetVariant.get_schema(),
            )
        ),
    )


def test_ld_clumping_drops_the_less_significant_lead(
    spark: SparkSession, credible_sets: tuple[CredibleSet, CredibleSetVariant]
) -> None:
    """B's lead is in LD with the more significant lead of A, so B goes with its variants."""
    study_index = StudyIndex(
        spark.createDataFrame(
            [("S1", "GCST", "gwas", [("nfe", 1.0)])],
            "studyId string, projectId string, studyType string, "
            "ldPopulationStructure array<struct<ldPopulation: string, "
            "relativeSampleSize: double>>",
        )
    )
    ld_index = LDIndex(
        spark.createDataFrame(
            [
                (
                    "1_10_A_T",
                    "1",
                    [("1_10_A_T", [("nfe", 1.0)]), ("1_20_C_G", [("nfe", 0.95)])],
                ),
                (
                    "1_20_C_G",
                    "1",
                    [("1_20_C_G", [("nfe", 1.0)]), ("1_10_A_T", [("nfe", 0.95)])],
                ),
                ("2_10_G_A", "2", [("2_10_G_A", [("nfe", 1.0)])]),
            ],
            LDIndex.get_schema(),
        )
    )

    kept, variants = credible_set_qc(
        *credible_sets, ld_index=ld_index, study_index=study_index, ld_min_r2=0.8
    )

    assert sorted(row.id for row in kept.df.collect()) == ["A", "C"]
    assert sorted(row.variantId for row in variants.df.collect()) == [
        "1_10_A_T",
        "1_11_A_T",
        "2_10_G_A",
    ]


def test_without_ld_index_nothing_is_clumped(
    credible_sets: tuple[CredibleSet, CredibleSetVariant],
) -> None:
    """Without an LD index, credible sets in LD are all kept."""
    kept, variants = credible_set_qc(*credible_sets)
    assert sorted(row.id for row in kept.df.collect()) == ["A", "B", "C"]
    assert variants.df.count() == 5


def test_lead_without_pvalue_does_not_pass(
    spark: SparkSession, credible_sets: tuple[CredibleSet, CredibleSetVariant]
) -> None:
    """A lead with no reported p-value, such as an imputed one, fails the threshold."""
    credible_set_df, variants = credible_sets
    imputed_lead = CredibleSetVariant(
        variants.df.filter(
            "credibleSetId != 'A' OR variantId != '1_10_A_T'"
        ).unionByName(
            spark.createDataFrame(
                [_variant("A", "1_10_A_T", None)], CredibleSetVariant.get_schema()
            )
        )
    )
    kept, _ = credible_set_qc(credible_set_df, imputed_lead)
    assert sorted(row.id for row in kept.df.collect()) == ["B", "C"]


def test_ld_index_requires_study_index(
    spark: SparkSession, credible_sets: tuple[CredibleSet, CredibleSetVariant]
) -> None:
    """Clumping without the studies' ancestries is refused."""
    ld_index = LDIndex(spark.createDataFrame([], LDIndex.get_schema()))
    with pytest.raises(ValueError, match="requires the study index"):
        credible_set_qc(*credible_sets, ld_index=ld_index)


def test_same_lead_from_overlapping_loci_is_kept_once(spark: SparkSession) -> None:
    """A lead fine-mapped in two overlapping windows keeps the credible set with the higher Bayes factor."""
    credible_sets = CredibleSet(
        _df=spark.createDataFrame(
            [
                _credible_set("W1", "1_500_A_T", locus_start=1, log_bayes_factor=4.0),
                _credible_set("W2", "1_500_A_T", locus_start=400, log_bayes_factor=6.0),
            ],
            CredibleSet.get_schema(),
        ),
        _schema=CredibleSet.get_schema(),
    )
    variants = CredibleSetVariant(
        _df=spark.createDataFrame(
            [_variant(id, "1_500_A_T", -10) for id in ("W1", "W2")],
            CredibleSetVariant.get_schema(),
        ),
        _schema=CredibleSetVariant.get_schema(),
    )
    kept, kept_variants = credible_set_qc(credible_sets, variants)
    assert [row.id for row in kept.df.collect()] == ["W2"]
    assert [row.credibleSetId for row in kept_variants.df.collect()] == ["W2"]
