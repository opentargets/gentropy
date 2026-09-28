"""Tests for the disease-pathway enrichment dataset."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from gentropy.dataset.dataset import Dataset
from gentropy.dataset.pathway_enrichment import PathwayEnrichment


def test_pathway_enrichment_creation(
    mock_pathway_enrichment: PathwayEnrichment,
) -> None:
    """Test that the mock enrichment dataset is a Dataset."""
    assert isinstance(mock_pathway_enrichment, Dataset)


def test_recomputed_adjusted_p_value_keeps_the_published_values(
    mock_pathway_enrichment: PathwayEnrichment,
) -> None:
    """Test that an adjusted p-value that is already there is left alone."""
    observed = {
        row["pathwayFromSourceName"]: row["pValueAdjusted"]
        for row in mock_pathway_enrichment.with_recomputed_adjusted_p_value()
        .df.filter("diseaseId = 'disease1'")
        .collect()
    }
    assert observed == {
        "pathway1 [Reactome]": pytest.approx(0.001),
        "pathway2 [GO BP]": pytest.approx(0.6),
        "pathway3 [GO BP]": pytest.approx(0.9),
    }


def test_recomputed_adjusted_p_value_fills_in_a_missing_one(
    mock_pathway_enrichment: PathwayEnrichment,
) -> None:
    """Test the Benjamini-Hochberg values for a disease whose adjusted p-value is null throughout.

    Its three p-values are 0.001, 0.02 and 0.5 over three tested pathways, so the step-up
    values are 0.001*3/1, 0.02*3/2 and 0.5*3/3.
    """
    observed = {
        row["pathwayFromSourceName"]: row["pValueAdjusted"]
        for row in mock_pathway_enrichment.with_recomputed_adjusted_p_value()
        .df.filter("diseaseId = 'disease3'")
        .collect()
    }
    assert observed == {
        "pathway1 [Reactome]": pytest.approx(0.003),
        "pathway2 [GO BP]": pytest.approx(0.03),
        "pathway3 [GO BP]": pytest.approx(0.5),
    }


def test_recomputed_adjusted_p_value_matches_r_on_ties(spark: SparkSession) -> None:
    """Test that tied p-values get the value R's `p.adjust(method = "BH")` gives them.

    `p.adjust(c(0.04, 0.04, 0.20), method = "BH")` is `c(0.06, 0.06, 0.20)`: the positions of
    tied p-values are consecutive, and the step-up minimum then gives both the same value.
    """
    enrichment = PathwayEnrichment(
        _df=spark.createDataFrame(
            [
                ("disease1", "pathway1", None, 2.0, 0.04, None),
                ("disease1", "pathway2", None, 2.0, 0.04, None),
                ("disease1", "pathway3", None, 1.0, 0.20, None),
            ],
            PathwayEnrichment.get_schema(),
        ),
        _schema=PathwayEnrichment.get_schema(),
    )
    observed = [
        row["pValueAdjusted"]
        for row in enrichment.with_recomputed_adjusted_p_value()
        .df.orderBy("pathwayFromSourceName")
        .collect()
    ]
    assert observed == [
        pytest.approx(0.06),
        pytest.approx(0.06),
        pytest.approx(0.20),
    ]


def test_recomputed_adjusted_p_value_is_monotonic(spark: SparkSession) -> None:
    """Test that a step-up value never falls below the one of a smaller p-value."""
    enrichment = PathwayEnrichment(
        _df=spark.createDataFrame(
            [
                ("disease1", "pathway1", None, 2.0, 0.04, None),
                ("disease1", "pathway2", None, 2.0, 0.04, None),
                ("disease1", "pathway3", None, 1.5, 0.05, None),
                ("disease1", "pathway4", None, 1.0, 0.06, None),
            ],
            PathwayEnrichment.get_schema(),
        ),
        _schema=PathwayEnrichment.get_schema(),
    )
    observed = [
        row["pValueAdjusted"]
        for row in enrichment.with_recomputed_adjusted_p_value()
        .df.orderBy("pathwayFromSourceName")
        .collect()
    ]
    # the raw values are 0.16, 0.08, 0.0667 and 0.06, and the step-up minimum pulls every one
    # of them down to the 0.06 of the largest p-value, as `p.adjust` does
    assert observed == [
        pytest.approx(0.06),
        pytest.approx(0.06),
        pytest.approx(0.06),
        pytest.approx(0.06),
    ]


def test_recomputed_adjusted_p_value_leaves_a_null_p_value_null(
    spark: SparkSession,
) -> None:
    """Test that a pathway with no p-value keeps a null adjusted p-value and is out of `n`.

    R's `p.adjust` returns NA for an NA p-value, and adjusts the rest over the two p-values it
    does have: 0.01*2/1 and 0.04*2/2.
    """
    enrichment = PathwayEnrichment(
        _df=spark.createDataFrame(
            [
                ("disease1", "pathway1", None, 2.0, 0.01, None),
                ("disease1", "pathway2", None, 1.5, 0.04, None),
                ("disease1", "pathway3", None, None, None, None),
            ],
            PathwayEnrichment.get_schema(),
        ),
        _schema=PathwayEnrichment.get_schema(),
    )
    observed = {
        row["pathwayFromSourceName"]: row["pValueAdjusted"]
        for row in enrichment.with_recomputed_adjusted_p_value().df.collect()
    }
    assert observed == {
        "pathway1": pytest.approx(0.02),
        "pathway2": pytest.approx(0.04),
        "pathway3": None,
    }


def test_degenerate_enrichment_is_masked(spark: SparkSession) -> None:
    """Test that a non-finite enrichment score loses its adjusted p-value, published or not.

    The p-value stays, so the pathway still counts as tested.
    """
    enrichment = PathwayEnrichment(
        _df=spark.createDataFrame(
            [
                ("disease1", "pathway1", None, float("inf"), 0.0, 0.0),
                ("disease1", "pathway2", None, float("nan"), 0.0, 0.0),
                ("disease1", "pathway3", None, 2.0, 0.01, 0.02),
                ("disease1", "pathway4", None, None, 0.2, 0.3),
            ],
            PathwayEnrichment.get_schema(),
        ),
        _schema=PathwayEnrichment.get_schema(),
    ).with_degenerate_enrichment_masked()
    observed = {
        row["pathwayFromSourceName"]: (row["pValue"], row["pValueAdjusted"])
        for row in enrichment.df.collect()
    }
    assert observed == {
        "pathway1": (0.0, None),
        "pathway2": (0.0, None),
        "pathway3": (0.01, pytest.approx(0.02)),
        "pathway4": (0.2, pytest.approx(0.3)),
    }
    assert {
        row["pathwayFromSourceName"] for row in enrichment.tested_pathways().collect()
    } == {"pathway1", "pathway2", "pathway3", "pathway4"}
    assert {
        row["pathwayFromSourceName"]
        for row in enrichment.enriched_pathways(0.05).collect()
    } == {"pathway3"}


def test_recomputed_adjusted_p_value_skips_infinite_enrichment(
    spark: SparkSession,
) -> None:
    """Test that a pathway with a non-finite enrichment score is left out of the correction.

    Those rows come with a p-value of exactly zero, so including them would make the most
    degenerate fits the most significant pathways of the disease, and would take a position in
    the ordering away from the rest.
    """
    enrichment = PathwayEnrichment(
        _df=spark.createDataFrame(
            [
                ("disease1", "pathway1", None, float("inf"), 0.0, None),
                ("disease1", "pathway2", None, 2.0, 0.01, None),
                ("disease1", "pathway3", None, 1.5, 0.04, None),
            ],
            PathwayEnrichment.get_schema(),
        ),
        _schema=PathwayEnrichment.get_schema(),
    )
    observed = {
        row["pathwayFromSourceName"]: row["pValueAdjusted"]
        for row in enrichment.with_recomputed_adjusted_p_value().df.collect()
    }
    assert observed == {
        "pathway1": None,
        "pathway2": pytest.approx(0.02),
        "pathway3": pytest.approx(0.04),
    }


def test_tested_pathways(spark: SparkSession) -> None:
    """Test that a pathway with neither a p-value nor an adjusted one is not counted as tested."""
    enrichment = PathwayEnrichment(
        _df=spark.createDataFrame(
            [
                # tested for one disease and missing from the other
                ("disease1", "pathway1", None, 2.0, 0.01, 0.02),
                ("disease2", "pathway1", None, None, None, None),
                # only ever an adjusted p-value
                ("disease1", "pathway2", None, 1.5, None, 0.3),
                # never tested for any disease
                ("disease1", "pathway3", None, None, None, None),
            ],
            PathwayEnrichment.get_schema(),
        ),
        _schema=PathwayEnrichment.get_schema(),
    )
    observed = {
        row["pathwayFromSourceName"] for row in enrichment.tested_pathways().collect()
    }
    assert observed == {"pathway1", "pathway2"}


def test_enriched_pathways(mock_pathway_enrichment: PathwayEnrichment) -> None:
    """Test that only the published adjusted p-values are used when nothing was recomputed."""
    observed = {
        (row["diseaseId"], row["pathwayFromSourceName"])
        for row in mock_pathway_enrichment.enriched_pathways(0.05).collect()
    }
    assert observed == {
        ("disease1", "pathway1 [Reactome]"),
        ("disease2", "pathway2 [GO BP]"),
    }


def test_enriched_pathways_after_recomputation(
    mock_pathway_enrichment: PathwayEnrichment,
) -> None:
    """Test that a disease with no published adjusted p-value contributes once recomputed."""
    observed = {
        (row["diseaseId"], row["pathwayFromSourceName"])
        for row in mock_pathway_enrichment.with_recomputed_adjusted_p_value()
        .enriched_pathways(0.05)
        .collect()
    }
    assert observed == {
        ("disease1", "pathway1 [Reactome]"),
        ("disease2", "pathway2 [GO BP]"),
        ("disease3", "pathway1 [Reactome]"),
        ("disease3", "pathway2 [GO BP]"),
    }
