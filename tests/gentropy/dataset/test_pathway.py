"""Tests for the pathway library and disease-pathway enrichment datasets."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from gentropy.common.session import Session
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.pathway_enrichment import PathwayEnrichment
from gentropy.dataset.pathway_index import PathwayIndex


def test_pathway_index_creation(mock_pathway_index: PathwayIndex) -> None:
    """Test that the mock pathway index is a Dataset."""
    assert isinstance(mock_pathway_index, Dataset)


def test_pathway_enrichment_creation(
    mock_pathway_enrichment: PathwayEnrichment,
) -> None:
    """Test that the mock enrichment dataset is a Dataset."""
    assert isinstance(mock_pathway_enrichment, Dataset)


def test_pathway_index_from_gmt(session: Session, tmp_path: Path) -> None:
    """Test that a GMT file is parsed into pathways, sources and de-duplicated gene sets."""
    gmt = tmp_path / "library.gmt"
    gmt.write_text(
        "pathway1 [Reactome]\tdescription\tGENE1\tGENE2\tGENE1\n"
        "\n"
        "pathway2 [GO BP]\t\tGENE3\n",
        encoding="utf-8",
    )

    observed = {
        row["pathway"]: (row["source"], row["geneSymbols"])
        for row in PathwayIndex.from_gmt(session, str(gmt)).df.collect()
    }
    assert observed == {
        "pathway1 [Reactome]": ("Reactome", ["GENE1", "GENE2"]),
        "pathway2 [GO BP]": ("GO BP", ["GENE3"]),
    }


def test_gene_membership(mock_pathway_index: PathwayIndex) -> None:
    """Test that gene sets are exploded into one row per pathway and gene."""
    observed = {
        (row["pathway"], row["geneSymbol"])
        for row in mock_pathway_index.gene_membership().collect()
    }
    assert observed == {
        ("pathway1 [Reactome]", "GENE1"),
        ("pathway1 [Reactome]", "GENE2"),
        ("pathway2 [GO BP]", "GENE1"),
        ("pathway2 [GO BP]", "GENE3"),
        ("pathway3 [GO BP]", "GENE2"),
    }


def test_with_corrected_fdr_keeps_the_published_values(
    mock_pathway_enrichment: PathwayEnrichment,
) -> None:
    """Test that an FDR that is already there is left alone."""
    observed = {
        row["pathway"]: row["fdr"]
        for row in mock_pathway_enrichment.with_corrected_fdr()
        .df.filter("diseaseId = 'disease1'")
        .collect()
    }
    assert observed == {
        "pathway1 [Reactome]": pytest.approx(0.001),
        "pathway2 [GO BP]": pytest.approx(0.6),
        "pathway3 [GO BP]": pytest.approx(0.9),
    }


def test_with_corrected_fdr_fills_in_a_missing_one(
    mock_pathway_enrichment: PathwayEnrichment,
) -> None:
    """Test the Benjamini-Hochberg values for a disease whose FDR is null throughout.

    Its three p-values are 0.001, 0.02 and 0.5 over three tested pathways, so the step-up
    values are 0.001*3/1, 0.02*3/2 and 0.5*3/3.
    """
    observed = {
        row["pathway"]: row["fdr"]
        for row in mock_pathway_enrichment.with_corrected_fdr()
        .df.filter("diseaseId = 'disease3'")
        .collect()
    }
    assert observed == {
        "pathway1 [Reactome]": pytest.approx(0.003),
        "pathway2 [GO BP]": pytest.approx(0.03),
        "pathway3 [GO BP]": pytest.approx(0.5),
    }


def test_with_corrected_fdr_is_monotonic(spark: SparkSession) -> None:
    """Test that a step-up value never falls below the one of a smaller p-value."""
    enrichment = PathwayEnrichment(
        _df=spark.createDataFrame(
            [
                ("disease1", "pathway1", None, None, 0.04, None),
                ("disease1", "pathway2", None, None, 0.04, None),
                ("disease1", "pathway3", None, None, 0.05, None),
                ("disease1", "pathway4", None, None, 0.06, None),
            ],
            PathwayEnrichment.get_schema(),
        ),
        _schema=PathwayEnrichment.get_schema(),
    )
    observed = [
        row["fdr"]
        for row in enrichment.with_corrected_fdr().df.orderBy("pathway").collect()
    ]
    # ties share a rank, and 0.06*4/4 = 0.06 pulls every earlier value down to at most 0.06
    assert observed == [
        pytest.approx(0.06),
        pytest.approx(0.06),
        pytest.approx(0.06),
        pytest.approx(0.06),
    ]


def test_enriched_pathways_uses_the_repaired_fdr(
    mock_pathway_enrichment: PathwayEnrichment,
) -> None:
    """Test that a disease with no published FDR still contributes enriched pathways."""
    observed = {
        (row["diseaseId"], row["pathway"])
        for row in mock_pathway_enrichment.enriched_pathways(0.05).collect()
    }
    assert observed == {
        ("disease1", "pathway1 [Reactome]"),
        ("disease2", "pathway2 [GO BP]"),
        ("disease3", "pathway1 [Reactome]"),
        ("disease3", "pathway2 [GO BP]"),
    }
