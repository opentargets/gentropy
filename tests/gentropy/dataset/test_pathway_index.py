"""Tests for the pathway library dataset."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from gentropy.common.session import Session
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.pathway_index import PathwayIndex
from gentropy.dataset.target_index import TargetIndex


def test_pathway_index_creation(mock_pathway_index: PathwayIndex) -> None:
    """Test that the mock pathway index is a Dataset."""
    assert isinstance(mock_pathway_index, Dataset)


def test_pathway_index_from_gmt(session: Session, tmp_path: Path) -> None:
    """Test that a GMT file is parsed into pathways, identifiers and de-duplicated gene sets."""
    gmt = tmp_path / "library.gmt"
    gmt.write_text(
        "pathway1 [Reactome]\tR-HSA-1\tGENE1\t GENE2 \tGENE1\n"
        "\n"
        "pathway2 [GO BP]\tGO:0000002\tGENE3\n"
        "pathway3\t\tGENE4\n",
        encoding="utf-8",
    )

    observed = {
        row["pathway"]: (
            row["pathwayId"],
            row["source"],
            row["geneSymbols"],
            row["geneIds"],
        )
        for row in PathwayIndex.from_gmt(session, str(gmt)).df.collect()
    }
    assert observed == {
        "pathway1 [Reactome]": ("R-HSA-1", "Reactome", ["GENE1", "GENE2"], None),
        "pathway2 [GO BP]": ("GO:0000002", "GO BP", ["GENE3"], None),
        # no identifier and no bracketed source tag, so both come back null
        "pathway3": (None, None, ["GENE4"], None),
    }


def test_resolve_gene_ids(
    session: Session, tmp_path: Path, spark: SparkSession
) -> None:
    """Test that gene symbols are resolved through approved and obsolete symbols."""
    gmt = tmp_path / "library.gmt"
    gmt.write_text(
        "pathway1 [Reactome]\tR-HSA-1\tGENE1\tOLDGENE2\n"
        "pathway2 [GO BP]\tGO:0000002\tNOSUCHGENE\n",
        encoding="utf-8",
    )
    target_index = TargetIndex(
        _df=spark.createDataFrame(
            # gene2 was renamed at some point, so OLDGENE2 is one of its obsolete symbols
            [
                ("gene1", "GENE1", "protein_coding", "1", 1000, []),
                ("gene2", "GENE2", "protein_coding", "1", 2000, ["OLDGENE2"]),
            ],
            "id string, approvedSymbol string, biotype string, chromosome string, "
            "tss long, obsoleteLabels array<string>",
        ).select(
            "id",
            "approvedSymbol",
            "biotype",
            f.struct(f.col("chromosome")).alias("genomicLocation"),
            "tss",
            f.transform(
                f.col("obsoleteLabels"),
                lambda label: f.struct(
                    label.alias("label"), f.lit("obsolete").alias("source")
                ),
            ).alias("obsoleteSymbols"),
        ),
        _schema=TargetIndex.get_schema(),
    )

    observed = {
        row["pathway"]: row["geneIds"]
        for row in PathwayIndex.from_gmt(session, str(gmt))
        .resolve_gene_ids(target_index)
        .df.collect()
    }
    # GENE1 is an approved symbol, OLDGENE2 an obsolete one of gene2, NOSUCHGENE neither
    assert observed == {
        "pathway1 [Reactome]": ["gene1", "gene2"],
        "pathway2 [GO BP]": None,
    }


def test_gene_membership(mock_pathway_index: PathwayIndex) -> None:
    """Test that gene sets are exploded into one row per pathway and gene identifier."""
    observed = {
        (row["pathway"], row["geneId"])
        for row in mock_pathway_index.gene_membership().collect()
    }
    assert observed == {
        ("pathway1 [Reactome]", "gene1"),
        ("pathway1 [Reactome]", "gene2"),
        ("pathway2 [GO BP]", "gene1"),
        ("pathway2 [GO BP]", "gene3"),
        ("pathway3 [GO BP]", "gene2"),
    }


def test_gene_membership_without_resolved_ids(session: Session, tmp_path: Path) -> None:
    """Test that gene membership is refused on an index whose identifiers are not resolved."""
    gmt = tmp_path / "library.gmt"
    gmt.write_text("pathway1 [Reactome]\tR-HSA-1\tGENE1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="no gene identifiers"):
        PathwayIndex.from_gmt(session, str(gmt)).gene_membership()
