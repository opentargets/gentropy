"""Tests for the fmPops step."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from gentropy.common.session import Session
from gentropy.dataset.fm_pops_score import FmPopsScore
from gentropy.fm_pops import FmPopsStep

CHROMOSOMES = {"1": 1_000_000, "2": 1_000_000, "6": 29_000_000}
GENES_PER_CHROMOSOME = 12


def _gene_ids() -> list[tuple[str, str, int]]:
    """Gene identifier, chromosome and TSS of every toy gene, 200 kb apart."""
    return [
        (f"chr{chromosome}_gene{i}", chromosome, first_tss + 200_000 * i)
        for chromosome, first_tss in CHROMOSOMES.items()
        for i in range(GENES_PER_CHROMOSOME)
    ]


def _write_inputs(spark: SparkSession, root: Path) -> None:
    """Write a release with one credible set per gene, each nearest to that gene."""
    genes = _gene_ids()
    spark.createDataFrame(
        [
            (gene, "protein_coding", chrom, tss, tss + 999, tss)
            for gene, chrom, tss in genes
        ],
        "id string, biotype string, chromosome string, start long, end long, tss long",
    ).select(
        "id",
        "biotype",
        f.struct("chromosome", "start", "end").alias("genomicLocation"),
        "tss",
    ).write.parquet(str(root / "target_index"))

    rng = np.random.default_rng(3)
    variants, credible_sets, studies = [], [], []
    for i, (gene, chrom, tss) in enumerate(genes):
        neighbour = genes[i + 1][0] if i + 1 < len(genes) else genes[i - 1][0]
        variants.append(
            (
                f"v{i}",
                chrom,
                tss + 100,
                "A",
                "T",
                [
                    (0, 100, gene, True, "protein_coding"),
                    (0, 199_900, neighbour, True, "protein_coding"),
                ],
            )
        )
        n_diseases = int(rng.integers(0, 4))
        studies.append((f"s{i}", "gwas", "p", [f"d{j}" for j in range(n_diseases)]))
        credible_sets.append((f"cs{i}", f"s{i}", f"v{i}", chrom, "gwas"))

    spark.createDataFrame(
        variants,
        "variantId string, chromosome string, position int, referenceAllele string, "
        "alternateAllele string, transcriptConsequences array<struct<"
        "distanceFromFootprint:long, distanceFromTss:long, targetId:string, "
        "isEnsemblCanonical:boolean, biotype:string>>",
    ).write.parquet(str(root / "variant_index"))
    spark.createDataFrame(
        studies,
        "studyId string, studyType string, projectId string, diseaseIds array<string>",
    ).write.parquet(str(root / "study_index"))
    spark.createDataFrame(
        credible_sets,
        "studyLocusId string, studyId string, variantId string, chromosome string, studyType string",
    ).write.parquet(str(root / "credible_set"))

    # PoPS features for every gene but the last one, plus a gene the target index lacks.
    feature_dir = root / "pops" / "munged_features"
    feature_dir.mkdir(parents=True)
    rows = [gene for gene, _, _ in genes[:-1]] + ["ENSG_not_in_release"]
    (feature_dir / "pops_features.rows.txt").write_text("\n".join(rows) + "\n")
    (root / "pops" / "control.features").write_text("expr.control\n")
    for i, columns in enumerate(
        [["expr.1", "expr.2", "expr.control"], ["ppi.1", "ppi.2", "ppi.3"]]
    ):
        (feature_dir / f"pops_features.cols.{i}.txt").write_text(
            "\n".join(columns) + "\n"
        )
        np.save(
            feature_dir / f"pops_features.mat.{i}.npy",
            rng.normal(size=(len(rows), len(columns))),
        )


@pytest.mark.step_test
def test_fm_pops_step(spark: SparkSession, session: Session, tmp_path: Path) -> None:
    """The step scores the genes with features and counts the diseases they are nearest for."""
    _write_inputs(spark, tmp_path)
    output_path = str(tmp_path / "fm_pops")

    FmPopsStep(
        session=session,
        credible_set_path=str(tmp_path / "credible_set"),
        study_index_path=str(tmp_path / "study_index"),
        variant_index_path=str(tmp_path / "variant_index"),
        target_index_path=str(tmp_path / "target_index"),
        pops_feature_dir=str(tmp_path / "pops"),
        output_path=output_path,
        lambda_grid=[0.1, 1.0, 10.0],
    )

    scores = FmPopsScore.from_parquet(session, output_path).df.toPandas()
    studies = spark.read.parquet(str(tmp_path / "study_index")).toPandas()
    expected_counts = {
        gene: len(diseases)
        for (gene, _, _), diseases in zip(
            _gene_ids(),
            studies.sort_values("studyId", key=lambda s: s.str[1:].astype(int))[
                "diseaseIds"
            ],
        )
    }

    assert len(scores) == len(_gene_ids()) - 1
    assert "ENSG_not_in_release" not in set(scores["geneId"])
    assert dict(zip(scores["geneId"], scores["targetCount"])) == {
        gene: count
        for gene, count in expected_counts.items()
        if gene in set(scores["geneId"])
    }
    assert np.isfinite(scores["fmPops"]).all()
    assert (scores["fmPops"] != 0).any()
