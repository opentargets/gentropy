"""Test the L2G training set step logic."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from gentropy.common.session import Session
from gentropy.dataset.effector_gene_list import EffectorGeneList
from gentropy.dataset.interactions import Interactions
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_locus import StudyLocusQualityCheck
from gentropy.training_set import TrainingSetStep


class TestLabel:
    """Test positive/negative labelling against the effector gene list."""

    def test_labelling(self, spark: SparkSession) -> None:
        """Genes matching an EGL pair in a locus are positives; other genes in those loci are negatives."""
        feature_matrix = spark.createDataFrame(
            [
                ("sl1", "ENSG_A", ["EFO_1"]),  # positive (EGL match)
                ("sl1", "ENSG_B", ["EFO_1"]),  # negative (same locus, no match)
                ("sl2", "ENSG_C", ["EFO_2", "EFO_3"]),  # positive (EGL match on EFO_2)
                ("sl3", "ENSG_D", ["EFO_3"]),  # dropped (locus has no positive)
            ],
            ["studyLocusId", "geneId", "diseaseIds"],
        )
        egl = EffectorGeneList(
            _df=spark.createDataFrame(
                [("EFO_1", "ENSG_A"), ("EFO_2", "ENSG_C"), ("EFO_9", "ENSG_X")],
                EffectorGeneList.get_schema(),
            )
        )
        result = {
            (r["studyLocusId"], r["geneId"], r["GSP"])
            for r in TrainingSetStep._label(feature_matrix, egl).collect()
        }
        assert result == {
            ("sl1", "ENSG_A", 1),
            ("sl1", "ENSG_B", 0),
            ("sl2", "ENSG_C", 1),
        }

    def test_disease_must_match(self, spark: SparkSession) -> None:
        """A gene present in the EGL but for a different disease is not a positive."""
        feature_matrix = spark.createDataFrame(
            [("sl1", "ENSG_A", ["EFO_2"])],
            ["studyLocusId", "geneId", "diseaseIds"],
        )
        egl = EffectorGeneList(
            _df=spark.createDataFrame(
                [("EFO_1", "ENSG_A")], EffectorGeneList.get_schema()
            )
        )
        # No locus contains a positive, so nothing is retained.
        assert TrainingSetStep._label(feature_matrix, egl).count() == 0


class TestCapPositivesPerLocus:
    """Test the maximum-positives-per-locus filter."""

    def test_cap(self, spark: SparkSession) -> None:
        """Loci with more positives than allowed are dropped entirely."""
        labelled = spark.createDataFrame(
            [
                ("sl1", "g1", 1),
                ("sl1", "g2", 1),  # sl1 has 2 positives -> kept (<= 2)
                ("sl1", "g3", 0),
                ("sl2", "g4", 1),
                ("sl2", "g5", 1),
                ("sl2", "g6", 1),  # sl2 has 3 positives -> dropped
            ],
            ["studyLocusId", "geneId", "GSP"],
        )
        result = {
            r["studyLocusId"]
            for r in TrainingSetStep._cap_positives_per_locus(labelled, 2).collect()
        }
        assert result == {"sl1"}

    def test_locus_without_positive_is_dropped(self, spark: SparkSession) -> None:
        """A locus left all-negative by an earlier filter is dropped when the cap is re-applied."""
        labelled = spark.createDataFrame(
            [
                ("sl1", "g1", 1),
                ("sl1", "g2", 0),
                ("sl2", "g3", 0),  # positives already removed -> dropped
            ],
            ["studyLocusId", "geneId", "GSP"],
        )
        result = {
            r["studyLocusId"]
            for r in TrainingSetStep._cap_positives_per_locus(labelled, 2).collect()
        }
        assert result == {"sl1"}


class TestDropInteractingNegatives:
    """Test removal of negatives interacting with positives in the same locus."""

    @pytest.mark.step_test
    @pytest.mark.parametrize("positive_column", ["targetA", "targetB"])
    def test_filter(
        self,
        session: Session,
        spark: SparkSession,
        tmp_path: Path,
        positive_column: str,
    ) -> None:
        """A negative is removed only when the positive is ``targetA`` and the negative ``targetB``."""
        labelled = spark.createDataFrame(
            [
                ("sl1", "ENSG_POS", 1),
                ("sl1", "ENSG_POS2", 1),  # interacts with ENSG_POS but positive -> kept
                ("sl1", "ENSG_PARTNER", 0),  # interacts with ENSG_POS -> removed
                ("sl1", "ENSG_OTHER", 0),  # no interaction -> kept
            ],
            ["studyLocusId", "geneId", "GSP"],
        )
        pairs = [("ENSG_POS", "ENSG_PARTNER"), ("ENSG_POS", "ENSG_POS2")]
        if positive_column == "targetB":
            pairs = [(b, a) for a, b in pairs]
        interaction_path = str(tmp_path / f"interactions_{positive_column}")
        spark.createDataFrame(
            [(*pair, "string", 0.9) for pair in pairs],
            ["targetA", "targetB", "sourceDatabase", "scoring"],
        ).write.parquet(interaction_path)

        interactions = Interactions.from_parquet(
            session, interaction_path
        ).high_confidence("string", 0.75)
        result = {
            (r["studyLocusId"], r["geneId"])
            for r in TrainingSetStep._drop_interacting_negatives(
                labelled, interactions
            ).collect()
        }
        expected = {("sl1", "ENSG_POS"), ("sl1", "ENSG_POS2"), ("sl1", "ENSG_OTHER")}
        if positive_column == "targetB":
            expected.add(("sl1", "ENSG_PARTNER"))
        assert result == expected


class TestDeduplicate:
    """Test deduplication of credible sets with identical positive profiles."""

    COLUMNS = [
        "studyLocusId",
        "geneId",
        "diseaseIds",
        "variantId",
        "GSP",
        "vepMaximum",
        "vepMean",
        "eQtlColocClppMaximum",
        "pQtlColocClppMaximum",
        "sQtlColocClppMaximum",
        "eQtlColocH4Maximum",
        "pQtlColocH4Maximum",
        "sQtlColocH4Maximum",
    ]

    def _kept(self, spark: SparkSession, rows: list[tuple[Any, ...]]) -> set[str]:
        labelled = spark.createDataFrame(rows, self.COLUMNS)
        return {
            r["studyLocusId"] for r in TrainingSetStep._deduplicate(labelled).collect()
        }

    def test_dedup(self, spark: SparkSession) -> None:
        """Loci whose positives share a profile collapse to the smallest studyLocusId.

        Colocalisation is compared at 2 dp, VEP exactly.
        """
        rows = [
            # sl1 and sl2 differ only in colocalisation past 2 dp and in the disease order.
            (
                "sl1",
                "gA",
                ["EFO_1", "EFO_2"],
                "v1",
                1,
                0.5,
                0.5,
                0.111,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
            ),
            (
                "sl2",
                "gA",
                ["EFO_2", "EFO_1"],
                "v1",
                1,
                0.5,
                0.5,
                0.112,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
            ),
            ("sl3", "gB", ["EFO_2"], "v2", 1, 0.9, 0.9, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0),
            # sl4 is sl1 with a vepMean differing past 2 dp: not a duplicate.
            (
                "sl4",
                "gA",
                ["EFO_1", "EFO_2"],
                "v1",
                1,
                0.5,
                0.501,
                0.111,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
            ),
        ]
        assert self._kept(spark, rows) == {"sl1", "sl3", "sl4"}

    def test_dedup_is_per_locus(self, spark: SparkSession) -> None:
        """Loci with two identical positives collapse to one locus, not one locus per positive."""
        rows = [
            (locus, gene, ["EFO_1"], "v1", 1, 0.5, 0.5, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0)
            for locus in ["sl1", "sl2"]
            for gene in ["gA", "gB"]
        ]
        assert self._kept(spark, rows) == {"sl1"}


class TestTrainingSetStep:
    """Test the step end to end."""

    @pytest.mark.step_test
    def test_step(self, session: Session, spark: SparkSession, tmp_path: Path) -> None:
        """Only replicated loci are labelled and the output is a valid L2GGoldStandard."""
        replicated = [StudyLocusQualityCheck.REPLICATED.value]
        paths = {
            name: str(tmp_path / name) for name in ["fm", "cs", "si", "egl", "out"]
        }
        spark.createDataFrame(
            [
                ("sl1", "v1", "s1", replicated),
                ("sl2", "v2", "s1", []),  # not replicated -> dropped
            ],
            ["studyLocusId", "variantId", "studyId", "qualityControls"],
        ).write.parquet(paths["cs"])
        spark.createDataFrame(
            [("s1", "p1", "gwas", ["EFO_1"])],
            ["studyId", "projectId", "studyType", "diseaseIds"],
        ).write.parquet(paths["si"])
        spark.createDataFrame(
            [
                ("sl1", "gA", 1000, 1),
                ("sl1", "gB", 0, 1),
                ("sl2", "gA", 1000, 1),
            ],
            ["studyLocusId", "geneId", "distanceSentinelFootprint", "isProteinCoding"],
        ).write.parquet(paths["fm"])
        spark.createDataFrame(
            [("EFO_1", "gA")], ["diseaseId", "targetId"]
        ).write.parquet(paths["egl"])
        TrainingSetStep(
            session,
            feature_matrix_path=paths["fm"],
            credible_set_path=paths["cs"],
            study_index_path=paths["si"],
            effector_gene_list_path=paths["egl"],
            training_set_path=paths["out"],
            apply_deduplication=False,
        )
        gold_standard = L2GGoldStandard.from_parquet(session, paths["out"])
        assert {
            (r["studyLocusId"], r["geneId"], r["goldStandardSet"])
            for r in gold_standard.df.collect()
        } == {("sl1", "gA", "positive"), ("sl1", "gB", "negative")}
