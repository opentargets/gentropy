"""Test the L2G training set step logic."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession

from gentropy.common.session import Session
from gentropy.dataset.study_locus import StudyLocusQualityCheck
from gentropy.training_set import TrainingSetStep


class TestLabelGoldStandard:
    """Test positive/negative labelling against the effector gene list."""

    def test_labelling(self, spark: SparkSession) -> None:
        """Genes matching an EGL pair in a locus are positives; other genes in those loci are negatives."""
        annotated_fm = spark.createDataFrame(
            [
                ("sl1", "ENSG_A", ["EFO_1"]),  # positive (EGL match)
                ("sl1", "ENSG_B", ["EFO_1"]),  # negative (same locus, no match)
                ("sl2", "ENSG_C", ["EFO_2"]),  # positive (EGL match)
                ("sl3", "ENSG_D", ["EFO_3"]),  # dropped (locus has no positive)
            ],
            ["studyLocusId", "geneId", "diseaseIds"],
        )
        egl = spark.createDataFrame(
            [("ENSG_A", "EFO_1"), ("ENSG_C", "EFO_2"), ("ENSG_X", "EFO_9")],
            ["targetId", "diseaseId"],
        )
        result = {
            (r["studyLocusId"], r["geneId"], r["GSP"])
            for r in TrainingSetStep._label_gold_standard(annotated_fm, egl).collect()
        }
        assert result == {
            ("sl1", "ENSG_A", 1),
            ("sl1", "ENSG_B", 0),
            ("sl2", "ENSG_C", 1),
        }

    def test_disease_must_match(self, spark: SparkSession) -> None:
        """A gene present in the EGL but for a different disease is not a positive."""
        annotated_fm = spark.createDataFrame(
            [("sl1", "ENSG_A", ["EFO_2"])],
            ["studyLocusId", "geneId", "diseaseIds"],
        )
        egl = spark.createDataFrame([("ENSG_A", "EFO_1")], ["targetId", "diseaseId"])
        # No locus contains a positive, so nothing is retained.
        assert TrainingSetStep._label_gold_standard(annotated_fm, egl).count() == 0


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


class TestFilterInteractingNegatives:
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
        """A negative interacting with a positive is removed, whichever side of the pair it is on.

        The interaction dataset is not guaranteed to hold both directions of a pair, so the
        interaction is written in one direction only and the test runs it both ways round.
        """
        labelled = spark.createDataFrame(
            [
                ("sl1", "ENSG_POS", 1),
                ("sl1", "ENSG_PARTNER", 0),  # interacts with ENSG_POS -> removed
                ("sl1", "ENSG_OTHER", 0),  # no interaction -> kept
            ],
            ["studyLocusId", "geneId", "GSP"],
        )
        pair = (
            ("ENSG_POS", "ENSG_PARTNER")
            if positive_column == "targetA"
            else ("ENSG_PARTNER", "ENSG_POS")
        )
        interaction_path = str(tmp_path / f"interactions_{positive_column}")
        spark.createDataFrame(
            [(*pair, "string", 0.9)],
            ["targetA", "targetB", "sourceDatabase", "scoring"],
        ).write.parquet(interaction_path)

        interactions = TrainingSetStep._interaction_pairs(
            session, interaction_path, "string", 0.75
        )
        result = {
            (r["studyLocusId"], r["geneId"])
            for r in TrainingSetStep._filter_interacting_negatives(
                labelled, interactions
            ).collect()
        }
        assert result == {("sl1", "ENSG_POS"), ("sl1", "ENSG_OTHER")}


class TestReplicatedLoci:
    """Test that the replication filter reads the credible set quality control flag."""

    def test_replication(self, spark: SparkSession) -> None:
        """Only the credible sets carrying the REPLICATED flag pass."""
        credible_set = SimpleNamespace(
            df=spark.createDataFrame(
                [
                    ("sl1", [StudyLocusQualityCheck.REPLICATED.value]),
                    (
                        "sl2",
                        [
                            StudyLocusQualityCheck.TOP_HIT.value,
                            StudyLocusQualityCheck.REPLICATED.value,
                        ],
                    ),
                    ("sl3", [StudyLocusQualityCheck.TOP_HIT.value]),  # not replicated
                    ("sl4", []),  # never assessed
                ],
                ["studyLocusId", "qualityControls"],
            )
        )
        result = {
            r["studyLocusId"]
            for r in TrainingSetStep._replicated_loci(
                credible_set,  # type: ignore[arg-type]
            ).collect()
        }
        assert result == {"sl1", "sl2"}


class TestDeduplicate:
    """Test deduplication of credible sets with identical positive profiles."""

    def test_dedup(self, spark: SparkSession) -> None:
        """Loci whose positives share an identical (rounded) feature profile collapse to one."""
        cols = [
            "studyLocusId",
            "geneId",
            "diseaseIds",
            "variantId",
            "vepMaximum",
            "vepMean",
            "GSP",
            "eQtlColocClppMaximum",
            "pQtlColocClppMaximum",
            "sQtlColocClppMaximum",
            "eQtlColocH4Maximum",
            "pQtlColocH4Maximum",
            "sQtlColocH4Maximum",
        ]
        # sl1 and sl2 positives share the same profile (coloc differs only past 2 dp).
        rows = [
            ("sl1", "gA", ["EFO_1"], "v1", 0.5, 0.5, 1, 0.111, 0.0, 0.0, 0.0, 0.0, 0.0),
            ("sl2", "gA", ["EFO_1"], "v1", 0.5, 0.5, 1, 0.112, 0.0, 0.0, 0.0, 0.0, 0.0),
            ("sl3", "gB", ["EFO_2"], "v2", 0.9, 0.9, 1, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0),
        ]
        labelled = spark.createDataFrame(rows, cols)
        kept = {
            r["studyLocusId"] for r in TrainingSetStep._deduplicate(labelled).collect()
        }
        # One of sl1/sl2 survives dedup, plus the distinct sl3.
        assert "sl3" in kept
        assert len(kept & {"sl1", "sl2"}) == 1


class TestTrainingSetStepValidation:
    """Test constructor-level validation."""

    @pytest.mark.step_test
    def test_interaction_filter_requires_path(self, session: Session) -> None:
        """Requesting the interaction filter without a path fails fast."""
        with pytest.raises(ValueError, match="interaction_path is required"):
            TrainingSetStep(
                session,
                feature_matrix_path="fm",
                credible_set_path="cs",
                study_index_path="si",
                effector_gene_list_path="egl",
                training_set_path="out",
                apply_interaction_filter=True,
                interaction_path=None,
            )
