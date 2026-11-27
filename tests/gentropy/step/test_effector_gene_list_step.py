"""Test the Effector Gene List step end to end."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from gentropy.common.session import Session
from gentropy.effector_gene_list import EffectorGeneListStep


@pytest.fixture()
def rare_variant_evidence_path(spark: SparkSession, tmp_path: Path) -> str:
    """Write a small rare-variant evidence parquet and return its path."""
    path = str(tmp_path / "evidence_rare")
    spark.createDataFrame(
        [
            ("EFO_1", "ENSG_A", 0.92),  # kept
            ("EFO_2", "ENSG_B", 0.75),  # kept (== threshold)
            ("EFO_3", "ENSG_C", 0.50),  # dropped (below threshold)
            ("EFO_1", "ENSG_A", 0.80),  # duplicate pair, kept once
        ],
        ["diseaseId", "targetId", "score"],
    ).write.parquet(path)
    return path


@pytest.fixture()
def clinical_evidence_path(spark: SparkSession, tmp_path: Path) -> str:
    """Write a small clinical precedence evidence parquet and return its path."""
    path = str(tmp_path / "evidence_clinical")
    spark.createDataFrame(
        [
            ("EFO_4", "ENSG_D", "APPROVAL"),  # kept
            ("EFO_5", "ENSG_E", "PHASE_3"),  # kept
            ("EFO_6", "ENSG_F", "PHASE_1"),  # dropped (not approved)
        ],
        ["diseaseId", "targetId", "clinicalStage"],
    ).write.parquet(path)
    return path


@pytest.fixture()
def gold_standard_path(spark: SparkSession, tmp_path: Path) -> str:
    """Write a small legacy OTG gold standard curation JSON and return its path."""
    path = str(tmp_path / "gold_standard")
    rows = [
        {
            "gold_standard_info": {"gene_id": "ENSG_G", "highest_confidence": "High"},
            "trait_info": {"ontology": ["EFO_7", "EFO_8"]},
        },
        {
            "gold_standard_info": {"gene_id": "ENSG_H", "highest_confidence": "Medium"},
            "trait_info": {"ontology": ["EFO_9"]},
        },
        {
            "gold_standard_info": {"gene_id": "ENSG_I", "highest_confidence": "Low"},
            "trait_info": {"ontology": ["EFO_10"]},
        },  # dropped (low confidence)
    ]
    spark.createDataFrame(rows).write.json(path)
    return path


class TestEffectorGeneListStep:
    """Test the EGL step's source combination and filtering."""

    @pytest.mark.step_test
    def test_all_sources(
        self,
        session: Session,
        spark: SparkSession,
        tmp_path: Path,
        rare_variant_evidence_path: str,
        clinical_evidence_path: str,
        gold_standard_path: str,
    ) -> None:
        """All three sources combine into a distinct, filtered gene-disease list."""
        out = str(tmp_path / "egl")
        EffectorGeneListStep(
            session,
            effector_gene_list_path=out,
            rare_variant_evidence_paths=[rare_variant_evidence_path],
            clinical_evidence_path=clinical_evidence_path,
            gold_standard_path=gold_standard_path,
        )
        result = {
            (row["diseaseId"], row["targetId"])
            for row in spark.read.parquet(out).collect()
        }
        assert result == {
            ("EFO_1", "ENSG_A"),  # rare variant
            ("EFO_2", "ENSG_B"),  # rare variant
            ("EFO_4", "ENSG_D"),  # clinical
            ("EFO_5", "ENSG_E"),  # clinical
            ("EFO_7", "ENSG_G"),  # gold standard (exploded ontology)
            ("EFO_8", "ENSG_G"),  # gold standard (exploded ontology)
            ("EFO_9", "ENSG_H"),  # gold standard
        }

    @pytest.mark.step_test
    def test_single_source(
        self,
        session: Session,
        spark: SparkSession,
        tmp_path: Path,
        clinical_evidence_path: str,
    ) -> None:
        """A single optional source is enough to build the list."""
        out = str(tmp_path / "egl_clinical_only")
        EffectorGeneListStep(
            session,
            effector_gene_list_path=out,
            clinical_evidence_path=clinical_evidence_path,
        )
        assert spark.read.parquet(out).count() == 2

    @pytest.mark.step_test
    def test_custom_thresholds(
        self,
        session: Session,
        spark: SparkSession,
        tmp_path: Path,
        rare_variant_evidence_path: str,
    ) -> None:
        """Source-specific filters are configurable."""
        out = str(tmp_path / "egl_strict")
        EffectorGeneListStep(
            session,
            effector_gene_list_path=out,
            rare_variant_evidence_paths=[rare_variant_evidence_path],
            rare_variant_score_threshold=0.9,
        )
        result = {
            (row["diseaseId"], row["targetId"])
            for row in spark.read.parquet(out).collect()
        }
        assert result == {("EFO_1", "ENSG_A")}

    @pytest.mark.step_test
    def test_no_source_raises(self, session: Session, tmp_path: Path) -> None:
        """The step fails when no source is provided."""
        with pytest.raises(ValueError, match="At least one EGL source"):
            EffectorGeneListStep(
                session, effector_gene_list_path=str(tmp_path / "egl_empty")
            )


class TestMultiTargetDrugs:
    """Test the split of clinical pairs from multi-target drugs into the PEGL."""

    @pytest.fixture()
    def clinical_path(self, spark: SparkSession, tmp_path: Path) -> str:
        """Clinical evidence with one drug above a limit of 5 targets and two within it."""
        path = str(tmp_path / "evidence_clinical_multi")
        rows = [
            # CHEMBL_MULTI: 6 targets, 5 at an approved stage and 1 only in phase 1.
            *[("CHEMBL_MULTI", "EFO_1", f"ENSG_{i}", "APPROVAL") for i in range(1, 6)],
            ("CHEMBL_MULTI", "EFO_2", "ENSG_6", "PHASE_1"),
            # CHEMBL_SINGLE: 1 target, also supports (EFO_1, ENSG_1).
            ("CHEMBL_SINGLE", "EFO_1", "ENSG_1", "PHASE_4"),
            # CHEMBL_FEW: 2 targets, both approved, one of them on another disease.
            ("CHEMBL_FEW", "EFO_3", "ENSG_7", "PHASE_3"),
            ("CHEMBL_FEW", "EFO_3", "ENSG_8", "PHASE_2"),
        ]
        spark.createDataFrame(
            rows, ["drugId", "diseaseId", "targetId", "clinicalStage"]
        ).write.parquet(path)
        return path

    @pytest.fixture()
    def rare_path(self, spark: SparkSession, tmp_path: Path) -> str:
        """Rare-variant evidence supporting one pair of the multi-target drug."""
        path = str(tmp_path / "evidence_rare_multi")
        spark.createDataFrame(
            [("EFO_1", "ENSG_2", 0.9)], ["diseaseId", "targetId", "score"]
        ).write.parquet(path)
        return path

    @staticmethod
    def _pairs(spark: SparkSession, path: str) -> set[tuple[str, str]]:
        return {
            (row["diseaseId"], row["targetId"])
            for row in spark.read.parquet(path).collect()
        }

    @pytest.mark.step_test
    def test_split(
        self,
        session: Session,
        spark: SparkSession,
        tmp_path: Path,
        clinical_path: str,
        rare_path: str,
    ) -> None:
        """Pairs supported only by a drug above the limit go to the PEGL, never to both lists.

        The phase 1 row counts towards the 6 targets of CHEMBL_MULTI but reaches neither list.
        (EFO_1, ENSG_1) is also supported by a 1-target drug and (EFO_1, ENSG_2) by rare-variant
        evidence, so both stay in the EGL.
        """
        egl, pegl = str(tmp_path / "egl"), str(tmp_path / "pegl")
        EffectorGeneListStep(
            session,
            effector_gene_list_path=egl,
            rare_variant_evidence_paths=[rare_path],
            clinical_evidence_path=clinical_path,
            max_targets_per_drug=5,
            plausible_effector_gene_list_path=pegl,
        )
        assert self._pairs(spark, egl) == {
            ("EFO_1", "ENSG_1"),  # also from CHEMBL_SINGLE
            ("EFO_1", "ENSG_2"),  # also from rare-variant evidence
            ("EFO_3", "ENSG_7"),  # CHEMBL_FEW, approved stage
        }
        assert self._pairs(spark, pegl) == {
            ("EFO_1", "ENSG_3"),
            ("EFO_1", "ENSG_4"),
            ("EFO_1", "ENSG_5"),
        }

    @pytest.mark.step_test
    def test_limit_counts_all_stages(
        self,
        session: Session,
        spark: SparkSession,
        tmp_path: Path,
        clinical_path: str,
    ) -> None:
        """With a limit of 6, CHEMBL_MULTI is within the limit and the PEGL is empty."""
        egl, pegl = str(tmp_path / "egl6"), str(tmp_path / "pegl6")
        EffectorGeneListStep(
            session,
            effector_gene_list_path=egl,
            clinical_evidence_path=clinical_path,
            max_targets_per_drug=6,
            plausible_effector_gene_list_path=pegl,
        )
        assert len(self._pairs(spark, egl)) == 6
        assert self._pairs(spark, pegl) == set()

    @pytest.mark.step_test
    def test_no_limit_is_unchanged(
        self,
        session: Session,
        spark: SparkSession,
        tmp_path: Path,
        clinical_path: str,
    ) -> None:
        """Without a limit every approved-stage pair is in the EGL, whatever the drug."""
        egl = str(tmp_path / "egl_no_limit")
        EffectorGeneListStep(
            session,
            effector_gene_list_path=egl,
            clinical_evidence_path=clinical_path,
        )
        assert self._pairs(spark, egl) == {
            *[("EFO_1", f"ENSG_{i}") for i in range(1, 6)],
            ("EFO_3", "ENSG_7"),
        }

    @pytest.mark.step_test
    def test_limit_without_pegl_path_raises(
        self, session: Session, tmp_path: Path, clinical_path: str
    ) -> None:
        """The limit needs an output path for the PEGL."""
        with pytest.raises(ValueError, match="plausible_effector_gene_list_path"):
            EffectorGeneListStep(
                session,
                effector_gene_list_path=str(tmp_path / "egl_raise"),
                clinical_evidence_path=clinical_path,
                max_targets_per_drug=5,
            )
