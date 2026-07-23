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
