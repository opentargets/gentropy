"""Test the credible set validation step."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from gentropy.common.session import Session
from gentropy.credible_set_validation import CredibleSetValidationStep
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.study_locus import StudyLocusQualityCheck


@pytest.mark.step_test
class TestCredibleSetValidationStep:
    """Run the step on two sources and check the valid and invalid outputs."""

    @pytest.fixture(autouse=True)
    def _setup(self, session: Session, tmp_path: Path) -> None:
        """Two sources of credible sets with their variants, a study index, targets and diseases.

        Source 1 has a valid credible set (keep) and one led by an MHC variant (mhc); source 2 has
        a valid one (keep2) and one of a study missing from the study index (missing).
        """
        spark = session.spark
        cs_schema = CredibleSet.get_schema()
        variant_schema = CredibleSetVariant.get_schema()

        def cs_row(
            cs_id: str, study: str, chromosome: str, lead: str
        ) -> tuple[Any, ...]:
            values = {
                "id": cs_id,
                "chromosome": chromosome,
                "locusStart": 1,
                "locusEnd": 40_000_000,
                "studyId": study,
                "studyType": "gwas",
                "leadVariantId": lead,
                "fineMappingMethod": "SuSiE",
                "qualityControls": [],
            }
            return tuple(values.get(c) for c in cs_schema.fieldNames())

        def variant_row(cs_id: str, variant: str) -> tuple[Any, ...]:
            values = {
                "credibleSetId": cs_id,
                "variantId": variant,
                "pValueMantissa": 1.0,
                "pValueExponent": -10,
                "posteriorProbability": 0.97,
                "qualityControls": [],
            }
            return tuple(values.get(c) for c in variant_schema.fieldNames())

        sources = {
            "one": [
                ("keep", "s1", "1", "1_100_A_T"),
                ("mhc", "s1", "6", "6_30000000_A_T"),
            ],
            "two": [
                ("keep2", "s2", "2", "2_100_A_T"),
                ("missing", "absent", "3", "3_100_A_T"),
            ],
        }
        self.cs_paths, self.variant_paths = [], []
        for name, rows in sources.items():
            cs_path, variant_path = (
                str(tmp_path / name / "cs"),
                str(tmp_path / name / "csv"),
            )
            spark.createDataFrame([cs_row(*r) for r in rows], cs_schema).write.parquet(
                cs_path
            )
            spark.createDataFrame(
                [variant_row(r[0], r[3]) for r in rows], variant_schema
            ).write.parquet(variant_path)
            self.cs_paths.append(cs_path)
            self.variant_paths.append(variant_path)

        self.study_index_path = str(tmp_path / "study_index")
        spark.createDataFrame(
            [("s1", "p", "gwas", ["d1"]), ("s2", "p", "gwas", ["d1"])],
            "studyId string, projectId string, studyType string, diseaseIds array<string>",
        ).write.parquet(self.study_index_path)
        self.target_index_path = str(tmp_path / "target_index")
        spark.createDataFrame(
            [("g1", "g1", "protein_coding", 1, ("1", 1, 2, 1))],
            "id string, approvedSymbol string, biotype string, tss long, "
            "genomicLocation struct<chromosome: string, start: long, end: long, strand: int>",
        ).write.parquet(self.target_index_path)
        self.disease_index_path = str(tmp_path / "disease_index")
        spark.createDataFrame(
            [("d1", ["MONDO_0045024"])], "id string, therapeuticAreas array<string>"
        ).write.parquet(self.disease_index_path)
        self.out = tmp_path / "out"

    def test_step(self, session: Session) -> None:
        """Invalid credible sets and their variants are split off; the valid ones get a confidence."""
        out = self.out
        CredibleSetValidationStep(
            session=session,
            credible_set_path=self.cs_paths,
            credible_set_variant_path=self.variant_paths,
            study_index_path=self.study_index_path,
            target_index_path=self.target_index_path,
            disease_index_path=self.disease_index_path,
            valid_credible_set_path=str(out / "valid_cs"),
            valid_credible_set_variant_path=str(out / "valid_csv"),
            invalid_credible_set_path=str(out / "invalid_cs"),
            invalid_credible_set_variant_path=str(out / "invalid_csv"),
            trans_qtl_threshold=5_000_000,
            invalid_qc_reasons=["IN_MHC", "MISSING_STUDY"],
        )
        valid = CredibleSet.from_parquet(session, str(out / "valid_cs")).df.collect()
        invalid = CredibleSet.from_parquet(
            session, str(out / "invalid_cs")
        ).df.collect()
        valid_variants = CredibleSetVariant.from_parquet(
            session, str(out / "valid_csv")
        ).df
        invalid_variants = CredibleSetVariant.from_parquet(
            session, str(out / "invalid_csv")
        ).df

        assert sorted(r.id for r in valid) == ["keep", "keep2"]
        assert {r.id: r.qualityControls for r in invalid} == {
            "mhc": [StudyLocusQualityCheck.IN_MHC.value],
            "missing": [StudyLocusQualityCheck.MISSING_STUDY.value],
        }
        assert {r.confidence for r in valid} == {4}
        # Both studies report d1 at different leads, so neither is replicated:
        assert all(r.qualityControls == [] for r in valid)
        assert sorted(r.credibleSetId for r in valid_variants.collect()) == [
            "keep",
            "keep2",
        ]
        assert sorted(r.credibleSetId for r in invalid_variants.collect()) == [
            "mhc",
            "missing",
        ]
