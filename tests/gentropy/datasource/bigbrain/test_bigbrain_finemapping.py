"""Test BigBrain fine-mapping ingestion."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame

from gentropy.bigbrain_ingestion import BigBrainFinemappingIngestionStep
from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import CredibleSetConfidenceClasses, StudyLocus
from gentropy.datasource.bigbrain.finemapping import BigBrainFinemapping

HEADER = "SNP\tCHR\tBP\tA1\tA2\tZ\tN\tP\tPIP\tBETA_MEAN\tBETA_SD\tDISTANCE_FROM_CENTER\tCREDIBLE_SET\tfeature"
ROWS = [
    # Single-variant credible set with a p-value far below the float64 floor.
    "rs1\t1\t1000\tC\tT\t40.0\t9310\t0\t1.0\t0.37\t0.01\t10\t1\tENSG01.1",
    # Two-variant credible set, plus an untested placeholder that is not its lead.
    "rs2\t1\t5000\tG\tA\t8.0\t9310\t1e-15\t0.6\t0.2\t0.02\t20\t2\tENSG01.1",
    "rs3\t1\t5100\tT\tC\t7.5\t9310\t1e-13\t0.38\t0.1\t0.02\t30\t2\tENSG01.1",
    "rs4\t1\t5200\tA\tG\t0\t9310\t1\t0.01\t0.0\t0.02\t40\t2\tENSG01.1",
    # Credible set led by an untested placeholder: dropped entirely.
    "rs5\t2\t100\tA\tC\t0\t9310\t1\t0.9\t-2.1\t0.01\t50\t3\tENSG01.1",
    "rs6\t2\t200\tG\tT\t3.0\t9310\t0.003\t0.08\t0.1\t0.01\t60\t3\tENSG01.1",
]


@pytest.fixture
def finemapping_file(tmp_path: Path) -> str:
    """Write a small BigBrain fine-mapping file."""
    path = tmp_path / "BigBrain_cis_eQTL_finemapping.txt"
    path.write_text("\n".join([HEADER, *ROWS]) + "\n")
    return str(path)


@pytest.fixture
def raw_finemapping(session: Session, finemapping_file: str) -> DataFrame:
    """Raw fine-mapping rows."""
    return BigBrainFinemapping.read_from_source(session, finemapping_file)


@pytest.fixture
def credible_sets(raw_finemapping: DataFrame) -> dict[int, dict[str, Any]]:
    """Credible sets keyed by credibleSetIndex."""
    cs = BigBrainFinemapping.from_source(raw_finemapping, "eqtl")
    assert isinstance(cs, StudyLocus)
    return {r.credibleSetIndex: r.asDict(recursive=True) for r in cs.df.collect()}


class TestBigBrainFinemapping:
    """Test BigBrainFinemapping.from_source."""

    def test_drops_credible_sets_led_by_untested_variant(
        self, credible_sets: dict[int, dict[str, Any]]
    ) -> None:
        """A credible set whose lead has Z == 0 is an artefact and is removed."""
        assert sorted(credible_sets) == [1, 2]

    def test_drops_untested_members(
        self, credible_sets: dict[int, dict[str, Any]]
    ) -> None:
        """Untested placeholder rows are removed from credible sets that are kept."""
        members = [v["variantId"] for v in credible_sets[2]["locus"]]
        assert members == ["1_5000_A_G", "1_5100_C_T"]

    def test_variant_id_uses_a1_as_alternate_allele(
        self, credible_sets: dict[int, dict[str, Any]]
    ) -> None:
        """`variantId` is CHR_BP_A2_A1, so the effect allele A1 is the alternate allele."""
        assert credible_sets[1]["variantId"] == "1_1000_T_C"
        assert credible_sets[1]["studyId"] == "BigBrain_eqtl_EUR_ENSG01.1"

    def test_p_value_recomputed_from_z(
        self, credible_sets: dict[int, dict[str, Any]]
    ) -> None:
        """The p-value is rebuilt from Z, so it survives values below the float64 floor."""
        lead = credible_sets[1]
        assert lead["pValueExponent"] < -300
        assert 1 <= lead["pValueMantissa"] < 10

    def test_purity_only_for_single_variant_sets(
        self, credible_sets: dict[int, dict[str, Any]]
    ) -> None:
        """Purity is 1 for single-variant sets and unknown otherwise."""
        assert credible_sets[1]["purityMinR2"] == 1.0
        assert credible_sets[1]["purityMeanR2"] == 1.0
        assert credible_sets[2]["purityMinR2"] is None

    def test_annotations(self, credible_sets: dict[int, dict[str, Any]]) -> None:
        """Credible sets carry in-sample-LD confidence, PIPs and no log Bayes factors."""
        cs = credible_sets[2]
        assert (
            cs["confidence"]
            == CredibleSetConfidenceClasses.FINEMAPPED_IN_SAMPLE_LD.value
        )
        assert cs["studyType"] == "eqtl"
        assert cs["finemappingMethod"] == "SuSie"
        assert cs["variantId"] == "1_5000_A_G"
        assert cs["region"] == "1:5000-5100"
        assert cs["sampleSize"] == 9310
        assert [v["posteriorProbability"] for v in cs["locus"]] == [0.6, 0.38]
        assert all(v["logBF"] is None for v in cs["locus"])


def test_finemapping_ingestion_step(
    session: Session, finemapping_file: str, tmp_path: Path
) -> None:
    """The step writes credible sets and a study index covering every one of them."""
    cs_path, si_path = str(tmp_path / "credible_set"), str(tmp_path / "study_index")
    BigBrainFinemappingIngestionStep(
        session=session,
        qtl_type="eqtl",
        finemapping_path=finemapping_file,
        credible_set_path=cs_path,
        study_index_path=si_path,
    )
    cs = StudyLocus.from_parquet(session, cs_path).df
    si = StudyIndex.from_parquet(session, si_path).df
    assert cs.count() == 2
    assert [(r.studyId, r.geneId) for r in si.collect()] == [
        ("BigBrain_eqtl_EUR_ENSG01.1", "ENSG01")
    ]
    assert cs.join(si, "studyId", "left_anti").count() == 0


def test_sqtl_ingestion_requires_top_assoc(
    session: Session, finemapping_file: str, tmp_path: Path
) -> None:
    """SQTL genes come from top_assoc, so the step refuses to run without it."""
    with pytest.raises(ValueError, match="top_assoc_path"):
        BigBrainFinemappingIngestionStep(
            session=session,
            qtl_type="sqtl",
            finemapping_path=finemapping_file,
            credible_set_path=str(tmp_path / "cs"),
            study_index_path=str(tmp_path / "si"),
        )


def test_malformed_rows_fail_loudly(session: Session, tmp_path: Path) -> None:
    """A field that does not parse raises rather than silently becoming null."""
    path = tmp_path / "bad.txt"
    path.write_text(HEADER + "\n" + ROWS[0].replace("\t1000\t", "\t1000\xa0\t") + "\n")
    with pytest.raises(Exception, match="(?i)malformed|parse"):
        BigBrainFinemapping.read_from_source(session, str(path)).collect()
