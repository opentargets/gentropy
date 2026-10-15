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
from gentropy.dataset.target_index import TargetIndex
from gentropy.datasource.bigbrain.finemapping import BigBrainFinemapping

COLUMNS = [
    "SNP", "CHR", "BP", "A1", "A2", "Z", "N", "P", "PIP",
    "BETA_MEAN", "BETA_SD", "DISTANCE_FROM_CENTER", "CREDIBLE_SET", "feature",
]  # fmt: skip
ROWS = [
    # Single-variant credible set with a p-value far below the float64 floor.
    ("rs1", 1, 1000, "C", "T", 40.0, 9310, 0, 1.0, 0.37, 0.01, 10, 1, "ENSG01.1"),
    # Two-variant credible set, plus an untested placeholder that is not its lead.
    ("rs2", 1, 5000, "G", "A", 8.0, 9310, 1e-15, 0.6, 0.2, 0.02, 20, 2, "ENSG01.1"),
    ("rs3", 1, 5100, "T", "C", 7.5, 9310, 1e-13, 0.38, 0.1, 0.02, 30, 2, "ENSG01.1"),
    ("rs4", 1, 5200, "A", "G", 0, 9310, 1, 0.01, 0.0, 0.02, 40, 2, "ENSG01.1"),
    # Beyond the 95% credible set (cumulative PIP before it is 0.98): dropped.
    ("rs7", 1, 5300, "C", "A", 2.0, 9310, 0.05, 0.01, 0.0, 0.02, 50, 2, "ENSG01.1"),
    # Credible set led by an untested placeholder: dropped entirely.
    ("rs5", 2, 100, "A", "C", 0, 9310, 1, 0.9, -2.1, 0.01, 50, 3, "ENSG01.1"),
    ("rs6", 2, 200, "G", "T", 3.0, 9310, 0.003, 0.08, 0.1, 0.01, 60, 3, "ENSG01.1"),
]  # fmt: skip


def write_tsv(path: Path, columns: list[str], rows: list[tuple[Any, ...]]) -> str:
    """Write rows as a tab-separated file with a header, as the source files are."""
    lines = ["\t".join(columns)] + ["\t".join(map(str, row)) for row in rows]
    path.write_text("\n".join(lines) + "\n")
    return str(path)


@pytest.fixture
def finemapping_file(tmp_path: Path) -> str:
    """Write a small BigBrain fine-mapping file."""
    return write_tsv(tmp_path / "BigBrain_cis_eQTL_finemapping.txt", COLUMNS, ROWS)


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

    def test_keeps_only_95_credible_set(
        self, credible_sets: dict[int, dict[str, Any]]
    ) -> None:
        """Members beyond the 95% credible set are dropped; the rest are flagged and sorted."""
        locus = credible_sets[2]["locus"]
        assert [v["variantId"] for v in locus] == ["1_5000_A_G", "1_5100_C_T"]
        assert all(v["is95CredibleSet"] for v in locus)
        assert credible_sets[2]["region"] == "1:5000-5100"

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
        assert cs["finemappingMethod"] == "SuSiE"
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


def test_sqtl_ingestion_step(session: Session, tmp_path: Path) -> None:
    """SQTL genes come from top_assoc, whose "NA" placeholders are read as null."""
    junction_rows = [(*row[:-1], "chr1_900_950") for row in ROWS]
    target_index_path = str(tmp_path / "target_index")
    schema = TargetIndex.get_schema()
    session.spark.createDataFrame(
        [{fld.name: None for fld in schema.fields} | {"id": "ENSG01"}], schema=schema
    ).write.parquet(target_index_path)
    si_path = str(tmp_path / "study_index")
    BigBrainFinemappingIngestionStep(
        session=session,
        qtl_type="sqtl",
        finemapping_path=write_tsv(tmp_path / "sqtl.txt", COLUMNS, junction_rows),
        credible_set_path=str(tmp_path / "credible_set"),
        study_index_path=si_path,
        top_assoc_path=write_tsv(
            tmp_path / "top_assoc.tsv",
            ["feature", "gene_id", "gene_name"],
            [("chr1_900_950", "NA", "NA")],
        ),
        target_index_path=target_index_path,
    )
    row = StudyIndex.from_parquet(session, si_path).df.collect()[0]
    assert row.studyId == "BigBrain_sqtl_EUR_chr1_900_950"
    assert row.geneId is None
    assert row.traitFromSource is None


def test_sqtl_ingestion_requires_top_assoc(
    session: Session, finemapping_file: str, tmp_path: Path
) -> None:
    """SQTL genes come from top_assoc, so the step refuses to run without it."""
    with pytest.raises(ValueError, match="top_assoc_path and target_index_path"):
        BigBrainFinemappingIngestionStep(
            session=session,
            qtl_type="sqtl",
            finemapping_path=finemapping_file,
            credible_set_path=str(tmp_path / "cs"),
            study_index_path=str(tmp_path / "si"),
        )


def test_malformed_rows_fail_loudly(session: Session, tmp_path: Path) -> None:
    """A field that does not parse raises rather than silently becoming null."""
    bad_row = (*ROWS[0][:2], "1000\xa0", *ROWS[0][3:])
    path = write_tsv(tmp_path / "bad.txt", COLUMNS, [bad_row])
    with pytest.raises(Exception, match="(?i)malformed|parse"):
        BigBrainFinemapping.read_from_source(session, path).collect()
