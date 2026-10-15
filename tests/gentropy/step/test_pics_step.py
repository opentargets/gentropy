"""Test PICS step."""

from __future__ import annotations

from pathlib import Path

import pytest

from gentropy import Session
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.study_locus import LDAnnotatedStudyLocus
from gentropy.pics import PICSStep


@pytest.mark.step_test
def test_pics_step_writes_credible_sets_of_significant_loci(
    session: Session, tmp_path: Path
) -> None:
    """The step fine-maps the loci with a significant lead and writes both datasets."""

    def locus(lead: str, exponent: int) -> list[tuple]:
        return [(lead, 1.0, exponent, 0.3, 1000, 0.1, 0.01)]

    rows = [
        ("L1", "S1", "1", 1, 1000, "1_100_A_T", [], locus("1_100_A_T", -10), [("1_100_A_T", 1.0), ("1_150_C_G", 0.8)]),
        ("L2", "S1", "2", 1, 1000, "2_100_A_T", [], locus("2_100_A_T", -6), [("2_100_A_T", 1.0)]),
    ]  # fmt: skip
    session.spark.createDataFrame(
        rows, LDAnnotatedStudyLocus.get_schema()
    ).write.parquet(str(tmp_path / "loci"))
    session.spark.createDataFrame(
        [("S1", "P1", "gwas")], "studyId string, projectId string, studyType string"
    ).write.parquet(str(tmp_path / "studies"))

    PICSStep(
        session=session,
        study_locus_ld_annotated_in=str(tmp_path / "loci"),
        study_index_path=str(tmp_path / "studies"),
        picsed_study_locus_out=str(tmp_path / "credible_sets"),
        picsed_credible_set_variant_out=str(tmp_path / "credible_set_variants"),
    )

    credible_sets = CredibleSet.from_parquet(session, str(tmp_path / "credible_sets"))
    variants = CredibleSetVariant.from_parquet(
        session, str(tmp_path / "credible_set_variants")
    )
    # The locus led at 1e-6 is below the GWAS significance and is not fine-mapped:
    assert [row.leadVariantId for row in credible_sets.df.collect()] == ["1_100_A_T"]
    assert sorted(row.variantId for row in variants.df.collect()) == [
        "1_100_A_T",
        "1_150_C_G",
    ]
