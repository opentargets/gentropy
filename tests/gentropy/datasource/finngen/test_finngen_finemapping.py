"""Tests for finemapping SuSie results from FinnGen."""

from __future__ import annotations

from pathlib import Path

import hail as hl
import pytest
from pyspark.sql import SparkSession

from gentropy.common.session import Session
from gentropy.dataset.study_locus import StudyLocus
from gentropy.datasource.finngen.finemapping import FinnGenFinemapping
from gentropy.finngen_finemapping_ingestion import FinnGenFinemappingIngestionStep


@pytest.mark.parametrize(
    [
        "finngen_susie_finemapping_snp_files",
        "finngen_susie_finemapping_cs_summary_files",
    ],
    [
        pytest.param(
            "tests/gentropy/data_samples/finngen_R9_AB1_EBV.SUSIE.snp.gz",
            "tests/gentropy/data_samples/finngen_credset_summary_sample.tsv",
            id="non block compressed files",
        ),
        pytest.param(
            "tests/gentropy/data_samples/finngen_R9_AB1_EBV.SUSIE.snp.bgz",
            "tests/gentropy/data_samples/finngen_credset_summary_sample.tsv.bgz",
            id="block compressed files",
        ),
    ],
)
def test_finngen_finemapping_from_finngen_susie_finemapping(
    spark: SparkSession,
    finngen_susie_finemapping_snp_files: str,
    finngen_susie_finemapping_cs_summary_files: str,
) -> None:
    """Test finemapping results (SuSie) from source."""
    hl.init(sc=spark.sparkContext, log="/dev/null", idempotent=True)
    assert isinstance(
        FinnGenFinemapping.from_finngen_susie_finemapping(
            spark=spark,
            finngen_susie_finemapping_snp_files=finngen_susie_finemapping_snp_files,
            finngen_susie_finemapping_cs_summary_files=finngen_susie_finemapping_cs_summary_files,
            finngen_release_prefix="FINNGEN_R11",
        ),
        StudyLocus,
    )


@pytest.mark.parametrize(
    [
        "finngen_susie_finemapping_snp_files",
        "finngen_susie_finemapping_cs_summary_files",
    ],
    [
        pytest.param(
            "tests/gentropy/data_samples/finngen_R9_AB1_EBV.SUSIE.snp.gz",
            "tests/gentropy/data_samples/finngen_credset_summary_sample.tsv",
            id="non block compressed files",
        ),
    ],
)
@pytest.mark.step_test
def test_finngen_finemapping_ingestion_step(
    session: Session,
    finngen_susie_finemapping_snp_files: str,
    finngen_susie_finemapping_cs_summary_files: str,
    tmp_path: Path,
) -> None:
    """Test finngen finemapping ingestion step."""
    output_path = tmp_path / "output"
    FinnGenFinemappingIngestionStep(
        session=session,
        finngen_finemapping_out=str(output_path),
        finngen_susie_finemapping_cs_summary_files=finngen_susie_finemapping_cs_summary_files,
        finngen_susie_finemapping_snp_files=finngen_susie_finemapping_snp_files,
        finngen_finemapping_lead_pvalue_threshold=1e-5,
        finngen_release_prefix="FINNGEN_R11",
    )
    assert output_path.is_dir()
    assert (output_path / "_SUCCESS").exists()

    cs = StudyLocus.from_parquet(session=session, path=str(output_path))
    assert cs.df.count() == 1
    study_id: str = cs.df.select("studyId").collect()[0]["studyId"]
    assert study_id.startswith("FINNGEN_R11_")
