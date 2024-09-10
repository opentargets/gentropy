"""Tests for finemapping SuSie results from FinnGen."""

from __future__ import annotations

import hail as hl
import pytest
from pyspark.sql import SparkSession

from gentropy.dataset.study_locus import StudyLocus
from gentropy.datasource.finngen.finemapping import FinnGenFinemapping


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
        ),
        StudyLocus,
    )
