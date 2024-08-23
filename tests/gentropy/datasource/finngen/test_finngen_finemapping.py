"""Tests for finemapping SuSie results from FinnGen."""

from __future__ import annotations

from pyspark.sql import SparkSession

from gentropy.dataset.study_locus import StudyLocus
from gentropy.datasource.finngen.finemapping import FinnGenFinemapping


def test_finngen_finemapping_from_finngen_susie_finemapping(
    spark: SparkSession,
) -> None:
    """Test finemapping results (SuSie) from source."""
    assert isinstance(
        FinnGenFinemapping.from_finngen_susie_finemapping(
            spark=spark,
            finngen_finemapping_df="tests/gentropy/data_samples/finngen_R9_AB1_EBV.SUSIE.snp.gz",
            finngen_finemapping_summaries="tests/gentropy/data_samples/finngen_credset_summary_sample.tsv",
            finngen_release_prefix="FINNGEN_R10",
        ),
        StudyLocus,
    )
