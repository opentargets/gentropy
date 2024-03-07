"""Test of the qc of summary statistics."""

from __future__ import annotations

import numpy as np
import pyspark.sql.functions as f
from gentropy.dataset.summary_statistics import SummaryStatistics


def test_qc_functions(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Test all sumstat qc functions."""
    gwas = sample_summary_statistics.sanity_filter()

    assert gwas.number_of_snps() == (1663, 29)
    assert np.round(gwas.gc_lambda_check(lambda_threshold=2)[1], 4) == 1.916
    assert np.round(gwas.sumstat_qc_beta_check()[1], 4) == 0.0013
    assert gwas.sumstat_n_eff_check(n_total=100000) == (True, 0)
    assert np.sum(np.round(gwas.sumstat_qc_pz_check(), 6)) == 2


def test_neff_check_eaf(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Test N_eff check using mock EAFs."""
    gwas = sample_summary_statistics.sanity_filter()
    gwas_df = gwas._df
    gwas_df = gwas_df.withColumn("effectAlleleFrequencyFromSource", f.lit(0.5))
    gwas._df = gwas_df

    assert np.round(gwas.sumstat_n_eff_check(n_total=100000)[1], 4) == 0.5586
